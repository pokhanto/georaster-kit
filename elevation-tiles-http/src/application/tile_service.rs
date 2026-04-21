//! Tile service for resolving H3 tiles and their aggregated elevations.

use futures::Stream;
use geo::{BoundingRect, Contains, Intersects, Point, Polygon};
use georaster_core::GeorasterSampling;
use georaster_domain::{Bounds, RasterGrid};
use h3o::{
    Resolution,
    geom::{ContainmentMode, TilerBuilder},
};
use moka::future::Cache;
use std::{collections::HashMap, str::FromStr};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    application::{
        elevation_calculation::ElevationCalculationStrategy, elevation_provider::ElevationProvider,
    },
    domain::{Elevation, ElevationTile, Tile},
};

/// Controls split of requested bounding box to smaller chunks.
const MAX_CELLS_PER_CHUNK: usize = 25000;

const GEORASTER_SAMPLING: GeorasterSampling = GeorasterSampling::Resolution {
    x_resolution: 0.005,
    y_resolution: 0.005,
};

/// Errors returned by [`TileService`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TileServiceError {
    #[error("Incorrect zoom level")]
    ZoomLevel,
    #[error("Can't build tiles for given bounding box")]
    BuildTiles,
    #[error("Unknown tile")]
    UnknownTile,
    #[error("Can't get elevation")]
    Elevation,
    #[error("Chunking requires explicit degree resolution")]
    ChunkResolution,
}

/// Resolves tiles and caches calculated results.
#[derive(Clone, Debug)]
pub struct TileService<EP> {
    elevation_provider: EP,
    // moka::Cache cloning is cheap
    cache: Cache<String, ElevationTile>,
    max_cells_per_chunk: usize,
}

impl<EP> TileService<EP>
where
    EP: ElevationProvider,
{
    /// Creates tile service with in-memory cache.
    pub fn new(elevation_provider: EP, cache_max_capacity: u64) -> Self {
        let cache = Cache::builder().max_capacity(cache_max_capacity).build();

        Self {
            elevation_provider,
            cache,
            max_cells_per_chunk: MAX_CELLS_PER_CHUNK,
        }
    }

    /// Returns tile by id, using cache when possible.
    #[tracing::instrument(skip(self, calculation_strategy), fields(tile_id = %tile_id))]
    pub async fn get_tile_by_id<S>(
        &self,
        tile_id: String,
        calculation_strategy: S,
    ) -> Result<ElevationTile, TileServiceError>
    where
        S: ElevationCalculationStrategy,
    {
        let cache_key = tile_cache_key(calculation_strategy.key(), &tile_id);

        if let Some(tile) = self.cache.get(&cache_key).await {
            tracing::debug!(tile_id, "tile cache hit");
            return Ok(tile);
        }

        tracing::debug!(tile_id, "tile cache miss");

        let tile = Tile::from_str(&tile_id).map_err(|err| {
            tracing::debug!(error = ?err, tile_id, "failed to parse tile id as h3 cell");
            TileServiceError::UnknownTile
        })?;

        let tile_bounding_rect = tile.bounding_rect().ok_or_else(|| {
            tracing::debug!("failed to calculate bounding rect from h3 cell boundary");
            TileServiceError::UnknownTile
        })?;

        let elevation_grid = self
            .elevation_provider
            .elevations_in_bbox(tile_bounding_rect.into(), Some(GEORASTER_SAMPLING))
            .await
            .map_err(|err| {
                tracing::debug!(error = ?err, "failed to get elevations for tile bbox");
                TileServiceError::Elevation
            })?;

        let mut calculation_state = calculation_strategy.new_state();
        let elevation_data = elevation_grid.band(1).ok_or(TileServiceError::Elevation)?;

        for value in elevation_data.data().iter() {
            calculation_strategy.update(&mut calculation_state, Elevation(*value));
        }

        let elevation_tile =
            ElevationTile::new(tile_id, calculation_strategy.finalize(calculation_state));

        self.cache.insert(cache_key, elevation_tile.clone()).await;

        Ok(elevation_tile)
    }
}

impl<EP> TileService<EP>
where
    EP: ElevationProvider + Clone + Send + Sync + 'static,
{
    /// Streams tiles for requested bounding box.
    ///
    /// To avoid loading elevations for whole bounding box at once service
    /// splits requested area into smaller chunks and processes them separately.
    ///
    /// Because H3 tiles may cross chunk boundaries tile aggregation state is kept
    /// across chunk processing. Tiles updated from every chunk they intersect
    /// and emitted only after all relevant chunks is processed.
    #[tracing::instrument(
        skip(self, calcualtion_strategy),
        fields(
            zoom_level,
            min_lon = bbox.min_lon(),
            min_lat = bbox.min_lat(),
            max_lon = bbox.max_lon(),
            max_lat = bbox.max_lat(),
        )
    )]
    pub fn stream_tiles_for_bbox<S>(
        &self,
        bbox: Bounds,
        zoom_level: u8,
        calcualtion_strategy: S,
    ) -> Result<
        impl Stream<Item = Result<ElevationTile, TileServiceError>> + Send + 'static + use<EP, S>,
        TileServiceError,
    >
    where
        S: ElevationCalculationStrategy + Send + Sync + 'static,
        S::State: Send + 'static,
    {
        // resolve all H3 tiles covering requested bbox
        let tile_ids = get_tile_ids_for_bbox(bbox, zoom_level)?;

        // split requested bbox in chunks to avoid loading whole area at once
        let chunks = split_bbox_into_chunks(bbox, GEORASTER_SAMPLING, self.max_cells_per_chunk)?;

        tracing::info!(chunks_count = chunks.len(), "got bbox chunks count");

        let elevation_provider = self.elevation_provider.clone();
        let cache = self.cache.clone();

        // assign stable ids to chunks so we can track which tiles depend on which chunks
        let chunk_infos = chunks
            .into_iter()
            .enumerate()
            .map(|(id, bounds)| ChunkInfo { id, bounds })
            .collect::<Vec<_>>();

        // initialize aggregation state for every requested tile
        // this needs because some tiles will fall to chunk edges
        // and this will require to update them
        let mut tile_states = tile_ids
            .into_iter()
            .map(|tile_id| {
                let tile = Tile::from_str(&tile_id).map_err(|err| {
                    tracing::debug!(error = ?err, tile_id, "failed to parse tile id as h3 cell");
                    TileServiceError::UnknownTile
                })?;

                let polygon = tile.polygon();

                let bounds = polygon
                    .bounding_rect()
                    .ok_or(TileServiceError::BuildTiles)?;

                Ok((
                    tile_id,
                    TileAggregationState {
                        polygon,
                        bounds: bounds.into(),
                        calculation_state: calcualtion_strategy.new_state(),
                        remaining_chunks: 0,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, TileServiceError>>()?;

        // for each chunk calculate which tiles may be affected by it
        // also count how many chunks each tile must wait for before it is complete
        let mut chunk_to_tile_ids: HashMap<usize, Vec<String>> = HashMap::new();

        for chunk in &chunk_infos {
            let chunk_rect: geo::Rect<f64> = chunk.bounds.into();

            for (tile_id, state) in &mut tile_states {
                let tile_rect: geo::Rect<f64> = state.bounds.into();

                if chunk_rect.intersects(&tile_rect) {
                    chunk_to_tile_ids
                        .entry(chunk.id)
                        .or_default()
                        .push(tile_id.clone());
                    state.remaining_chunks += 1;
                }
            }
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ElevationTile, TileServiceError>>(128);

        tokio::spawn(async move {
            let strategy_key = calcualtion_strategy.key();
            // process chunks one by one and update only tiles that intersect current chunk
            for chunk in chunk_infos {
                let affected_tile_ids = chunk_to_tile_ids
                    .get(&chunk.id)
                    .cloned()
                    .unwrap_or_default();

                if affected_tile_ids.is_empty() {
                    continue;
                }

                // try to find this chunk in cache
                let mut uncached_tile_ids = Vec::new();

                for tile_id in &affected_tile_ids {
                    // tile may already be completed and removed by previous chunk
                    if !tile_states.contains_key(tile_id) {
                        continue;
                    }
                    let cache_key = tile_cache_key(strategy_key, tile_id);

                    if let Some(tile) = cache.get(&cache_key).await {
                        // cached tile does not need to be calculated, so remove its state and emit immediately
                        tile_states.remove(tile_id);

                        if tx.send(Ok(tile)).await.is_err() {
                            tracing::debug!("tile streaming was not successful");
                            return;
                        }
                    } else {
                        uncached_tile_ids.push(tile_id.clone());
                    }
                }

                // if every tile for this chunk was already cached, skip bbox read completely
                if uncached_tile_ids.is_empty() {
                    tracing::debug!(
                        chunk_id = chunk.id,
                        "skipping chunk because all tiles are cached"
                    );
                    continue;
                }

                // fetch elevations only for tiles still missing from cache
                let chunk_elevations = match elevation_provider
                    .elevations_in_bbox(chunk.bounds, Some(GEORASTER_SAMPLING))
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!(
                            error = ?err,
                            chunk_id = chunk.id,
                            ?chunk.bounds,
                            "failed to get elevations for chunk"
                        );
                        let _ = tx.send(Err(TileServiceError::Elevation)).await;
                        return;
                    }
                };

                update_selected_tile_states_from_chunk(
                    &chunk_elevations,
                    chunk.bounds,
                    uncached_tile_ids.iter().map(String::as_str),
                    &mut tile_states,
                    &calcualtion_strategy,
                );

                tracing::info!(
                    chunk_id = chunk.id,
                    affected_tiles = affected_tile_ids.len(),
                    uncached_tiles = uncached_tile_ids.len(),
                    "processed chunk"
                );

                let mut completed_tile_ids = Vec::new();

                for tile_id in uncached_tile_ids {
                    let Some(state) = tile_states.get_mut(&tile_id) else {
                        continue;
                    };

                    // tile got data for current chunk, decrease counter
                    if state.remaining_chunks > 0 {
                        state.remaining_chunks -= 1;
                    }

                    // if no counters - tile got data from all chunks
                    if state.remaining_chunks == 0 {
                        completed_tile_ids.push(tile_id);
                    }
                }

                for tile_id in completed_tile_ids {
                    let Some(state) = tile_states.remove(&tile_id) else {
                        continue;
                    };

                    let tile = ElevationTile::new(
                        tile_id.clone(),
                        calcualtion_strategy.finalize(state.calculation_state),
                    );

                    let cache_key = tile_cache_key(strategy_key, tile.id());

                    cache.insert(cache_key, tile.clone()).await;

                    if tx.send(Ok(tile)).await.is_err() {
                        tracing::debug!("tile streaming was not successful");
                        return;
                    }
                }

                tokio::task::yield_now().await;
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}

#[derive(Debug, Clone, Copy)]
struct ChunkInfo {
    /// Chunk id used to map chunk with to tiles.
    id: usize,
    /// Geographic bounds of this chunk.
    bounds: Bounds,
}

#[derive(Debug, Clone)]
struct TileAggregationState<S> {
    /// Exact tile polygon.
    polygon: Polygon<f64>,
    /// Bounding box of tile polygon.
    bounds: Bounds,
    /// Running accumulator for elevation values contributed to this tile.
    calculation_state: S,
    /// Number of chunks that may still contribute to this tile.
    remaining_chunks: usize,
}

pub fn get_tile_ids_for_bbox(
    bbox: Bounds,
    zoom_level: u8,
) -> Result<Vec<String>, TileServiceError> {
    let resolution: Resolution = zoom_level.try_into().map_err(|err| {
        tracing::debug!(error = ?err, "invalid zoom level for h3 resolution");
        TileServiceError::ZoomLevel
    })?;

    let mut tiler = TilerBuilder::new(resolution)
        .containment_mode(ContainmentMode::Covers)
        .build();

    tiler.add(bbox.into()).map_err(|err| {
        tracing::debug!(error = ?err, "failed to add bbox to tiler");
        TileServiceError::BuildTiles
    })?;

    let tile_ids = tiler
        .into_coverage()
        .map(|tile| tile.to_string())
        .collect::<Vec<_>>();

    tracing::info!(tile_count = tile_ids.len(), "resolved tile ids for bbox");

    Ok(tile_ids)
}

/// Updates only selected tiles from one chunk.
fn update_selected_tile_states_from_chunk<'a, S>(
    elevations: &RasterGrid,
    bbox: Bounds,
    tile_ids: impl IntoIterator<Item = &'a str>,
    tile_states: &mut HashMap<String, TileAggregationState<S::State>>,
    strategy: &S,
) where
    S: ElevationCalculationStrategy,
{
    if elevations.width() == 0 || elevations.height() == 0 {
        return;
    }

    // geographic size of one cell of grid in returned elevation grid
    let lon_step = (bbox.max_lon() - bbox.min_lon()) / elevations.width() as f64;
    let lat_step = (bbox.max_lat() - bbox.min_lat()) / elevations.height() as f64;

    // update only tiles that affected by this chunk
    for tile_id in tile_ids {
        let Some(state) = tile_states.get_mut(tile_id) else {
            continue;
        };

        // find overlap between tile bounds and current chunk bbox
        // if there is no overlap - skip
        let overlap = match state.bounds.intersection(&bbox) {
            Some(v) => v,
            None => continue,
        };

        // map overlap bbox into column range of chunk elevation grid
        let start_col =
            (((overlap.min_lon() - bbox.min_lon()) / lon_step).floor() as isize).max(0) as usize;
        let end_col_exclusive = (((overlap.max_lon() - bbox.min_lon()) / lon_step).ceil() as isize)
            .min(elevations.width() as isize)
            .max(0) as usize;

        // map overlap bbox into row range of chunk elevation grid
        let start_row =
            (((bbox.max_lat() - overlap.max_lat()) / lat_step).floor() as isize).max(0) as usize;
        let end_row_exclusive = (((bbox.max_lat() - overlap.min_lat()) / lat_step).ceil() as isize)
            .min(elevations.height() as isize)
            .max(0) as usize;

        // skip empty target window
        if start_col >= end_col_exclusive || start_row >= end_row_exclusive {
            continue;
        }

        for row in start_row..end_row_exclusive {
            // calculate latitude of cell center for current row
            let lat = bbox.max_lat() - (row as f64 + 0.5) * lat_step;

            for col in start_col..end_col_exclusive {
                let idx = row * elevations.width() + col;
                let Some(value) = elevations.band(1).map(|band| band.data()[idx]) else {
                    continue;
                };

                // calculate longitude of cell center for current column
                let lon = bbox.min_lon() + (col as f64 + 0.5) * lon_step;

                // cheap rectangular prefilter before exact polygon check
                if !state.bounds.contains_point(lon, lat) {
                    continue;
                }

                let point = Point::new(lon, lat);

                // exact containment check against H3 tile polygon
                if state.polygon.contains(&point) {
                    strategy.update(&mut state.calculation_state, Elevation(value));
                }
            }
        }
    }
}

/// Splits requested bbox in chunks.
fn split_bbox_into_chunks(
    bbox: Bounds,
    sampling: GeorasterSampling,
    max_cells_per_chunk: usize,
) -> Result<Vec<Bounds>, TileServiceError> {
    let (lon_resolution, lat_resolution) = match sampling {
        GeorasterSampling::Resolution {
            x_resolution,
            y_resolution,
        } => (x_resolution, y_resolution),
        _ => return Err(TileServiceError::ChunkResolution),
    };

    // approximate square chunk size in output grid cells
    let chunk_side_cells = (max_cells_per_chunk as f64).sqrt().floor().max(1.0);

    // convert chunk size in cells into geographic step in degrees
    let chunk_lon_step = chunk_side_cells * lon_resolution;
    let chunk_lat_step = chunk_side_cells * lat_resolution;

    let mut chunks = Vec::new();

    // split bbox row by row in latitude direction
    let mut min_lat = bbox.min_lat();
    while min_lat < bbox.max_lat() {
        let max_lat = (min_lat + chunk_lat_step).min(bbox.max_lat());

        // split current latitude column by column in longitude direction
        let mut min_lon = bbox.min_lon();
        while min_lon < bbox.max_lon() {
            let max_lon = (min_lon + chunk_lon_step).min(bbox.max_lon());

            let chunk = Bounds::try_new(min_lon, min_lat, max_lon, max_lat)
                .map_err(|_| TileServiceError::BuildTiles)?;

            chunks.push(chunk);
            min_lon = max_lon;
        }

        min_lat = max_lat;
    }

    Ok(chunks)
}

fn tile_cache_key(strategy_key: &str, tile_id: &str) -> String {
    format!("{strategy_key}:{tile_id}")
}
