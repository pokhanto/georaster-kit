use elevation_types::{Bounds, ResolutionHint};
use geo::{BoundingRect, LineString};
use h3o::{
    CellIndex, Resolution,
    geom::{ContainmentMode, TilerBuilder},
};
use std::str::FromStr;

use crate::{application::elevation_provider::ElevationProvider, domain::Tile};

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TileServiceError {
    #[error("Incorrect zoom level")]
    ZoomLevel,
    #[error("Can'n build tiles for given bounding box")]
    BuildTiles,
    #[error("Unknown tile")]
    UnknownTile,
    #[error("Can't get elevation")]
    Elevation,
}

#[derive(Clone, Debug)]
pub struct TileService<EP> {
    elevation_provider: EP,
}

impl<EP> TileService<EP>
where
    EP: ElevationProvider,
{
    pub fn new(elevation_provider: EP) -> Self {
        Self { elevation_provider }
    }

    #[tracing::instrument(skip(self), fields(tile_id = %tile_id))]
    pub async fn get_tile_by_id(&self, tile_id: String) -> Result<Tile, TileServiceError> {
        let cell_index = CellIndex::from_str(&tile_id).map_err(|err| {
            tracing::debug!(error = ?err, "failed to parse tile id as h3 cell");
            TileServiceError::UnknownTile
        })?;

        let cell_bounding_rect = LineString::from(cell_index.boundary())
            .bounding_rect()
            .ok_or_else(|| {
                tracing::debug!("failed to compute bounding rect from h3 cell boundary");
                TileServiceError::UnknownTile
            })?;

        tracing::debug!(?cell_bounding_rect, "resolved tile bounding rect");

        let elevations = self
            .elevation_provider
            .elevations_in_bbox(cell_bounding_rect.into(), Some(ResolutionHint::Highest))
            .await
            .map_err(|err| {
                tracing::debug!(error = ?err, "failed to get elevations for tile bbox");
                TileServiceError::Elevation
            })?;

        tracing::debug!(
            value_count = elevations.values.len(),
            "fetched elevations for tile"
        );

        let tile = Tile::new_with_mean_elevation(tile_id, elevations.values);

        tracing::info!("tile with elevation resolved");

        Ok(tile)
    }

    #[tracing::instrument(
        skip(self),
        fields(
            zoom_level,
            min_lon = bbox.min_lon,
            min_lat = bbox.min_lat,
            max_lon = bbox.max_lon,
            max_lat = bbox.max_lat,
        )
    )]
    pub fn get_tile_ids_for_bbox(
        &self,
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
            .map(|cell| cell.to_string())
            .collect::<Vec<_>>();

        tracing::info!(tile_count = tile_ids.len(), "resolved tile ids for bbox");

        Ok(tile_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elevation_types::{BboxElevations, Bounds, Elevation, ResolutionHint};
    use std::sync::{Arc, Mutex};

    use crate::application::elevation_provider::{ElevationProvider, ElevationProviderError};

    type FakeElevationProviderData = (Bounds, Option<ResolutionHint>);
    #[derive(Clone, Debug)]
    struct FakeElevationProvider {
        result: Result<BboxElevations, ElevationProviderError>,
        calls: Arc<Mutex<Vec<FakeElevationProviderData>>>,
    }

    impl FakeElevationProvider {
        fn ok(values: Vec<Option<Elevation>>) -> Self {
            Self {
                result: Ok(BboxElevations {
                    bbox: Bounds {
                        min_lon: 11.1,
                        min_lat: 11.1,
                        max_lon: 11.1,
                        max_lat: 11.1,
                    },
                    width: 11,
                    height: 11,
                    values,
                }),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn err(error: ElevationProviderError) -> Self {
            Self {
                result: Err(error),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(Bounds, Option<ResolutionHint>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl ElevationProvider for FakeElevationProvider {
        async fn elevations_in_bbox(
            &self,
            bbox: Bounds,
            hint: Option<ResolutionHint>,
        ) -> Result<BboxElevations, ElevationProviderError> {
            self.calls.lock().unwrap().push((bbox, hint));
            self.result.clone()
        }
    }

    fn valid_tile_id() -> String {
        "8a1e23fffffffff".to_string()
    }

    fn valid_bbox() -> Bounds {
        Bounds {
            min_lon: 36.20,
            min_lat: 49.96,
            max_lon: 36.30,
            max_lat: 50.02,
        }
    }

    #[tokio::test]
    async fn get_tile_by_id_returns_tile_with_mean_elevation() {
        let provider = FakeElevationProvider::ok(vec![
            Some(Elevation(10.0)),
            Some(Elevation(20.0)),
            None,
            Some(Elevation(30.0)),
        ]);
        let service = TileService::new(provider);

        let tile = service.get_tile_by_id(valid_tile_id()).await.unwrap();

        assert_eq!(tile.id(), "8a1e23fffffffff");
        assert_eq!(tile.elevation().map(|e| e.0), Some(20.0));
    }

    #[tokio::test]
    async fn get_tile_by_id_returns_unknown_tile_for_invalid_id() {
        let provider = FakeElevationProvider::ok(vec![Some(Elevation(10.0))]);
        let service = TileService::new(provider);

        let result = service.get_tile_by_id("not-a-valid-tile".to_string()).await;

        assert_eq!(result.unwrap_err(), TileServiceError::UnknownTile);
    }

    #[tokio::test]
    async fn get_tile_by_id_returns_elevation_error_when_provider_fails() {
        let provider = FakeElevationProvider::err(ElevationProviderError::Elevation(
            elevation_core::ElevationServiceError::Metadata,
        ));
        let service = TileService::new(provider);

        let result = service.get_tile_by_id(valid_tile_id()).await;

        assert_eq!(result.unwrap_err(), TileServiceError::Elevation);
    }

    #[tokio::test]
    async fn get_tile_by_id_passes_highest_resolution_hint_to_provider() {
        let provider = FakeElevationProvider::ok(vec![Some(Elevation(42.0))]);
        let provider_for_assert = provider.clone();
        let service = TileService::new(provider);

        let _ = service.get_tile_by_id(valid_tile_id()).await.unwrap();

        let calls = provider_for_assert.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, Some(ResolutionHint::Highest));
    }

    #[tokio::test]
    async fn get_tile_by_id_passes_proper_bbox_to_provider() {
        let provider = FakeElevationProvider::ok(vec![Some(Elevation(42.0))]);
        let provider_for_assert = provider.clone();
        let service = TileService::new(provider);

        let _ = service.get_tile_by_id(valid_tile_id()).await.unwrap();

        let calls = provider_for_assert.calls();
        assert_eq!(calls.len(), 1);

        let bbox = calls[0].0;
        assert!(bbox.min_lon < bbox.max_lon);
        assert!(bbox.min_lat < bbox.max_lat);
    }

    #[test]
    fn get_tile_ids_for_bbox_returns_non_empty_result_for_valid_input() {
        let provider = FakeElevationProvider::ok(vec![]);
        let service = TileService::new(provider);

        let tile_ids = service.get_tile_ids_for_bbox(valid_bbox(), 10).unwrap();

        assert!(!tile_ids.is_empty());
    }

    #[test]
    fn get_tile_ids_for_bbox_returns_zoom_level_error_for_invalid_zoom() {
        let provider = FakeElevationProvider::ok(vec![]);
        let service = TileService::new(provider);

        let result = service.get_tile_ids_for_bbox(valid_bbox(), 255);

        assert_eq!(result.unwrap_err(), TileServiceError::ZoomLevel);
    }

    #[test]
    fn get_tile_ids_for_bbox_returns_only_valid_h3_indexes() {
        let provider = FakeElevationProvider::ok(vec![]);
        let service = TileService::new(provider);

        let tile_ids = service.get_tile_ids_for_bbox(valid_bbox(), 10).unwrap();

        assert!(!tile_ids.is_empty());

        for tile_id in tile_ids {
            let parsed = CellIndex::from_str(&tile_id);
            assert!(parsed.is_ok(), "invalid h3 tile id: {tile_id}");
        }
    }

    #[test]
    fn get_tile_ids_for_bbox_returns_unique_tile_ids() {
        let provider = FakeElevationProvider::ok(vec![]);
        let service = TileService::new(provider);

        let tile_ids = service.get_tile_ids_for_bbox(valid_bbox(), 10).unwrap();

        let unique_count = tile_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        assert_eq!(unique_count, tile_ids.len());
    }
}
