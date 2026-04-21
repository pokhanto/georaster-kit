use georaster_domain::{
    BandSelection, Bounds, DatasetMetadata, MetadataStorage, RasterBand, RasterGrid, RasterPoint,
    RasterPointBand, RasterReadQuery, RasterReader, RasterRepresentation, RasterSize,
    WindowPlacement,
};

use crate::GeorasterSampling;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum GeorasterServiceError {
    #[error("Failed to load dataset metadata")]
    MetadataLoad,

    #[error("Failed to resolve output resolution")]
    Resolution,

    #[error("Failed to build raster processing plan")]
    RasterPlan,

    #[error("Failed to read raster data")]
    RasterRead,
}

/// Service for resolving raster values from raster data using dataset metadata.
#[derive(Debug, Clone)]
pub struct GeorasterService<M, R> {
    metadata_storage: M,
    raster_reader: R,
}

impl<M, R> GeorasterService<M, R> {
    /// Creates new georaster service with metadata storage and raster reader.
    pub fn new(metadata_storage: M, raster_reader: R) -> Self {
        Self {
            metadata_storage,
            raster_reader,
        }
    }
}

impl<M, R> GeorasterService<M, R>
where
    M: MetadataStorage,
    R: RasterReader,
{
    /// Returns raster value at requested geographic point.
    ///
    /// Service:
    /// - loads available dataset metadata,
    /// - finds datasets whose bounds contain requested point,
    /// - selects one dataset to read from,
    /// - converts geographic coordinate into raster row/column,
    /// - reads a single raster cell from selected dataset,
    /// - returns its value unless it matches dataset `nodata`.
    ///
    /// When multiple datasets contain point current implementation
    /// selects highest resolution dataset.
    ///
    /// # Parameters
    ///
    /// - `lon`: Longitude / X coordinate of requested point in dataset
    ///   coordinate space.
    /// - `lat`: Latitude / Y coordinate of requested point in dataset
    ///   coordinate space.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(RasterPointValue))` if a dataset contains point and a valid
    ///   raster value is found.
    /// - `Ok(None)` if no dataset contains point mapped raster cell
    ///   is outside raster bounds or resulting value equals dataset
    ///   `nodata`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`GeorasterServiceError::MetadataLoad`] if dataset metadata cannot be loaded,
    /// - [`GeorasterServiceError::RasterRead`] if raster data cannot be read.
    ///
    /// # Notes
    ///
    /// Current implementation assumes axis-aligned rasters in the same
    /// coordinate space as dataset metadata.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let value = service.raster_data_at_point(30.5234, 50.4501).await?;
    ///
    /// if let Some(value) = value {
    ///     println!("Raster value: {}", value.0);
    /// }
    /// ```
    #[tracing::instrument(skip(self), fields(lon, lat, band_selection, raster_representation))]
    pub async fn raster_data_at_point(
        &self,
        lon: f64,
        lat: f64,
        band_selection: BandSelection,
        raster_representation: RasterRepresentation,
    ) -> Result<Option<RasterPoint>, GeorasterServiceError> {
        tracing::info!(lon, lat, "starting point raster query");

        let datasets = self.metadata_storage.load_metadata().await.map_err(|err| {
            tracing::error!(
                error = %err,
                lon,
                lat,
                "failed to load dataset metadata"
            );

            GeorasterServiceError::MetadataLoad
        })?;

        let dataset = datasets
            .into_iter()
            .filter(|ds| {
                ds.raster.raster_representation == raster_representation
                    && ds.raster.bounds.contains_point(lon, lat)
            })
            .min_by(|a, b| {
                let a_area = a.raster.geo_transform.pixel_width.abs()
                    * a.raster.geo_transform.pixel_height.abs();
                let b_area = b.raster.geo_transform.pixel_width.abs()
                    * b.raster.geo_transform.pixel_height.abs();

                a_area
                    .partial_cmp(&b_area)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        let Some(dataset) = dataset else {
            tracing::debug!(lon, lat, "no dataset contains requested point");
            return Ok(None);
        };

        let Some(placement) = lonlat_to_raster_coord(&dataset, lon, lat) else {
            tracing::debug!(
                dataset_id = %dataset.dataset_id,
                lon,
                lat,
                "failed to map point into raster coordinates"
            );
            return Ok(None);
        };

        let band_indexes = dataset.raster.resolve_band_indexes(&band_selection);

        if band_indexes.is_empty() {
            tracing::debug!(
                dataset_id = %dataset.dataset_id,
                lon,
                lat,
                "no bands resolved for point query"
            );
            return Ok(None);
        }

        let raster_query = RasterReadQuery::new_point(placement, band_indexes);

        let raster_grid = self
            .raster_reader
            .read_window(&dataset.artifact_path, raster_query.clone())
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    dataset_id = %dataset.dataset_id,
                    artifact = %dataset.artifact_path,
                    lon,
                    lat,
                    raster_query = ?raster_query,
                    "failed to read raster value at point"
                );

                GeorasterServiceError::RasterRead
            })?;

        let values = raster_grid
            .into_bands()
            .into_iter()
            .filter_map(|band| {
                let value = band.data().first().copied()?;

                let nodata = dataset
                    .raster
                    .bands
                    .iter()
                    .find(|metadata_band| metadata_band.band_index == band.band_index())
                    .and_then(|metadata_band| metadata_band.nodata);

                if nodata == Some(value) {
                    return None;
                }

                Some(RasterPointBand::new(band.band_index(), value))
            })
            .collect::<Vec<_>>();

        if values.is_empty() {
            return Ok(None);
        }

        Ok(Some(RasterPoint::new(values)))
    }

    /// Returns a raster values grid for requested bounding box.
    ///
    /// Service:
    /// - loads available dataset metadata,
    /// - finds all datasets whose bounds intersect requested `bbox`,
    /// - resolves output grid dimensions from `sampling`,
    /// - reads corresponding raster windows from contributing datasets,
    /// - merges them into one resulting raster values grid.
    ///
    /// Result values are stored in row-major order:
    /// `values[row * width + column]`.
    ///
    /// When multiple datasets overlap, lower-resolution datasets are processed
    /// first so higher-resolution datasets can overwrite them in result.
    ///
    /// Cells whose raster value equals dataset `nodata` remain empty (`None`).
    ///
    /// # Parameters
    ///
    /// - `bbox`: Requested geographic area in dataset coordinate space.
    /// - `sampling`: Optional policy controlling output grid size. If `None`
    ///   service uses its default sampling policy - preview.
    ///
    /// # Returns
    ///
    /// [`BboxRasterValues`] containing:
    /// - requested bounding box,
    /// - resulting grid width,
    /// - resulting grid height,
    /// - flattened raster values.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`GeorasterServiceError::MetadataLoad`] if dataset metadata cannot be loaded,
    /// - [`GeorasterServiceError::RasterPlan`] if a raster processing plan cannot be built,
    /// - [`GeorasterServiceError::RasterRead`] if raster data cannot be read.
    ///
    /// # Notes
    ///
    /// Current implementation assumes axis-aligned rasters and uses bounding-box based
    /// processing in the same coordinate space as dataset metadata.
    #[tracing::instrument(
        skip(self),
        fields(bbox, sampling, band_selection, raster_representation)
    )]
    pub async fn raster_data_in_bbox(
        &self,
        bbox: Bounds,
        sampling: Option<GeorasterSampling>,
        band_selection: BandSelection,
        raster_representation: RasterRepresentation,
    ) -> Result<RasterGrid, GeorasterServiceError> {
        tracing::info!(
            bbox = ?bbox,
            sampling = ?sampling,
            "starting getting raster data in bbox with resolution"
        );

        let datasets = self.metadata_storage.load_metadata().await.map_err(|err| {
            tracing::error!(
                error = %err,
                bbox = ?bbox,
                sampling = ?sampling,
                "failed to load dataset metadata"
            );

            GeorasterServiceError::MetadataLoad
        })?;
        let datasets_len = datasets.len();

        let mut intersections: Vec<(DatasetMetadata, Bounds)> = datasets
            .into_iter()
            .filter_map(|dataset| {
                if dataset.raster.raster_representation != raster_representation {
                    return None;
                }

                dataset
                    .raster
                    .bounds
                    .intersection(&bbox)
                    .map(|intersection| (dataset, intersection))
            })
            .collect();

        tracing::info!(
            "found {} intersections for requested bbox {:?} from {} datasets",
            intersections.len(),
            bbox,
            datasets_len
        );

        let (width, height) = sampling
            .unwrap_or(GeorasterSampling::Preview)
            .bbox_dimensions(&bbox);

        // highest quality first
        intersections.sort_by(|(a, _), (b, _)| {
            let a_area = a.raster.geo_transform.pixel_width.abs()
                * a.raster.geo_transform.pixel_height.abs();
            let b_area = b.raster.geo_transform.pixel_width.abs()
                * b.raster.geo_transform.pixel_height.abs();

            a_area
                .partial_cmp(&b_area)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // build resulting bands based on resolved band indexes from first compatible dataset
        let requested_band_indexes = intersections
            .first()
            .map(|(dataset, _)| dataset.raster.resolve_band_indexes(&band_selection))
            .unwrap_or_default();

        // keep intermediate merged data as Option<f64> so we can represent uncovered cells
        let mut merged_bands: Vec<(usize, Vec<Option<f64>>)> = requested_band_indexes
            .iter()
            .map(|band_index| (*band_index, vec![None; width * height]))
            .collect();

        for (dataset, intersection) in intersections {
            let band_indexes = dataset.raster.resolve_band_indexes(&band_selection);

            // skip dataset if its resolved band set does not match expected result schema
            if band_indexes != requested_band_indexes {
                tracing::debug!(
                    dataset_id = %dataset.dataset_id,
                    ?band_indexes,
                    ?requested_band_indexes,
                    "skipping dataset with incompatible resolved band indexes"
                );
                continue;
            }

            let (raster_read_query, target_placement) = create_raster_processing_plan(
                &intersection,
                &bbox,
                &dataset,
                width,
                height,
                band_indexes,
            )
            .ok_or(GeorasterServiceError::RasterPlan)?;

            let target_width = raster_read_query.target_size().width();
            let target_height = raster_read_query.target_size().height();
            let target_base_col = target_placement.column();
            let target_base_row = target_placement.row();

            let raster_grid = self
                .raster_reader
                .read_window(&dataset.artifact_path, raster_read_query.clone())
                .await
                .map_err(|err| {
                    tracing::error!(
                        error = %err,
                        dataset_id = %dataset.dataset_id,
                        artifact = %dataset.artifact_path,
                        raster_query = ?raster_read_query,
                        "failed to read raster window"
                    );

                    GeorasterServiceError::RasterRead
                })?;

            // merge each returned band into corresponding result band
            for source_band in raster_grid.bands() {
                let Some((_, target_band_values)) = merged_bands
                    .iter_mut()
                    .find(|(band_index, _)| *band_index == source_band.band_index())
                else {
                    continue;
                };

                // nodata is resolved per band
                let nodata = dataset
                    .raster
                    .bands
                    .iter()
                    .find(|band| band.band_index == source_band.band_index())
                    .and_then(|band| band.nodata);

                for row in 0..target_height {
                    let target_row = target_base_row + row;
                    if target_row >= height {
                        continue;
                    }

                    for col in 0..target_width {
                        let target_col = target_base_col + col;
                        if target_col >= width {
                            continue;
                        }

                        let source_idx = row * target_width + col;
                        let target_idx = target_row * width + target_col;

                        let Some(value) = source_band.data().get(source_idx).copied() else {
                            continue;
                        };

                        if nodata == Some(value) {
                            continue;
                        }

                        // later datasets overwrite earlier ones;
                        // because we sorted from lower quality to higher quality,
                        // higher quality data wins
                        target_band_values[target_idx] = Some(value);
                    }
                }
            }
        }

        // convert merged optional data into final raster bands
        let bands = merged_bands
            .into_iter()
            .map(|(band_index, values)| {
                let values = values
                    .into_iter()
                    .map(|v| v.unwrap_or_default())
                    .collect::<Vec<_>>();
                RasterBand::new(band_index, values)
            })
            .collect::<Vec<_>>();

        RasterGrid::try_new(width, height, bands).map_err(|err| {
            tracing::error!(
                error = %err,
                width,
                height,
                "failed to build resulting raster grid"
            );
            GeorasterServiceError::RasterRead
        })
    }
}

/// Converts geographic coordinate into raster column and row, based on metadata.
///
/// Returns `None` if calculated coordinate is non finite or falls outside
/// raster bounds.
fn lonlat_to_raster_coord(
    metadata: &DatasetMetadata,
    lon: f64,
    lat: f64,
) -> Option<WindowPlacement> {
    let gt = &metadata.raster.geo_transform;

    let col = ((lon - gt.origin_lon) / gt.pixel_width).floor();
    let row = ((lat - gt.origin_lat) / gt.pixel_height).floor();

    if !col.is_finite() || !row.is_finite() {
        tracing::debug!(
            lon,
            lat,
            origin_lon = gt.origin_lon,
            origin_lat = gt.origin_lat,
            pixel_width = gt.pixel_width,
            pixel_height = gt.pixel_height,
            col,
            row,
            "pixel coordinate produced non finite values"
        );
        return None;
    }

    let col = col as i64;
    let row = row as i64;

    if col < 0 || row < 0 {
        tracing::debug!(col, row, "requested coordinates are less than 0");
        return None;
    }

    let col = col as usize;
    let row = row as usize;

    if col >= metadata.raster.width || row >= metadata.raster.height {
        tracing::debug!(
            col,
            row,
            width = metadata.raster.width,
            height = metadata.raster.height,
            "requested coordinates are out of bounds"
        );
        return None;
    }

    Some(WindowPlacement::new(col, row))
}

/// Builds raster read window and target placement for intersecting bbox.
///
/// Returns `None` when source or target coordinates cannot be mapped to valid window.
fn create_raster_processing_plan(
    intersection: &Bounds,
    requested_bbox: &Bounds,
    dataset: &DatasetMetadata,
    final_width: usize,
    final_height: usize,
    bands: Vec<usize>,
) -> Option<(RasterReadQuery, WindowPlacement)> {
    if final_width == 0 || final_height == 0 {
        return None;
    }

    let gt = &dataset.raster.geo_transform;

    // compute destination grid resolution in geographic units
    let lon_step = (requested_bbox.max_lon() - requested_bbox.min_lon()) / final_width as f64;
    let lat_step = (requested_bbox.max_lat() - requested_bbox.min_lat()) / final_height as f64;

    // map intersection bounds into source raster column range
    let source_start_col =
        ((intersection.min_lon() - gt.origin_lon) / gt.pixel_width).floor() as isize;
    let source_end_col_exclusive =
        ((intersection.max_lon() - gt.origin_lon) / gt.pixel_width).ceil() as isize;

    // map intersection bounds into source raster column range
    let source_start_row =
        ((gt.origin_lat - intersection.max_lat()) / gt.pixel_height.abs()).floor() as isize;
    let source_end_row_exclusive =
        ((gt.origin_lat - intersection.min_lat()) / gt.pixel_height.abs()).ceil() as isize;

    if source_start_col < 0
        || source_start_row < 0
        || source_end_col_exclusive < 0
        || source_end_row_exclusive < 0
    {
        return None;
    }

    // clamp source range to raster dimensions
    let source_start_col = source_start_col as usize;
    let source_start_row = source_start_row as usize;
    let source_end_col_exclusive = (source_end_col_exclusive as usize).min(dataset.raster.width);
    let source_end_row_exclusive = (source_end_row_exclusive as usize).min(dataset.raster.height);

    // reject ranges where start falls completely outside raster bounds
    if source_start_col >= dataset.raster.width || source_start_row >= dataset.raster.height {
        return None;
    }

    // reject empty source window
    if source_end_col_exclusive <= source_start_col || source_end_row_exclusive <= source_start_row
    {
        return None;
    }

    // map intersection in destination result grid
    let target_start_col =
        ((intersection.min_lon() - requested_bbox.min_lon()) / lon_step).floor() as isize;
    let target_end_col_exclusive =
        ((intersection.max_lon() - requested_bbox.min_lon()) / lon_step).ceil() as isize;

    let target_start_row =
        ((requested_bbox.max_lat() - intersection.max_lat()) / lat_step).floor() as isize;
    let target_end_row_exclusive =
        ((requested_bbox.max_lat() - intersection.min_lat()) / lat_step).ceil() as isize;

    if target_start_col < 0
        || target_start_row < 0
        || target_end_col_exclusive < 0
        || target_end_row_exclusive < 0
    {
        return None;
    }

    // clamp target range to final result dimensions
    let target_start_col = target_start_col as usize;
    let target_start_row = target_start_row as usize;
    let target_end_col_exclusive = (target_end_col_exclusive as usize).min(final_width);
    let target_end_row_exclusive = (target_end_row_exclusive as usize).min(final_height);

    if target_end_col_exclusive <= target_start_col || target_end_row_exclusive <= target_start_row
    {
        return None;
    }

    // build source raster window and destination placement
    let placement = WindowPlacement::new(source_start_col, source_start_row);
    let source_size = RasterSize::new(
        source_end_col_exclusive - source_start_col,
        source_end_row_exclusive - source_start_row,
    );
    let target_size = RasterSize::new(
        target_end_col_exclusive - target_start_col,
        target_end_row_exclusive - target_start_row,
    );

    let target_placement = WindowPlacement::new(target_start_col, target_start_row);

    Some((
        RasterReadQuery::new(placement, source_size, target_size, bands),
        target_placement,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use georaster_domain::{
        ArtifactLocator, BandSelection, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform,
        MetadataStorage, MetadataStorageError, RasterBand, RasterBandMetadata, RasterGrid,
        RasterMetadata, RasterPointBand, RasterReader, RasterReaderError, RasterRepresentation,
    };

    #[derive(Clone, Default)]
    struct FakeMetadataStorage {
        datasets: Vec<DatasetMetadata>,
        should_fail: bool,
    }

    impl MetadataStorage for FakeMetadataStorage {
        async fn load_metadata(&self) -> Result<Vec<DatasetMetadata>, MetadataStorageError> {
            if self.should_fail {
                return Err(MetadataStorageError::Load);
            }

            Ok(self.datasets.clone())
        }

        async fn save_metadata(
            &self,
            _metadata: DatasetMetadata,
        ) -> Result<(), MetadataStorageError> {
            todo!()
        }
    }

    #[derive(Debug, Clone)]
    struct FakeRasterReaderData {
        artifact_path: String,
        window: RasterReadQuery,
        result: RasterGrid,
    }

    #[derive(Clone, Default)]
    struct FakeRasterReader {
        reads: Arc<Mutex<Vec<(String, RasterReadQuery)>>>,
        responses: Vec<FakeRasterReaderData>,
        should_fail: bool,
    }

    impl FakeRasterReader {
        fn recorded_reads(&self) -> Vec<(String, RasterReadQuery)> {
            self.reads.lock().unwrap().clone()
        }
    }

    impl RasterReader for FakeRasterReader {
        async fn read_window(
            &self,
            artifact_path: &ArtifactLocator,
            window: RasterReadQuery,
        ) -> Result<RasterGrid, RasterReaderError> {
            self.reads
                .lock()
                .unwrap()
                .push((artifact_path.to_string(), window.clone()));

            if self.should_fail {
                return Err(RasterReaderError::Read);
            }

            let response = self
                .responses
                .iter()
                .find(|candidate| {
                    candidate.artifact_path == artifact_path.as_ref() && candidate.window == window
                })
                .unwrap_or_else(|| {
                    panic!("unexpected raster read: path={artifact_path}, window={window:?}")
                });

            Ok(response.result.clone())
        }
    }

    fn dataset(
        dataset_id: &str,
        artifact_path: &str,
        bounds: Bounds,
        pixel_width: f64,
        pixel_height: f64,
        nodata: Option<f64>,
    ) -> DatasetMetadata {
        DatasetMetadata {
            dataset_id: dataset_id.to_string(),
            artifact_path: ArtifactLocator::new(artifact_path),
            raster: RasterMetadata {
                crs: Crs::new("Test"),
                width: ((bounds.max_lon() - bounds.min_lon()) / pixel_width.abs()).ceil() as usize,
                height: ((bounds.max_lat() - bounds.min_lat()) / pixel_height.abs()).ceil()
                    as usize,
                geo_transform: GeoTransform {
                    origin_lon: bounds.min_lon(),
                    origin_lat: bounds.max_lat(),
                    pixel_width,
                    pixel_height,
                },
                bounds,
                overview_count: 0,
                raster_representation: RasterRepresentation::Grayscale,
                bands: vec![RasterBandMetadata {
                    band_index: 1,
                    nodata,
                    block_size: BlockSize {
                        width: 1,
                        height: 1,
                    },
                    color_interpretation: "Gray".to_string(),
                }],
            },
        }
    }

    fn grid(width: usize, height: usize, bands: Vec<RasterBand>) -> RasterGrid {
        RasterGrid::try_new(width, height, bands).unwrap()
    }

    fn bbox(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Bounds {
        Bounds::try_new(min_lon, min_lat, max_lon, max_lat).unwrap()
    }

    #[tokio::test]
    async fn raster_data_at_point_returns_none_when_no_dataset_contains_point() {
        let metadata = FakeMetadataStorage {
            datasets: vec![dataset(
                "ds-1",
                "a.tif",
                bbox(10.0, 10.0, 12.0, 12.0),
                1.0,
                -1.0,
                None,
            )],
            should_fail: false,
        };

        let raster = FakeRasterReader::default();
        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_at_point(
                1.0,
                1.0,
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn raster_data_at_point_returns_point_for_single_band() {
        let metadata = FakeMetadataStorage {
            datasets: vec![dataset(
                "ds-1",
                "a.tif",
                bbox(0.0, 0.0, 2.0, 2.0),
                1.0,
                -1.0,
                None,
            )],
            should_fail: false,
        };

        let raster_query = RasterReadQuery::new_point(WindowPlacement::new(1, 1), vec![1]);

        let raster = FakeRasterReader {
            responses: vec![FakeRasterReaderData {
                artifact_path: "a.tif".to_string(),
                window: raster_query.clone(),
                result: grid(1, 1, vec![RasterBand::new(1, vec![42.0])]),
            }],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster.clone());

        let result = service
            .raster_data_at_point(
                1.0,
                1.0,
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            result,
            RasterPoint::new(vec![RasterPointBand::new(1, 42.0)])
        );
        assert_eq!(
            raster.recorded_reads(),
            vec![("a.tif".to_string(), raster_query)]
        );
    }

    #[tokio::test]
    async fn raster_data_at_point_returns_none_when_value_is_nodata() {
        let metadata = FakeMetadataStorage {
            datasets: vec![dataset(
                "ds-1",
                "a.tif",
                bbox(0.0, 0.0, 2.0, 2.0),
                1.0,
                -1.0,
                Some(0.0),
            )],
            should_fail: false,
        };

        let raster_query = RasterReadQuery::new_point(WindowPlacement::new(1, 1), vec![1]);

        let raster = FakeRasterReader {
            responses: vec![FakeRasterReaderData {
                artifact_path: "a.tif".to_string(),
                window: raster_query,
                result: grid(1, 1, vec![RasterBand::new(1, vec![0.0])]),
            }],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_at_point(
                1.0,
                1.0,
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn raster_data_in_bbox_returns_empty_grid_when_no_dataset_intersects() {
        let requested_bbox = bbox(0.0, 0.0, 2.0, 2.0);

        let metadata = FakeMetadataStorage {
            datasets: vec![dataset(
                "ds-1",
                "a.tif",
                bbox(10.0, 10.0, 12.0, 12.0),
                1.0,
                -1.0,
                None,
            )],
            should_fail: false,
        };

        let raster = FakeRasterReader::default();
        let service = GeorasterService::new(metadata, raster.clone());

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::Resolution {
                    x_resolution: 1.0,
                    y_resolution: 1.0,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 2);
        assert_eq!(result.height(), 2);
        assert!(result.bands().is_empty());
        assert!(raster.recorded_reads().is_empty());
    }

    #[tokio::test]
    async fn raster_data_in_bbox_returns_values_from_single_covering_dataset() {
        let requested_bbox = bbox(0.0, 0.0, 2.0, 2.0);

        let metadata = FakeMetadataStorage {
            datasets: vec![dataset("ds-1", "a.tif", requested_bbox, 1.0, -1.0, None)],
            should_fail: false,
        };

        let raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
            vec![1],
        );

        let raster = FakeRasterReader {
            responses: vec![FakeRasterReaderData {
                artifact_path: "a.tif".to_string(),
                window: raster_read_query.clone(),
                result: grid(2, 2, vec![RasterBand::new(1, vec![1.0, 2.0, 3.0, 4.0])]),
            }],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster.clone());

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::Resolution {
                    x_resolution: 1.0,
                    y_resolution: 1.0,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 2);
        assert_eq!(result.height(), 2);
        assert_eq!(result.bands().len(), 1);
        assert_eq!(result.band(1).unwrap().data(), &[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(
            raster.recorded_reads(),
            vec![("a.tif".to_string(), raster_read_query)]
        );
    }

    #[tokio::test]
    async fn raster_data_in_bbox_merges_disjoint_dataset_contributions() {
        let requested_bbox = bbox(0.0, 0.0, 4.0, 2.0);

        let left = dataset(
            "left",
            "left.tif",
            bbox(0.0, 0.0, 2.0, 2.0),
            1.0,
            -1.0,
            None,
        );

        let right = dataset(
            "right",
            "right.tif",
            bbox(2.0, 0.0, 4.0, 2.0),
            1.0,
            -1.0,
            None,
        );

        let metadata = FakeMetadataStorage {
            datasets: vec![left, right],
            should_fail: false,
        };

        let left_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
            vec![1],
        );

        let right_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
            vec![1],
        );

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "left.tif".to_string(),
                    window: left_raster_read_query,
                    result: grid(2, 2, vec![RasterBand::new(1, vec![10.0, 10.0, 10.0, 10.0])]),
                },
                FakeRasterReaderData {
                    artifact_path: "right.tif".to_string(),
                    window: right_raster_read_query,
                    result: grid(2, 2, vec![RasterBand::new(1, vec![20.0, 20.0, 20.0, 20.0])]),
                },
            ],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::Resolution {
                    x_resolution: 1.0,
                    y_resolution: 1.0,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 4);
        assert_eq!(result.height(), 2);
        assert_eq!(
            result.band(1).unwrap().data(),
            &[10.0, 10.0, 20.0, 20.0, 10.0, 10.0, 20.0, 20.0]
        );
    }

    #[tokio::test]
    async fn raster_data_in_bbox_does_not_overwrite_real_value_with_nodata() {
        let requested_bbox = bbox(0.0, 0.0, 2.0, 2.0);

        let low_quality = dataset("low", "low.tif", requested_bbox, 1.0, -1.0, None);

        let high_quality = dataset(
            "high",
            "high.tif",
            bbox(1.0, 1.0, 2.0, 2.0),
            1.0,
            -1.0,
            Some(0.0),
        );

        let metadata = FakeMetadataStorage {
            datasets: vec![low_quality, high_quality],
            should_fail: false,
        };

        let low_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
            vec![1],
        );

        let high_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(1, 1),
            RasterSize::new(1, 1),
            vec![1],
        );

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "low.tif".to_string(),
                    window: low_raster_read_query,
                    result: grid(2, 2, vec![RasterBand::new(1, vec![10.0, 10.0, 10.0, 10.0])]),
                },
                FakeRasterReaderData {
                    artifact_path: "high.tif".to_string(),
                    window: high_raster_read_query,
                    result: grid(1, 1, vec![RasterBand::new(1, vec![0.0])]),
                },
            ],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::Resolution {
                    x_resolution: 1.0,
                    y_resolution: 1.0,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 2);
        assert_eq!(result.height(), 2);
        assert_eq!(result.band(1).unwrap().data(), &[10.0, 10.0, 10.0, 10.0]);
    }

    #[tokio::test]
    async fn raster_data_in_bbox_uses_exact_output_size() {
        let requested_bbox = bbox(0.0, 0.0, 4.0, 4.0);

        let metadata = FakeMetadataStorage {
            datasets: vec![dataset("ds-1", "a.tif", requested_bbox, 1.0, -1.0, None)],
            should_fail: false,
        };

        let raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(4, 4),
            RasterSize::new(2, 2),
            vec![1],
        );

        let raster = FakeRasterReader {
            responses: vec![FakeRasterReaderData {
                artifact_path: "a.tif".to_string(),
                window: raster_read_query,
                result: grid(2, 2, vec![RasterBand::new(1, vec![1.0, 2.0, 3.0, 4.0])]),
            }],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::OutputSize {
                    width: 2,
                    height: 2,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 2);
        assert_eq!(result.height(), 2);
        assert_eq!(result.band(1).unwrap().data(), &[1.0, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn raster_data_in_bbox_gets_data_from_two_datasets_with_different_resolution() {
        let requested_bbox = bbox(3.0, 0.0, 6.0, 3.0);

        let left = dataset("left", "left.tif", bbox(0.0, 0.0, 4.0, 8.0), 1.0, 1.0, None);
        let right = dataset(
            "right",
            "right.tif",
            bbox(4.0, 0.0, 8.0, 8.0),
            2.0,
            -2.0,
            None,
        );

        let metadata = FakeMetadataStorage {
            datasets: vec![left, right],
            should_fail: false,
        };

        let left_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(3, 5),
            RasterSize::new(1, 3),
            RasterSize::new(1, 3),
            vec![1],
        );

        let right_raster_read_query = RasterReadQuery::new(
            WindowPlacement::new(0, 2),
            RasterSize::new(1, 2),
            RasterSize::new(2, 3),
            vec![1],
        );

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "left.tif".to_string(),
                    window: left_raster_read_query,
                    result: grid(1, 3, vec![RasterBand::new(1, vec![10.0, 10.0, 10.0])]),
                },
                FakeRasterReaderData {
                    artifact_path: "right.tif".to_string(),
                    window: right_raster_read_query,
                    result: grid(
                        2,
                        3,
                        vec![RasterBand::new(1, vec![20.0, 20.0, 20.0, 20.0, 20.0, 20.0])],
                    ),
                },
            ],
            ..Default::default()
        };

        let service = GeorasterService::new(metadata, raster);

        let result = service
            .raster_data_in_bbox(
                requested_bbox,
                Some(GeorasterSampling::Resolution {
                    x_resolution: 1.0,
                    y_resolution: 1.0,
                }),
                BandSelection::First,
                RasterRepresentation::Grayscale,
            )
            .await
            .unwrap();

        assert_eq!(result.width(), 3);
        assert_eq!(result.height(), 3);
        assert_eq!(
            result.band(1).unwrap().data(),
            &[10.0, 20.0, 20.0, 10.0, 20.0, 20.0, 10.0, 20.0, 20.0]
        );
    }
}
