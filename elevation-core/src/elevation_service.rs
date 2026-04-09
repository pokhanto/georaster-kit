use elevation_domain::{
    BboxElevations, Bounds, DatasetMetadata, Elevation, MetadataStorage, RasterReadWindow,
    RasterReader, RasterSize, ResolutionHint, WindowPlacement,
};

// TODO: Add more error variants
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationServiceError {
    #[error("Can't read metadata")]
    Metadata,

    #[error("Can't read raster")]
    Raster,
}

/// Service for resolving elevations from raster data using dataset metadata.
#[derive(Debug, Clone)]
pub struct ElevationService<M, R> {
    metadata: M,
    raster: R,
}

impl<M, R> ElevationService<M, R> {
    /// Creates new elevation service with metadata storage and raster reader.
    pub fn new(metadata: M, raster: R) -> Self {
        Self { metadata, raster }
    }
}

impl<M, R> ElevationService<M, R>
where
    M: MetadataStorage,
    R: RasterReader<f64>,
{
    /// Returns elevation at given geographic point.
    ///
    /// Service:
    /// - loads available dataset metadata,
    /// - selects dataset whose bounds contain requested point,
    /// - converts geographic coordinate into raster pixel coordinates,
    /// - reads single raster cell,
    /// - returns its value as [`Elevation`].
    ///
    /// If no dataset contains point, or if resolved raster value equals
    /// dataset's `nodata` value, this method returns `Ok(None)`.
    ///
    /// # Parameters
    ///
    /// - `lon`: Longitude of requested point.
    /// - `lat`: Latitude of requested point.
    ///
    /// # Returns
    ///
    /// Returns:
    /// - `Ok(Some(Elevation))` when valid elevation value is found,
    /// - `Ok(None)` when:
    ///   - no dataset covers point,
    ///   - point resolves outside raster bounds,
    ///   - raster value is `nodata`,
    /// - `Err(ElevationServiceError)` on metadata or raster read failure.
    ///
    /// # Errors
    ///
    /// Returns [`ElevationServiceError::Metadata`] if dataset metadata cannot be loaded.
    ///
    /// Returns [`ElevationServiceError::Raster`] if raster cell cannot be read.
    ///
    /// # Notes
    ///
    /// When multiple datasets contain requested point, dataset selection is
    /// delegated to internal dataset resolution logic.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let elevation = service.elevation_at_point(30.5234, 50.4501)?;
    ///
    /// match elevation {
    ///     Some(value) => println!("Elevation: {}", value.0),
    ///     None => println!("No elevation data for point"),
    /// }
    /// ```
    #[tracing::instrument(skip(self), fields(lon, lat))]
    pub async fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> Result<Option<Elevation>, ElevationServiceError> {
        tracing::info!(lon, lat, "starting getting elevation at point");
        let datasets = self.metadata.load_metadata().await.map_err(|err| {
            tracing::error!(
                error = %err,
                lon = lon,
                lat = lat,
                "failed to load dataset metadata"
            );

            ElevationServiceError::Metadata
        })?;
        let dataset = match get_dataset_for_point(datasets, lon, lat) {
            Some(dataset) => dataset,
            None => return Ok(None),
        };
        tracing::info!(dataset_id = %dataset.dataset_id, "dataset selected");

        let pixel_placement = match lonlat_to_raster_coord(&dataset, lon, lat) {
            Some(pixel) => pixel,
            None => return Ok(None),
        };

        tracing::info!(
            col = pixel_placement.column(),
            row = pixel_placement.row(),
            "pixel resolved"
        );

        let elevation_data = self
            .raster
            .read_window(
                &dataset.artifact_path,
                RasterReadWindow::new_point(pixel_placement),
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    dataset_id = %dataset.dataset_id,
                    artifact = %dataset.artifact_path,
                    lon = lon,
                    lat = lat,
                    col = pixel_placement.column(),
                    row = pixel_placement.row(),
                    "failed to read raster pixel"
                );

                ElevationServiceError::Raster
            })?;

        let elevation_value = elevation_data.get(0, 0).copied();

        if dataset.raster.nodata == elevation_value {
            tracing::info!("elevation at point resolved to be nodata");
            return Ok(None);
        }

        tracing::info!(elevation = ?elevation_value, "elevation at point resolved");
        Ok(elevation_value.map(Elevation))
    }

    /// Returns grid of elevations for requested bounding box.
    ///
    /// Service:
    /// - loads available dataset metadata,
    /// - finds all datasets whose bounds intersect requested `bbox`,
    /// - selects output resolution using `resolution_hint`,
    /// - reads corresponding raster windows,
    /// - merges them into one resulting elevation grid.
    ///
    /// Output values is grid and returned in order:
    /// `values[row * width + column]`.
    ///
    /// When multiple datasets overlap lower-resolution datasets are processed first,
    /// so higher-resolution datasets can overwrite them in result.
    ///
    /// `ResolutionHint` controls resolution(size) of resulting grid:
    /// - `Highest` picks the finest resolution among intersecting datasets,
    /// - `Lowest` picks the coarsest resolution among intersecting datasets,
    /// - `Degrees { .. }` uses explicit requested resolution.
    ///
    /// Cells with raster value equals dataset's `nodata` are left empty (`None`).
    ///
    /// # Parameters
    ///
    /// - `bbox`: Requested geographic area in dataset coordinate space.
    /// - `resolution_hint`: Optional hint controlling resolution(size) of resulting grid.
    ///
    /// # Returns
    ///
    /// [`BboxElevations`] containing:
    /// - requested bounding box,
    /// - resulting grid width,
    /// - resulting grid height,
    /// - flattened elevation values.
    ///
    /// # Errors
    ///
    /// Returns [`ElevationServiceError::Metadata`] if:
    /// - dataset metadata cannot be loaded,
    /// - resolution cannot be resolved from intersecting datasets,
    /// - raster processing plan cannot be created.
    ///
    /// Returns [`ElevationServiceError::Raster`] if raster data cannot be read.
    ///
    /// # Notes
    ///
    /// Current implementation assumes resolution in geographic degree units.
    /// This may not be correct for all CRS types.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let elevations = service.elevations_in_bbox(
    ///     Bounds::new(30.0, 50.0, 31.0 51.0),
    ///     Some(ResolutionHint::Highest),
    /// )?;
    ///
    /// assert!(elevations.width > 0);
    /// assert!(elevations.height > 0);
    /// ```
    #[tracing::instrument(skip(self), fields(bbox, resolution))]
    pub async fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        resolution_hint: Option<ResolutionHint>,
    ) -> Result<BboxElevations, ElevationServiceError> {
        tracing::info!(bbox = ?bbox, resolution_hint = ?resolution_hint, "starting getting elevations in bbox with resolution");

        let datasets = self.metadata.load_metadata().await.map_err(|err| {
            tracing::error!(
                error = %err,
                bbox = ?bbox,
                resolution_hint = ?resolution_hint,
                "failed to load dataset metadata"
            );

            ElevationServiceError::Metadata
        })?;

        let mut intersections: Vec<(DatasetMetadata, Bounds)> = datasets
            .into_iter()
            .filter_map(|dataset| {
                dataset
                    .raster
                    .bounds
                    .intersection(&bbox)
                    .map(|intersection| (dataset, intersection))
            })
            .collect();

        let resolution_degrees = match resolution_hint.unwrap_or(ResolutionHint::Highest) {
            ResolutionHint::Highest => intersections
                .iter()
                .map(|(dataset, _)| {
                    (
                        dataset.raster.geo_transform.pixel_width.abs(),
                        dataset.raster.geo_transform.pixel_height.abs(),
                    )
                })
                .min_by(|(lon_a, lat_a), (lon_b, lat_b)| {
                    let area_a = lon_a * lat_a;
                    let area_b = lon_b * lat_b;
                    area_a
                        .partial_cmp(&area_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .ok_or(ElevationServiceError::Metadata)?,
            ResolutionHint::Lowest => intersections
                .iter()
                .map(|(dataset, _)| {
                    (
                        dataset.raster.geo_transform.pixel_width.abs(),
                        dataset.raster.geo_transform.pixel_height.abs(),
                    )
                })
                .max_by(|(lon_a, lat_a), (lon_b, lat_b)| {
                    let area_a = lon_a * lat_a;
                    let area_b = lon_b * lat_b;
                    area_a
                        .partial_cmp(&area_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .ok_or(ElevationServiceError::Metadata)?,
            ResolutionHint::Degrees {
                lon_resolution,
                lat_resolution,
            } => (lon_resolution, lat_resolution),
        };

        let width = ((bbox.max_lon() - bbox.min_lon()) / resolution_degrees.0).ceil() as usize;
        let height = ((bbox.max_lat() - bbox.min_lat()) / resolution_degrees.1).ceil() as usize;

        let mut values = vec![None; width * height];
        let mut covered = vec![0_u8; width * height];

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

        for (dataset, intersection) in intersections {
            let (raster_read_window, target_placement) =
                create_raster_processing_plan(&intersection, &bbox, &dataset, width, height)
                    .ok_or(ElevationServiceError::Metadata)?;

            let target_width = raster_read_window.target_size().width();
            let target_height = raster_read_window.target_size().height();
            let target_base_col = target_placement.column();
            let target_base_row = target_placement.row();

            // skip whole intersection if its target rectangle is already fully covered
            let mut fully_covered = true;
            for row in 0..target_height {
                let target_row = target_base_row + row;
                if target_row >= height {
                    fully_covered = false;
                    break;
                }

                let target_row_start = target_row * width + target_base_col;
                let target_row_end =
                    (target_row_start + target_width).min((target_row + 1) * width);

                if target_row_start >= target_row_end {
                    fully_covered = false;
                    break;
                }

                if covered[target_row_start..target_row_end].contains(&0) {
                    fully_covered = false;
                    break;
                }
            }

            if fully_covered {
                tracing::debug!(
                    dataset_id = %dataset.dataset_id,
                    "skipping fully covered intersection"
                );
                continue;
            }

            let raster_data = self
                .raster
                .read_window(&dataset.artifact_path, raster_read_window)
                .await
                .map_err(|err| {
                    tracing::error!(
                        error = %err,
                        dataset_id = %dataset.dataset_id,
                        artifact = %dataset.artifact_path,
                        raster_window = ?raster_read_window,
                        "failed to read raster window"
                    );

                    ElevationServiceError::Raster
                })?;

            for row in 0..raster_data.target_height() {
                let target_row = target_base_row + row;
                if target_row >= height {
                    continue;
                }

                let row_start = target_row * width + target_base_col;
                let row_end =
                    (row_start + raster_data.target_width()).min((target_row + 1) * width);

                if row_start >= row_end {
                    continue;
                }

                // skip whole row if it is already fully covered
                if covered[row_start..row_end].iter().all(|&cell| cell == 1) {
                    continue;
                }

                for col in 0..raster_data.target_width() {
                    let target_column = target_base_col + col;
                    if target_column >= width {
                        continue;
                    }

                    let target_index = target_row * width + target_column;

                    // lower quality datasets only fill gaps
                    if covered[target_index] == 1 {
                        continue;
                    }

                    let raster_value = raster_data.get(col, row).copied();

                    if let Some(value) = raster_value {
                        if dataset.raster.nodata == Some(value) {
                            continue;
                        }

                        values[target_index] = Some(Elevation(value));
                        covered[target_index] = 1;
                    }
                }
            }
        }

        Ok(BboxElevations {
            bbox,
            width,
            height,
            values,
        })
    }
}

fn get_dataset_for_point(
    datasets: Vec<DatasetMetadata>,
    lon: f64,
    lat: f64,
) -> Option<DatasetMetadata> {
    // TODO: consider filter by quality too
    let mut filtered: Vec<DatasetMetadata> = datasets
        .into_iter()
        .filter(|ds| ds.raster.bounds.contains_point(lon, lat))
        .collect();

    filtered.pop()
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

    let col = ((lon - gt.origin_lon) / gt.pixel_width).floor().abs();
    let row = ((lat - gt.origin_lat) / gt.pixel_height).floor().abs();

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

    if col > metadata.raster.width || row > metadata.raster.height {
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

/// Builds raster read window and target placement for an intersecting bbox.
///
/// Returns `None` when source or target coordinates cannot be mapped to valid window.
fn create_raster_processing_plan(
    intersection: &Bounds,
    requested_bbox: &Bounds,
    dataset: &DatasetMetadata,
    final_width: usize,
    final_height: usize,
) -> Option<(RasterReadWindow, WindowPlacement)> {
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
        RasterReadWindow::new(placement, source_size, target_size),
        target_placement,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use elevation_domain::{
        ArtifactLocator, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform, MetadataStorage,
        MetadataStorageError, RasterMetadata, RasterReader, RasterReaderError, RasterWindowData,
        ResolutionHint,
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
        /// Expected to be passed by service to raster reader
        artifact_path: String,
        /// Expected to be passed by service to raster reader
        window: RasterReadWindow,
        /// Result to return from fake raster reader
        result: RasterWindowData<f64>,
    }

    #[derive(Clone, Default)]
    struct FakeRasterReader {
        reads: Arc<Mutex<Vec<(String, RasterReadWindow)>>>,
        responses: Vec<FakeRasterReaderData>,
        should_fail: bool,
    }

    impl FakeRasterReader {
        fn recorded_reads(&self) -> Vec<(String, RasterReadWindow)> {
            self.reads.lock().unwrap().clone()
        }
    }

    impl RasterReader<f64> for FakeRasterReader {
        async fn read_window(
            &self,
            artifact_path: &ArtifactLocator,
            window: RasterReadWindow,
        ) -> Result<RasterWindowData<f64>, RasterReaderError> {
            self.reads
                .lock()
                .unwrap()
                .push((artifact_path.to_string(), window));

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
                bounds,
                width: ((bounds.max_lon() - bounds.min_lon()) / pixel_width.abs()).ceil() as usize,
                height: ((bounds.max_lat() - bounds.min_lat()) / pixel_height.abs()).ceil()
                    as usize,
                nodata,
                geo_transform: GeoTransform {
                    origin_lon: bounds.min_lon(),
                    origin_lat: bounds.max_lat(),
                    pixel_width,
                    pixel_height,
                },
                block_size: BlockSize {
                    width: 1,
                    height: 1,
                },
                overview_count: 0,
                crs: Crs::new("Test"),
            },
        }
    }

    fn window_data(
        raster_read_window: RasterReadWindow,
        values: Vec<f64>,
    ) -> RasterWindowData<f64> {
        RasterWindowData::try_new(raster_read_window, values).unwrap()
    }

    fn bbox(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Bounds {
        Bounds::new(min_lon, min_lat, max_lon, max_lat).unwrap()
    }

    #[tokio::test]
    async fn elevations_in_bbox_returns_empty_grid_when_no_dataset_intersects() {
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
        let service = ElevationService::new(metadata, raster.clone());

        let result = service
            .elevations_in_bbox(
                requested_bbox,
                Some(ResolutionHint::Degrees {
                    lon_resolution: 1.0,
                    lat_resolution: 1.0,
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.bbox, requested_bbox);
        assert_eq!(result.width, 2);
        assert_eq!(result.height, 2);
        assert_eq!(result.values, vec![None, None, None, None]);
        assert!(raster.recorded_reads().is_empty());
    }

    #[tokio::test]
    async fn elevations_in_bbox_returns_values_from_single_covering_dataset() {
        let requested_bbox = bbox(0.0, 0.0, 2.0, 2.0);

        let metadata = FakeMetadataStorage {
            datasets: vec![dataset("ds-1", "a.tif", requested_bbox, 1.0, -1.0, None)],
            should_fail: false,
        };

        let raster_read_window = RasterReadWindow::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
        );

        let raster = FakeRasterReader {
            responses: vec![FakeRasterReaderData {
                artifact_path: "a.tif".to_string(),
                window: raster_read_window,
                result: window_data(raster_read_window, vec![1.0, 2.0, 3.0, 4.0]),
            }],
            ..Default::default()
        };

        let service = ElevationService::new(metadata, raster.clone());

        let result = service
            .elevations_in_bbox(
                requested_bbox,
                Some(ResolutionHint::Degrees {
                    lon_resolution: 1.0,
                    lat_resolution: 1.0,
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.bbox, requested_bbox);
        assert_eq!(result.width, 2);
        assert_eq!(result.height, 2);
        assert_eq!(
            result.values,
            vec![
                Some(Elevation(1.0)),
                Some(Elevation(2.0)),
                Some(Elevation(3.0)),
                Some(Elevation(4.0)),
            ]
        );
        assert_eq!(
            raster.recorded_reads(),
            vec![("a.tif".to_string(), raster_read_window)]
        );
    }

    #[tokio::test]
    async fn elevations_in_bbox_merges_disjoint_dataset_contributions() {
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

        let left_raster_read_window = RasterReadWindow::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
        );

        let right_raster_read_window = RasterReadWindow::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
        );

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "left.tif".to_string(),
                    window: left_raster_read_window,
                    result: window_data(left_raster_read_window, vec![10.0, 10.0, 10.0, 10.0]),
                },
                FakeRasterReaderData {
                    artifact_path: "right.tif".to_string(),
                    window: right_raster_read_window,
                    result: window_data(right_raster_read_window, vec![20.0, 20.0, 20.0, 20.0]),
                },
            ],
            ..Default::default()
        };

        let service = ElevationService::new(metadata, raster);

        let result = service
            .elevations_in_bbox(
                requested_bbox,
                Some(ResolutionHint::Degrees {
                    lon_resolution: 1.0,
                    lat_resolution: 1.0,
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.width, 4);
        assert_eq!(result.height, 2);
        assert_eq!(
            result.values,
            vec![
                Some(Elevation(10.0)),
                Some(Elevation(10.0)),
                Some(Elevation(20.0)),
                Some(Elevation(20.0)),
                Some(Elevation(10.0)),
                Some(Elevation(10.0)),
                Some(Elevation(20.0)),
                Some(Elevation(20.0)),
            ]
        );
    }

    #[tokio::test]
    async fn elevations_in_bbox_does_not_overwrite_real_value_with_nodata() {
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

        let low_raster_read_window = RasterReadWindow::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(2, 2),
            RasterSize::new(2, 2),
        );

        let high_raster_read_window = RasterReadWindow::new(
            WindowPlacement::new(0, 0),
            RasterSize::new(1, 1),
            RasterSize::new(1, 1),
        );

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "low.tif".to_string(),
                    window: low_raster_read_window,
                    result: window_data(low_raster_read_window, vec![10.0, 10.0, 10.0, 10.0]),
                },
                FakeRasterReaderData {
                    artifact_path: "high.tif".to_string(),
                    window: high_raster_read_window,
                    result: window_data(high_raster_read_window, vec![0.0]),
                },
            ],
            ..Default::default()
        };

        let service = ElevationService::new(metadata, raster);

        let result = service
            .elevations_in_bbox(
                requested_bbox,
                Some(ResolutionHint::Degrees {
                    lon_resolution: 1.0,
                    lat_resolution: 1.0,
                }),
            )
            .await
            .unwrap();

        assert_eq!(result.width, 2);
        assert_eq!(result.height, 2);
        assert_eq!(
            result.values,
            vec![
                Some(Elevation(10.0)),
                Some(Elevation(10.0)),
                Some(Elevation(10.0)),
                Some(Elevation(10.0)),
            ]
        );
    }

    #[tokio::test]
    async fn elevations_in_bbox_uses_highest_resolution_hint() {
        let requested_bbox = bbox(0.0, 0.0, 4.0, 4.0);

        let low = dataset("low", "low.tif", requested_bbox, 2.0, -2.0, None);
        let high = dataset("high", "high.tif", requested_bbox, 1.0, -1.0, None);

        let metadata = FakeMetadataStorage {
            datasets: vec![low, high],
            should_fail: false,
        };

        let placement = WindowPlacement::new(0, 0);

        let low_raster_read_window =
            RasterReadWindow::new(placement, RasterSize::new(2, 2), RasterSize::new(4, 4));

        let high_raster_read_window =
            RasterReadWindow::new(placement, RasterSize::new(4, 4), RasterSize::new(4, 4));

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "low.tif".to_string(),
                    window: low_raster_read_window,
                    result: window_data(
                        low_raster_read_window,
                        vec![
                            10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0,
                            10.0, 10.0, 10.0, 10.0,
                        ],
                    ),
                },
                FakeRasterReaderData {
                    artifact_path: "high.tif".to_string(),
                    window: high_raster_read_window,
                    result: window_data(
                        high_raster_read_window,
                        vec![
                            20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0,
                            20.0, 20.0, 20.0, 20.0,
                        ],
                    ),
                },
            ],
            ..Default::default()
        };

        let service = ElevationService::new(metadata, raster);

        let result = service
            .elevations_in_bbox(requested_bbox, Some(ResolutionHint::Highest))
            .await
            .unwrap();

        assert_eq!(result.width, 4);
        assert_eq!(result.height, 4);
        assert_eq!(
            result
                .values
                .iter()
                .map(|el| el.unwrap().0)
                .collect::<Vec<f64>>(),
            vec![
                20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0,
                20.0, 20.0,
            ]
        );
    }

    #[tokio::test]
    async fn elevations_in_bbox_uses_lowest_resolution_hint() {
        let requested_bbox = bbox(0.0, 0.0, 4.0, 4.0);

        let low = dataset("low", "low.tif", requested_bbox, 2.0, -2.0, None);
        let high = dataset("high", "high.tif", requested_bbox, 1.0, -1.0, None);

        let metadata = FakeMetadataStorage {
            datasets: vec![low, high],
            should_fail: false,
        };

        let placement = WindowPlacement::new(0, 0);

        let low_raster_read_window =
            RasterReadWindow::new(placement, RasterSize::new(2, 2), RasterSize::new(2, 2));

        let high_raster_read_window =
            RasterReadWindow::new(placement, RasterSize::new(4, 4), RasterSize::new(2, 2));

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "low.tif".to_string(),
                    window: low_raster_read_window,
                    result: window_data(low_raster_read_window, vec![10.0, 10.0, 10.0, 10.0]),
                },
                FakeRasterReaderData {
                    artifact_path: "high.tif".to_string(),
                    window: high_raster_read_window,
                    result: window_data(high_raster_read_window, vec![10.1, 10.2, 20.0, 20.0]),
                },
            ],
            ..Default::default()
        };

        let service = ElevationService::new(metadata, raster);

        let result = service
            .elevations_in_bbox(requested_bbox, Some(ResolutionHint::Lowest))
            .await
            .unwrap();

        assert_eq!(result.width, 2);
        assert_eq!(result.height, 2);
        assert_eq!(
            result
                .values
                .iter()
                .map(|el| el.unwrap().0)
                .collect::<Vec<f64>>(),
            vec![10.1, 10.2, 20.0, 20.0]
        );
    }

    #[tokio::test]
    async fn elevations_in_bbox_gets_data_from_two_datasets_with_different_resolution() {
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

        let placement = WindowPlacement::new(3, 5);
        let source_size = RasterSize::new(1, 3);
        let target_size = RasterSize::new(1, 3);
        let left_raster_read_window = RasterReadWindow::new(placement, source_size, target_size);

        let placement = WindowPlacement::new(0, 2);
        let source_size = RasterSize::new(1, 2);
        let target_size = RasterSize::new(2, 3);
        let right_raster_read_window = RasterReadWindow::new(placement, source_size, target_size);

        let raster = FakeRasterReader {
            responses: vec![
                FakeRasterReaderData {
                    artifact_path: "left.tif".to_string(),
                    window: left_raster_read_window,
                    result: window_data(left_raster_read_window, vec![10.0, 10.0, 10.0]),
                },
                FakeRasterReaderData {
                    artifact_path: "right.tif".to_string(),
                    window: right_raster_read_window,
                    result: window_data(
                        right_raster_read_window,
                        vec![20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
                    ),
                },
            ],
            ..Default::default()
        };
        let service = ElevationService::new(metadata, raster);

        let result = service
            .elevations_in_bbox(requested_bbox, Some(ResolutionHint::Highest))
            .await
            .unwrap();

        assert_eq!(result.width, 3);
        assert_eq!(result.height, 3);
        assert_eq!(
            result
                .values
                .iter()
                .map(|el| { el.unwrap().0 })
                .collect::<Vec<f64>>(),
            vec![10.0, 20.0, 20.0, 10.0, 20.0, 20.0, 10.0, 20.0, 20.0]
        );
    }
}
