//! GDAL-backed raster reader.

use gdal::Dataset;
use georaster_domain::{
    ArtifactLocator, ArtifactResolver, RasterBand, RasterGrid, RasterReadQuery, RasterReader,
    RasterReaderError, ResolvedArtifactPath,
};

/// Reads raster windows from raster artifacts using GDAL.
///
/// This reader:
/// - resolves artifact locator into concrete path,
/// - opens raster dataset with GDAL,
/// - reads requested source window for each requested band,
/// - resamples it into requested target size when needed,
/// - returns resulting values as [`RasterGrid`].
///
/// Returned band data is stored per band, with each band containing values
/// in row-major order.
#[derive(Debug, Clone)]
pub struct GdalRasterReader<AR> {
    artifact_resolver: AR,
}

impl<AR> GdalRasterReader<AR> {
    /// Creates new GDAL-backed raster reader.
    pub fn new(artifact_resolver: AR) -> Self {
        Self { artifact_resolver }
    }
}

impl<AR> RasterReader for GdalRasterReader<AR>
where
    AR: ArtifactResolver + Send + Sync,
{
    #[tracing::instrument(
        skip(self),
        fields(
            artifact_path = %path,
        )
    )]
    async fn read_window(
        &self,
        path: &ArtifactLocator,
        raster_query: RasterReadQuery,
    ) -> Result<RasterGrid, RasterReaderError> {
        let path = self.artifact_resolver.resolve(path).map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %path,
                "failed to resolve path for raster"
            );
            RasterReaderError::Path
        })?;

        tracing::info!(path = %path, "resolved artifact path");

        tokio::task::spawn_blocking(move || read_raster_window(path, raster_query))
            .await
            .map_err(|err| {
                tracing::debug!(error = %err, "spawn_blocking task failed");
                RasterReaderError::Read
            })?
    }
}

fn read_raster_window(
    path: ResolvedArtifactPath,
    raster_query: RasterReadQuery,
) -> Result<RasterGrid, RasterReaderError> {
    let dataset = Dataset::open(path.as_ref()).map_err(|err| {
        tracing::debug!(
            error = %err,
            path = %path,
            "failed to open raster dataset"
        );
        RasterReaderError::Open
    })?;

    let placement = raster_query.placement();
    let source_size = raster_query.source_size();
    let target_size = raster_query.target_size();

    let bands = raster_query
        .bands()
        .iter()
        .map(|band_index| {
            let band = dataset.rasterband(*band_index).map_err(|err| {
                tracing::debug!(
                    error = %err,
                    band_index,
                    path = %path,
                    "failed to read band at requested index"
                );
                RasterReaderError::Read
            })?;

            let buffer = band
                .read_as::<f64>(
                    (placement.column() as isize, placement.row() as isize),
                    (source_size.width(), source_size.height()),
                    (target_size.width(), target_size.height()),
                    None,
                )
                .map_err(|err| {
                    tracing::debug!(
                        error = %err,
                        band_index,
                        placement = ?placement,
                        source_size = ?source_size,
                        target_size = ?target_size,
                        path = %path,
                        "failed to read window in band"
                    );
                    RasterReaderError::Read
                })?;

            Ok(RasterBand::new(*band_index, buffer.data()))
        })
        .collect::<Result<Vec<_>, RasterReaderError>>()?;

    RasterGrid::try_new(target_size.width(), target_size.height(), bands).map_err(|err| {
        tracing::debug!(
            error = %err,
            placement = ?placement,
            source_size = ?source_size,
            target_size = ?target_size,
            path = %path,
            "failed to construct resulting data for requested window"
        );
        RasterReaderError::Read
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use georaster_domain::{
        ArtifactLocator, ArtifactResolveError, RasterReadQuery, RasterSize, WindowPlacement,
    };
    use tempfile::tempdir;

    #[derive(Clone)]
    struct FakeArtifactResolver {
        result: Result<ResolvedArtifactPath, ArtifactResolveError>,
    }

    impl ArtifactResolver for FakeArtifactResolver {
        fn resolve(
            &self,
            _locator: &ArtifactLocator,
        ) -> Result<ResolvedArtifactPath, ArtifactResolveError> {
            self.result.clone()
        }
    }

    fn window(
        col: usize,
        row: usize,
        source_width: usize,
        source_height: usize,
        target_width: usize,
        target_height: usize,
        bands: Vec<usize>,
    ) -> RasterReadQuery {
        RasterReadQuery::new(
            WindowPlacement::new(col, row),
            RasterSize::new(source_width, source_height),
            RasterSize::new(target_width, target_height),
            bands,
        )
    }

    #[tokio::test]
    async fn read_window_returns_path_error_when_resolver_fails() {
        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Err(ArtifactResolveError::UnsupportedLocator("bad".to_string())),
        });

        let result = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(0, 0, 1, 1, 1, 1, vec![1]),
            )
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Path);
    }

    #[tokio::test]
    async fn read_window_returns_open_error_for_missing_file() {
        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new("/thisis/missing/file.tif")),
        });

        let result = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(0, 0, 1, 1, 1, 1, vec![1]),
            )
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Open);
    }

    #[tokio::test]
    async fn read_window_returns_read_error_for_out_of_bounds_window() {
        let temp = tempdir().unwrap();
        let raster_path = temp.path().join("small.tif");

        create_test_geotiff(&raster_path, 2, 2, &[vec![1.0, 2.0, 3.0, 4.0]]);

        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new(raster_path.to_string_lossy())),
        });

        let result = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(10, 10, 1, 1, 1, 1, vec![1]),
            )
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Read);
    }

    #[tokio::test]
    async fn read_window_reads_expected_values_for_single_band() {
        let temp = tempdir().unwrap();
        let raster_path = temp.path().join("small.tif");

        create_test_geotiff(&raster_path, 3, 2, &[vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]]);

        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new(raster_path.to_string_lossy())),
        });

        let grid = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(1, 0, 2, 2, 2, 2, vec![1]),
            )
            .await
            .unwrap();

        assert_eq!(grid.width(), 2);
        assert_eq!(grid.height(), 2);
        assert_eq!(grid.bands().len(), 1);

        let band = grid.band(1).unwrap();
        assert_eq!(band.data(), &[2.0, 3.0, 5.0, 6.0]);
    }

    #[tokio::test]
    async fn read_window_reads_expected_values_for_multiple_bands() {
        let temp = tempdir().unwrap();
        let raster_path = temp.path().join("multiband.tif");

        create_test_geotiff(
            &raster_path,
            2,
            2,
            &[vec![1.0, 2.0, 3.0, 4.0], vec![10.0, 20.0, 30.0, 40.0]],
        );

        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new(raster_path.to_string_lossy())),
        });

        let grid = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(0, 0, 2, 2, 2, 2, vec![1, 2]),
            )
            .await
            .unwrap();

        assert_eq!(grid.width(), 2);
        assert_eq!(grid.height(), 2);
        assert_eq!(grid.bands().len(), 2);

        let band1 = grid.band(1).unwrap();
        assert_eq!(band1.data(), &[1.0, 2.0, 3.0, 4.0]);

        let band2 = grid.band(2).unwrap();
        assert_eq!(band2.data(), &[10.0, 20.0, 30.0, 40.0]);
    }

    fn create_test_geotiff(path: &Path, width: usize, height: usize, band_values: &[Vec<f64>]) {
        let band_count = band_values.len();
        let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
        let mut dataset = driver
            .create_with_band_type::<f64, _>(path, width, height, band_count)
            .unwrap();

        for (idx, values) in band_values.iter().enumerate() {
            let mut band = dataset.rasterband(idx + 1).unwrap();
            let mut buffer = gdal::raster::Buffer::new((width, height), values.clone());
            band.write((0, 0), (width, height), &mut buffer).unwrap();
        }

        dataset.flush_cache().unwrap();
    }
}
