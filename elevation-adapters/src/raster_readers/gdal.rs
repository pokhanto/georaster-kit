//! GDAL-backed raster reader.

use elevation_domain::{
    ArtifactLocator, ArtifactResolver, RasterReadWindow, RasterReader, RasterReaderError,
    RasterWindowData, ResolvedArtifactPath,
};
use gdal::Dataset;

const RASTER_BAND_INDEX_WITH_DATA: usize = 1;

/// Reads raster windows from artifacts using GDAL.
#[derive(Debug, Clone)]
pub struct GdalRasterReader<AR> {
    artifact_resolver: AR,
}

impl<AR> GdalRasterReader<AR> {
    pub fn new(artifact_resolver: AR) -> Self {
        Self { artifact_resolver }
    }
}

impl<AR> RasterReader<f64> for GdalRasterReader<AR>
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
        raster_window: RasterReadWindow,
    ) -> Result<RasterWindowData<f64>, RasterReaderError> {
        let path = self.artifact_resolver.resolve(path).map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %path,
                "failed to resolve path for raster"
            );
            RasterReaderError::Path
        })?;

        tracing::info!(path = %path, "resolved artifact path");

        tokio::task::spawn_blocking(move || read_raster_window(path, raster_window))
            .await
            .map_err(|err| {
                tracing::debug!(error = %err, "spawn_blocking task failed");
                RasterReaderError::Read
            })?
    }
}

fn read_raster_window(
    path: ResolvedArtifactPath,
    raster_window: RasterReadWindow,
) -> Result<RasterWindowData<f64>, RasterReaderError> {
    let dataset = Dataset::open(path.as_ref()).map_err(|err| {
        tracing::debug!(
            error = %err,
            path = %path,
            "failed to open raster dataset"
        );
        RasterReaderError::Open
    })?;

    let band = dataset
        .rasterband(RASTER_BAND_INDEX_WITH_DATA)
        .map_err(|err| {
            tracing::debug!(
                error = %err,
                band_index = ?RASTER_BAND_INDEX_WITH_DATA,
                path = %path,
                "failed to read band at requested index"
            );
            RasterReaderError::Read
        })?;

    let placement = raster_window.placement();
    let source_size = raster_window.source_size();
    let target_size = raster_window.target_size();

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
                placement = ?placement,
                source_size = ?source_size,
                target_size = ?target_size,
                band_index = ?RASTER_BAND_INDEX_WITH_DATA,
                path = %path,
                "failed to read window in band"
            );
            RasterReaderError::Read
        })?;

    RasterWindowData::try_new(raster_window, buffer.data()).map_err(|err| {
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
    use elevation_domain::{
        ArtifactLocator, ArtifactResolveError, RasterReadWindow, RasterSize, WindowPlacement,
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
    ) -> RasterReadWindow {
        RasterReadWindow::new(
            WindowPlacement::new(col, row),
            RasterSize::new(source_width, source_height),
            RasterSize::new(target_width, target_height),
        )
    }

    #[tokio::test]
    async fn read_window_returns_path_error_when_resolver_fails() {
        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Err(ArtifactResolveError::UnsupportedLocator("bad".to_string())),
        });

        let result = reader
            .read_window(&ArtifactLocator::new("something"), window(0, 0, 1, 1, 1, 1))
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Path);
    }

    #[tokio::test]
    async fn read_window_returns_open_error_for_missing_file() {
        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new("/thisis/missing/file.tif")),
        });

        let result = reader
            .read_window(&ArtifactLocator::new("something"), window(0, 0, 1, 1, 1, 1))
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Open);
    }

    #[tokio::test]
    async fn read_window_returns_read_error_for_out_of_bounds_window() {
        let temp = tempdir().unwrap();
        let raster_path = temp.path().join("small.tif");

        create_test_geotiff(&raster_path, 2, 2, &[1.0, 2.0, 3.0, 4.0]);

        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new(raster_path.to_string_lossy())),
        });

        let result = reader
            .read_window(
                &ArtifactLocator::new("something"),
                window(10, 10, 1, 1, 1, 1),
            )
            .await;

        assert_eq!(result.unwrap_err(), RasterReaderError::Read);
    }

    #[tokio::test]
    async fn read_window_reads_expected_values() {
        let temp = tempdir().unwrap();
        let raster_path = temp.path().join("small.tif");

        create_test_geotiff(&raster_path, 3, 2, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);

        let reader = GdalRasterReader::new(FakeArtifactResolver {
            result: Ok(ResolvedArtifactPath::new(raster_path.to_string_lossy())),
        });

        let data = reader
            .read_window(&ArtifactLocator::new("something"), window(1, 0, 2, 2, 2, 2))
            .await
            .unwrap();

        assert_eq!(data.target_width(), 2);
        assert_eq!(data.target_height(), 2);
        assert_eq!(data.get(0, 0), Some(&2.0));
        assert_eq!(data.get(1, 0), Some(&3.0));
        assert_eq!(data.get(0, 1), Some(&5.0));
        assert_eq!(data.get(1, 1), Some(&6.0));
    }

    fn create_test_geotiff(path: &Path, width: usize, height: usize, values: &[f64]) {
        let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
        let mut dataset = driver
            .create_with_band_type::<f64, _>(path, width, height, 1)
            .unwrap();

        let mut band = dataset.rasterband(1).unwrap();
        let mut buffer = gdal::raster::Buffer::new((width, height), values.to_vec());
        band.write((0, 0), (width, height), &mut buffer).unwrap();

        dataset.flush_cache().unwrap();
    }
}
