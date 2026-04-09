//! GDAL-backed raster reader.

use elevation_domain::{
    ArtifactLocator, ArtifactResolver, RasterReadWindow, RasterReader, RasterReaderError,
    RasterWindowData,
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
        let path = self.artifact_resolver.resolve(path).unwrap();
        tracing::info!(path = %path, "resolved artifact path");

        tokio::task::spawn_blocking(move || {
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
        })
        .await
        .map_err(|err| {
            tracing::debug!(error = %err, "spawn_blocking task failed");
            RasterReaderError::Read
        })?
    }
}
