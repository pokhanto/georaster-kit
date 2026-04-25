//! Thin abstraction over low-level elevation service.

use georaster_adapters::{FsArtifactResolver, FsMetadataStorage, GdalRasterReader};
use georaster_core::{GeorasterService, GeorasterServiceError};
use georaster_domain::{BandSelection, RasterPoint, RasterRepresentation};

/// Error returned by [`ElevationProvider`].
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    #[error("Elevation service error")]
    Elevation(#[from] GeorasterServiceError),
}

/// Abstraction over [`GeorasterService`] used to reduce coupling and improve testability.
pub trait ElevationProvider {
    /// Returns elevation at given geographic point.
    ///
    /// For detailed behavior see
    /// [`georaster_core::GeorasterService::raster_data_at_point`].
    fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> impl Future<Output = Result<Option<RasterPoint>, ElevationProviderError>> + Send;
}

/// Real implementation [`ElevationProvider`] backed by [`GeorasterService`].
impl ElevationProvider
    for GeorasterService<FsMetadataStorage, GdalRasterReader<FsArtifactResolver>>
{
    async fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> Result<Option<RasterPoint>, ElevationProviderError> {
        // TODO: since this is "thin abstraction", it is not clear
        // why decision about BandSelection and RasterRepresentation made here
        let elevations = GeorasterService::raster_data_at_point(
            self,
            lon,
            lat,
            BandSelection::First,
            RasterRepresentation::Grayscale,
        )
        .await?;

        Ok(elevations)
    }
}
