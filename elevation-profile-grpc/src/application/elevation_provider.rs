//! Thin abstraction over low-level elevation service.

use georaster_adapters::{FsMetadataStorage, GdalRasterReader, GdalS3ArtifactResolver};
use georaster_core::{GeorasterService, GeorasterServiceError};
use georaster_domain::{RasterPoint, RasterRepresentation};

/// Error returned by [`ElevationProvider`].
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    #[error("Elevation service error")]
    Elevation(#[from] GeorasterServiceError),
}

/// Abstraction over [`ElevationService`] used to reduce coupling and improve testability.
pub trait ElevationProvider {
    /// Returns elevation at given geographic point.
    ///
    /// For detailed behavior see
    /// [`georaster_core::ElevationService::elevation_at_point`].
    fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> impl Future<Output = Result<Option<RasterPoint>, ElevationProviderError>> + Send;
}

/// Real implementation [`ElevationProvider`] backed by [`ElevationService`].
impl ElevationProvider
    for GeorasterService<FsMetadataStorage, GdalRasterReader<GdalS3ArtifactResolver>>
{
    async fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> Result<Option<RasterPoint>, ElevationProviderError> {
        let elevations = GeorasterService::raster_data_at_point(
            self,
            lon,
            lat,
            georaster_domain::BandSelection::First,
            RasterRepresentation::Grayscale,
        )
        .await?;

        Ok(elevations)
    }
}
