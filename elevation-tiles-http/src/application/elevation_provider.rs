//! Abstraction for retrieving elevation data.
//!
//! Main purpose is to keep high level services testable by allowing
//! other providers like fakes or mocks.
use georaster_adapters::{FsArtifactResolver, FsMetadataStorage, GdalRasterReader};
use georaster_core::{GeorasterSampling, GeorasterService, GeorasterServiceError};
use georaster_domain::{Bounds, RasterGrid, RasterRepresentation};

/// Error returned by [`ElevationProvider`].
///
/// This wraps lower level elevation service errors.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    /// Elevation service failed.
    #[error("Elevation service error")]
    Elevation(#[from] GeorasterServiceError),
}

/// Provides elevation values for bounding box.
pub trait ElevationProvider {
    /// Returns elevations for given bounding box.
    ///
    /// For detailed behavior see
    /// [`georaster_core::ElevationService::elevations_in_bbox`].
    fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        sampling: Option<GeorasterSampling>,
    ) -> impl Future<Output = Result<RasterGrid, ElevationProviderError>> + Send;
}

/// Production [`ElevationProvider`] implementation backed by
/// [`ElevationService<FsMetadataStorage, GdalRasterReader>`].
impl ElevationProvider
    for GeorasterService<FsMetadataStorage, GdalRasterReader<FsArtifactResolver>>
{
    async fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        sampling: Option<GeorasterSampling>,
    ) -> Result<RasterGrid, ElevationProviderError> {
        let elevations = GeorasterService::raster_data_in_bbox(
            self,
            bbox,
            sampling,
            georaster_domain::BandSelection::First,
            RasterRepresentation::Grayscale,
        )
        .await?;
        Ok(elevations)
    }
}
