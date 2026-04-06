//! Abstraction for retrieving elevation data.
//!
//! The main purpose is to keep high level services testable by allowing
//! other providers like fakes or mocks.
use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::{ElevationService, ElevationServiceError};
use elevation_types::{BboxElevations, Bounds, ResolutionHint};

/// Error returned by [`ElevationProvider`].
///
/// This wraps lower level elevation service errors.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    /// Elevation service failed.
    #[error("Elevation service error")]
    Elevation(#[from] ElevationServiceError),
}

/// Provides elevation values for bounding box.
pub trait ElevationProvider {
    /// Returns elevations for given bounding box.
    fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        hint: Option<ResolutionHint>,
    ) -> impl Future<Output = Result<BboxElevations, ElevationProviderError>>;
}

/// Production [`ElevationProvider`] implementation backed by
/// [`ElevationService<FsMetadataStorage, GdalRasterReader>`].
impl ElevationProvider for ElevationService<FsMetadataStorage, GdalRasterReader> {
    async fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        hint: Option<ResolutionHint>,
    ) -> Result<BboxElevations, ElevationProviderError> {
        let elevations = ElevationService::elevations_in_bbox(self, bbox, hint).await?;
        Ok(elevations)
    }
}
