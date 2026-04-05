use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::{ElevationService, ElevationServiceError};
use elevation_types::{BboxElevations, Bounds, ResolutionHint};

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    #[error("Elevation service error")]
    Elevation(#[from] ElevationServiceError),
}

pub trait ElevationProvider {
    fn elevations_in_bbox(
        &self,
        bbox: Bounds,
        hint: Option<ResolutionHint>,
    ) -> impl Future<Output = Result<BboxElevations, ElevationProviderError>>;
}

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
