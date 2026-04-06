use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::{ElevationService, ElevationServiceError};
use elevation_types::Elevation;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ElevationProviderError {
    #[error("Elevation service error")]
    Elevation(#[from] ElevationServiceError),
}

pub trait ElevationProvider {
    fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> impl Future<Output = Result<Option<Elevation>, ElevationProviderError>> + Send;
}

impl ElevationProvider for ElevationService<FsMetadataStorage, GdalRasterReader> {
    async fn elevation_at_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> Result<Option<Elevation>, ElevationProviderError> {
        let elevations = ElevationService::elevation_at_point(self, lon, lat).await?;

        Ok(elevations)
    }
}
