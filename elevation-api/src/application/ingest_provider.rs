//! Thin abstraction over low-level ingest service.

use std::path::PathBuf;

use georaster_adapters::{FsArtifactStorage, FsMetadataStorage};
use georaster_core::{IngestService, IngestServiceError};

/// Error returned by [`IngestProvider`].
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum IngestProviderError {
    #[error("Elevation service error")]
    IngestProvider(#[from] IngestServiceError),
}

/// Abstraction over [`IngestService`] used to reduce coupling and improve testability.
pub trait IngestProvider {
    /// Runs ingest for specified source file with dataset_id
    ///
    /// For detailed behavior see
    /// [`georaster_core::IngestService::run`].
    fn ingest(
        &self,
        dataset_id: impl Into<String> + Send,
        source_path: PathBuf,
    ) -> impl Future<Output = Result<(), IngestProviderError>> + Send;
}

/// Real implementation [`IngestProvider`] backed by [`IngestService`].
impl IngestProvider for IngestService<FsArtifactStorage, FsMetadataStorage> {
    async fn ingest(
        &self,
        dataset_id: impl Into<String> + Send,
        source_path: PathBuf,
    ) -> Result<(), IngestProviderError> {
        self.run(dataset_id, source_path).await?;

        Ok(())
    }
}
