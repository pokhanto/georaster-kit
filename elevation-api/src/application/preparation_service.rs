use std::{
    fs,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use georaster_core::IngestServiceError;

use crate::application::ingest_provider::{IngestProvider, IngestProviderError};

/// Errors returned by [`PreparationService`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PreparationServiceError {
    /// Dataset ingestion failed.
    #[error("Failed to ingest dataset.")]
    Ingest,
    /// Source file metadata could not be read.
    #[error("Can't get file to process.")]
    File,
    /// Dataset identifier could not be derived from file metadata.
    #[error("Can't process dataset Id.")]
    DatasetId,
}

/// Prepares source raster file for serving by delegating ingest to provider.
pub struct PreparationService<IP> {
    ingest_provider: IP,
}

impl<IP> PreparationService<IP> {
    /// Creates new preparation service.
    pub fn new(ingest_provider: IP) -> Self {
        Self { ingest_provider }
    }
}

impl<IP> PreparationService<IP>
where
    IP: IngestProvider,
{
    /// Ingests provided source file.
    ///
    /// Service derives dataset id from file metadata and asks ingest provider
    /// to prepare dataset for serving.
    ///
    /// If dataset with same derived id already exists, ingestion is skipped
    /// and method returns success.
    pub async fn ingest(&self, file_to_ingest: PathBuf) -> Result<(), PreparationServiceError> {
        tracing::info!(?file_to_ingest, "starting ingesting dataset");
        let dataset_id = generate_dataset_id(&file_to_ingest)?;

        let ingest_result = self
            .ingest_provider
            .ingest(&dataset_id, file_to_ingest)
            .await;

        // skip ingest if dataset already ingested
        if let Err(IngestProviderError::IngestProvider(IngestServiceError::DuplicatedId)) =
            ingest_result
        {
            tracing::info!(dataset_id, "dataset id already exists, skipping ingestion");
            return Ok(());
        }

        ingest_result.map_err(|err| {
            tracing::debug!(error = %err, "failed to ingest file");
            PreparationServiceError::Ingest
        })?;

        Ok(())
    }
}

/// Builds dataset identifier from file name, modification timestamp and size.
///
/// Returned identifier is stable while file name, modification time and file
/// size remain unchanged.
fn generate_dataset_id(path: impl AsRef<Path>) -> Result<String, PreparationServiceError> {
    let path = path.as_ref();
    let metadata = fs::metadata(path).map_err(|err| {
        tracing::debug!(error = %err, "failed to get file metadata");
        PreparationServiceError::File
    })?;

    let modified_time = metadata.modified().map_err(|err| {
        tracing::debug!(error = %err, "failed to read file modified time");
        PreparationServiceError::DatasetId
    })?;

    let modified_time = modified_time.duration_since(UNIX_EPOCH).map_err(|err| {
        tracing::debug!(error = %err, "failed to calculate modified time");
        PreparationServiceError::DatasetId
    })?;

    let dataset_id = format!(
        "{}-{}-{}",
        path.file_name()
            .map(|s| s.to_string_lossy())
            .unwrap_or("filename".into()),
        modified_time.as_millis(),
        metadata.len()
    );

    Ok(dataset_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use georaster_core::IngestServiceError;
    use tempfile::tempdir;
    use tokio::fs;

    #[derive(Debug, Clone)]
    struct FakeIngestProvider {
        calls: Arc<Mutex<Vec<(String, PathBuf)>>>,
        result: Result<(), IngestProviderError>,
    }

    impl FakeIngestProvider {
        fn new(result: Result<(), IngestProviderError>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                result,
            }
        }

        fn calls(&self) -> Vec<(String, PathBuf)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl IngestProvider for FakeIngestProvider {
        async fn ingest(
            &self,
            dataset_id: impl Into<String> + Send,
            file_to_ingest: PathBuf,
        ) -> Result<(), IngestProviderError> {
            self.calls
                .lock()
                .unwrap()
                .push((dataset_id.into(), file_to_ingest));

            self.result.clone()
        }
    }

    #[tokio::test]
    async fn ingest_calls_provider_and_returns_ok_on_success() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("dem.tif");
        fs::write(&file, b"abc").await.unwrap();

        let provider = FakeIngestProvider::new(Ok(()));
        let service = PreparationService::new(provider.clone());

        let result = service.ingest(file.clone()).await;

        assert_eq!(result, Ok(()));
        assert_eq!(provider.calls().len(), 1);
        assert_eq!(provider.calls()[0].1, file);
        assert!(provider.calls()[0].0.contains("dem.tif"));
    }

    #[tokio::test]
    async fn ingest_returns_ok_when_dataset_is_already_ingested() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("dem.tif");
        fs::write(&file, b"abc").await.unwrap();

        let provider = FakeIngestProvider::new(Err(IngestProviderError::IngestProvider(
            IngestServiceError::DuplicatedId,
        )));
        let service = PreparationService::new(provider.clone());

        let result = service.ingest(file).await;

        assert_eq!(result, Ok(()));
        assert_eq!(provider.calls().len(), 1);
    }

    #[tokio::test]
    async fn ingest_returns_ingest_error_on_non_duplicate_provider_error() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("dem.tif");
        fs::write(&file, b"abc").await.unwrap();

        let provider = FakeIngestProvider::new(Err(IngestProviderError::IngestProvider(
            IngestServiceError::ArtifactStorage,
        )));
        let service = PreparationService::new(provider);

        let result = service.ingest(file).await;

        assert_eq!(result, Err(PreparationServiceError::Ingest));
    }

    #[tokio::test]
    async fn ingest_returns_file_error_when_cant_process_file_metadata() {
        let provider = FakeIngestProvider::new(Ok(()));
        let service = PreparationService::new(provider);

        let result = service
            .ingest(PathBuf::from("/definitely/missing/file.tif"))
            .await;

        assert_eq!(result, Err(PreparationServiceError::File));
    }

    #[test]
    fn generate_dataset_id_returns_error_for_missing_file() {
        let result = generate_dataset_id("/definitely/missing/file.tif");

        assert_eq!(result, Err(PreparationServiceError::File));
    }

    #[test]
    fn generate_dataset_id_contains_file_name() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("terrain.tif");
        std::fs::write(&path, b"abc").unwrap();

        let dataset_id = generate_dataset_id(&path).unwrap();

        assert!(dataset_id.contains("terrain.tif"));
    }

    #[test]
    fn generate_dataset_id_differs_for_different_file_sizes() {
        let dir = tempdir().unwrap();

        let path1 = dir.path().join("a.tif");
        let path2 = dir.path().join("b.tif");

        std::fs::write(&path1, b"abc").unwrap();
        std::fs::write(&path2, b"abcdef").unwrap();

        let id1 = generate_dataset_id(&path1).unwrap();
        let id2 = generate_dataset_id(&path2).unwrap();

        assert_ne!(id1, id2);
    }
}
