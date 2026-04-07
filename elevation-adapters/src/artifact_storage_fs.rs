//! Filesystem-backed artifact storage.

use elevation_domain::{ArtifactLocator, ArtifactStorage, ArtifactStorageError};
use std::path::{Path, PathBuf};
use tokio::fs;

/// Stores artifacts in local filesystem.
pub struct FsArtifactStorage {
    base_dir: PathBuf,
}

impl FsArtifactStorage {
    /// Creates filesystem artifact storage rooted at `base_dir`.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
}

impl ArtifactStorage for FsArtifactStorage {
    #[tracing::instrument(skip(self), fields(dataset_id, source_path))]
    async fn save_artifact(
        &self,
        dataset_id: &str,
        source_path: &Path,
    ) -> Result<ArtifactLocator, ArtifactStorageError> {
        tracing::debug!(base_dir = %self.base_dir.display(), "preparing artifact storage directory");

        fs::create_dir_all(&self.base_dir).await.map_err(|err| {
            tracing::debug!(error = %err, base_dir = %self.base_dir.display(), "failed to create artifact storage directory");

            ArtifactStorageError::PrepareStorage
        })?;

        let storage_path = self.base_dir.join(format!("{dataset_id}.tif"));
        tracing::debug!(storage_path = %storage_path.display(), "artifact storage path composed");

        fs::copy(source_path, &storage_path).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                source_path = %source_path.display(),
                storage_path = %storage_path.display(),
                "failed to copy artifact into storage"
            );

            ArtifactStorageError::Save
        })?;

        Ok(ArtifactLocator::from(storage_path))
    }
}
