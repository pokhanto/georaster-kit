//! Filesystem-backed artifact storage.

use elevation_domain::{ArtifactLocator, ArtifactStorage, ArtifactStorageError};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, OpenOptions},
    io,
};

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
        source_path: impl AsRef<Path> + Send,
    ) -> Result<ArtifactLocator, ArtifactStorageError> {
        let source_path = source_path.as_ref();
        tracing::debug!(base_dir = %self.base_dir.display(), "preparing artifact storage directory");

        fs::create_dir_all(&self.base_dir).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                base_dir = %self.base_dir.display(),
                "failed to create artifact storage directory"
            );

            ArtifactStorageError::PrepareStorage
        })?;

        let storage_path = self.base_dir.join(format!("{dataset_id}.tif"));
        tracing::debug!(storage_path = %storage_path.display(), "artifact storage path composed");

        let mut source_file = fs::File::open(source_path).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                source_path = %source_path.display(),
                "failed to open source artifact"
            );

            ArtifactStorageError::Save
        })?;

        let mut destination_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&storage_path)
            .await
            .map_err(|err| {
                tracing::debug!(
                    error = %err,
                    storage_path = %storage_path.display(),
                    "failed to create destination artifact"
                );

                if err.kind() == io::ErrorKind::AlreadyExists {
                    ArtifactStorageError::DuplicateId
                } else {
                    ArtifactStorageError::Save
                }
            })?;

        io::copy(&mut source_file, &mut destination_file)
            .await
            .map_err(|err| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn save_artifact_copies_source_file_and_returns_locator() {
        let temp = tempdir().unwrap();
        let source_path = temp.path().join("source.tif");
        let base_dir = temp.path().join("artifacts");

        fs::write(&source_path, b"test-geotiff-data").await.unwrap();

        let storage = FsArtifactStorage::new(base_dir.clone());

        let locator = storage
            .save_artifact("dataset-1", &source_path)
            .await
            .unwrap();

        let expected_path = base_dir.join("dataset-1.tif");
        let saved_content = fs::read(&expected_path).await.unwrap();

        assert_eq!(PathBuf::from(String::from(locator)), expected_path);
        assert_eq!(saved_content, b"test-geotiff-data");
    }

    #[tokio::test]
    async fn save_artifact_creates_base_directory_if_missing() {
        let temp = tempdir().unwrap();
        let source_path = temp.path().join("source.tif");
        let base_dir = temp.path().join("nested").join("artifacts");

        fs::write(&source_path, b"abc").await.unwrap();

        let storage = FsArtifactStorage::new(base_dir.clone());

        storage
            .save_artifact("dataset-1", &source_path)
            .await
            .unwrap();

        assert!(base_dir.exists());
        assert!(base_dir.join("dataset-1.tif").exists());
    }

    #[tokio::test]
    async fn save_artifact_returns_duplicate_id_when_target_already_exists() {
        let temp = tempdir().unwrap();
        let source_path = temp.path().join("source.tif");
        let base_dir = temp.path().join("artifacts");

        fs::write(&source_path, b"first").await.unwrap();

        let storage = FsArtifactStorage::new(base_dir.clone());

        storage
            .save_artifact("dataset-1", &source_path)
            .await
            .unwrap();

        let result = storage.save_artifact("dataset-1", &source_path).await;

        assert_eq!(result.unwrap_err(), ArtifactStorageError::DuplicateId);
    }

    #[tokio::test]
    async fn save_artifact_returns_save_when_source_file_does_not_exist() {
        let temp = tempdir().unwrap();
        let source_path = temp.path().join("missing.tif");
        let base_dir = temp.path().join("artifacts");

        let storage = FsArtifactStorage::new(base_dir);

        let result = storage.save_artifact("dataset-1", &source_path).await;

        assert_eq!(result.unwrap_err(), ArtifactStorageError::Save);
    }
}
