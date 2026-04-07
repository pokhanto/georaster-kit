//! Filesystem-backed metadata storage.

use std::path::{Path, PathBuf};

use elevation_domain::{DatasetMetadata, MetadataStorage, MetadataStorageError};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

/// Stores dataset metadata in local JSON registry file.
#[derive(Debug, Clone)]
pub struct FsMetadataStorage {
    base_dir: PathBuf,
    registry_name: String,
}

impl FsMetadataStorage {
    /// Creates filesystem metadata storage rooted at `base_dir`.
    pub fn new(base_dir: PathBuf, registry_name: String) -> Self {
        Self {
            base_dir,
            registry_name,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct FsMetadataRegistry {
    metadata: Vec<DatasetMetadata>,
}

impl MetadataStorage for FsMetadataStorage {
    #[tracing::instrument(skip(self), fields(base_dir = %self.base_dir.display()))]
    async fn save_metadata(
        &self,
        metadata_to_save: DatasetMetadata,
    ) -> Result<(), MetadataStorageError> {
        fs::create_dir_all(&self.base_dir).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                base_dir = %self.base_dir.display(),
                "failed to create metadata storage directory"
            );
            MetadataStorageError::PrepareStorage
        })?;

        let registry_filename = format!("{}.json", self.registry_name);
        let metadata_path = Path::new(&self.base_dir).join(registry_filename);

        let mut metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&metadata_path)
            .await
            .map_err(|err| {
                tracing::debug!(
                    error = %err,
                    path = %metadata_path.display(),
                    "failed to open metadata file"
                );
                MetadataStorageError::PrepareStorage
            })?;

        let mut buf = Vec::new();
        metadata_file.read_to_end(&mut buf).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to read metadata file"
            );
            MetadataStorageError::PrepareStorage
        })?;

        let mut registry: FsMetadataRegistry = if buf.is_empty() {
            FsMetadataRegistry { metadata: vec![] }
        } else {
            serde_json::from_slice(&buf).map_err(|err| {
                tracing::debug!(
                    error = %err,
                    path = %metadata_path.display(),
                    "failed to deserialize metadata registry"
                );
                MetadataStorageError::PrepareStorage
            })?
        };

        tracing::debug!(registry = ?registry, "registry resolved");

        if registry
            .metadata
            .iter()
            .any(|m| m.dataset_id == metadata_to_save.dataset_id)
        {
            return Err(MetadataStorageError::DuplicateId);
        }

        registry.metadata.push(metadata_to_save);

        let serialized = serde_json::to_vec_pretty(&registry).map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to serialize metadata registry"
            );
            MetadataStorageError::Save
        })?;

        let mut metadata_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&metadata_path)
            .await
            .map_err(|err| {
                tracing::debug!(
                    error = %err,
                    path = %metadata_path.display(),
                    "failed to open metadata file for writing"
                );
                MetadataStorageError::PrepareStorage
            })?;

        metadata_file.write_all(&serialized).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to write metadata file"
            );
            MetadataStorageError::Save
        })?;

        metadata_file.flush().await.map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to flush metadata file"
            );
            MetadataStorageError::Save
        })?;

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(base_dir = %self.base_dir.display()))]
    async fn load_metadata(&self) -> Result<Vec<DatasetMetadata>, MetadataStorageError> {
        let registry_filename = format!("{}.json", self.registry_name);
        let metadata_path = Path::new(&self.base_dir).join(registry_filename);

        let mut metadata_file = File::open(&metadata_path).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to open metadata file"
            );
            MetadataStorageError::Load
        })?;

        let mut buf = Vec::new();
        metadata_file.read_to_end(&mut buf).await.map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to read metadata file"
            );
            MetadataStorageError::Load
        })?;

        let registry: FsMetadataRegistry = serde_json::from_slice(&buf).map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %metadata_path.display(),
                "failed to deserialize metadata file"
            );
            MetadataStorageError::Load
        })?;

        Ok(registry.metadata)
    }
}
