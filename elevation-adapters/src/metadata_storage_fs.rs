use std::path::{Path, PathBuf};

use elevation_types::{DatasetMetadata, MetadataStorage, MetadataStorageError};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

pub struct FsMetadataStorage {
    base_dir: PathBuf,
}

impl FsMetadataStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct FsMetadataRegistry {
    metadata: Vec<DatasetMetadata>,
}

const METADATA_FILE_NAME: &str = "registry.json";

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

        let metadata_path = Path::new(&self.base_dir).join(METADATA_FILE_NAME);

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
        let metadata_path = Path::new(&self.base_dir).join(METADATA_FILE_NAME);

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
