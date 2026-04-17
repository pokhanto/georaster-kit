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

#[cfg(test)]
mod tests {
    use super::*;
    use elevation_domain::{
        ArtifactLocator, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform, MetadataStorage,
        MetadataStorageError, RasterMetadata,
    };
    use tempfile::tempdir;

    fn dataset(dataset_id: &str) -> DatasetMetadata {
        DatasetMetadata {
            dataset_id: dataset_id.to_string(),
            artifact_path: ArtifactLocator::new(format!("{dataset_id}.tif")),
            raster: RasterMetadata {
                bounds: Bounds::try_new(10.0, 20.0, 11.0, 21.0).unwrap(),
                width: 100,
                height: 100,
                nodata: None,
                geo_transform: GeoTransform {
                    origin_lon: 10.0,
                    origin_lat: 21.0,
                    pixel_width: 0.01,
                    pixel_height: -0.01,
                },
                block_size: BlockSize {
                    width: 256,
                    height: 256,
                },
                overview_count: 0,
                crs: Crs::new("EPSG:4326"),
            },
        }
    }

    fn registry_path(base_dir: &Path, registry_name: &str) -> PathBuf {
        base_dir.join(format!("{registry_name}.json"))
    }

    #[tokio::test]
    async fn save_metadata_creates_directory_and_registry_file() {
        let temp = tempdir().unwrap();
        let base_dir = temp.path().join("metadata");
        let storage = FsMetadataStorage::new(base_dir.clone(), "registry".to_string());

        storage.save_metadata(dataset("dataset1")).await.unwrap();

        assert!(base_dir.exists());
        assert!(registry_path(&base_dir, "registry").exists());
    }

    #[tokio::test]
    async fn save_and_load_metadata() {
        let temp = tempdir().unwrap();
        let base_dir = temp.path().to_path_buf();
        let storage = FsMetadataStorage::new(base_dir.clone(), "registry".to_string());

        let first = dataset("dataset1");
        let second = dataset("dataset2");

        storage.save_metadata(first.clone()).await.unwrap();
        storage.save_metadata(second.clone()).await.unwrap();

        let loaded = storage.load_metadata().await.unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].dataset_id, first.dataset_id);
        assert_eq!(loaded[1].dataset_id, second.dataset_id);
    }

    #[tokio::test]
    async fn save_metadata_returns_duplicate_id_for_existing_dataset() {
        let temp = tempdir().unwrap();
        let storage = FsMetadataStorage::new(temp.path().to_path_buf(), "registry".to_string());

        storage.save_metadata(dataset("dataset1")).await.unwrap();

        let result = storage.save_metadata(dataset("dataset1")).await;

        assert_eq!(result.unwrap_err(), MetadataStorageError::DuplicateId);
    }

    #[tokio::test]
    async fn load_metadata_returns_load_when_registry_does_not_exist() {
        let temp = tempdir().unwrap();
        let storage = FsMetadataStorage::new(temp.path().to_path_buf(), "registry".to_string());

        let result = storage.load_metadata().await;

        assert_eq!(result.unwrap_err(), MetadataStorageError::Load);
    }

    #[tokio::test]
    async fn load_metadata_returns_load_for_invalid_json() {
        let temp = tempdir().unwrap();
        let base_dir = temp.path().to_path_buf();
        let path = registry_path(&base_dir, "registry");

        fs::write(&path, b"{ not valid json").await.unwrap();

        let storage = FsMetadataStorage::new(base_dir, "registry".to_string());

        let result = storage.load_metadata().await;

        assert_eq!(result.unwrap_err(), MetadataStorageError::Load);
    }

    #[tokio::test]
    async fn save_metadata_returns_prepare_storage_for_invalid_existing_registry() {
        let temp = tempdir().unwrap();
        let base_dir = temp.path().to_path_buf();
        let path = registry_path(&base_dir, "registry");

        fs::write(&path, b"{ not valid json").await.unwrap();

        let storage = FsMetadataStorage::new(base_dir, "registry".to_string());

        let result = storage.save_metadata(dataset("dataset1")).await;

        assert_eq!(result.unwrap_err(), MetadataStorageError::PrepareStorage);
    }
}
