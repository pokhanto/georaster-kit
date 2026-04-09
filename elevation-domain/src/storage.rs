//! Storage-related traits and types.
//!
//! This module defines abstractions for persisting dataset metadata and raster
//! artifacts.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::metadata::DatasetMetadata;

/// Errors returned by metadata storage implementations.
#[derive(Debug, thiserror::Error)]
pub enum MetadataStorageError {
    #[error("Failed to prepare metadata storage")]
    PrepareStorage,

    #[error("Failed to save metadata")]
    Save,

    #[error("Failed to load metadata")]
    Load,

    #[error("Metadata with Id already exists")]
    DuplicateId,
}

/// Persists dataset metadata.
///
/// Implementations are responsible for storing and loading metadata records
/// describing ingested datasets. This trait is used by ingest pipeline to
/// save metadata after artifacts are prepared, and by runtime services to load
/// available dataset information.
pub trait MetadataStorage {
    /// Saves metadata for dataset.
    ///
    /// Implementations should persist provided record so it can be loaded
    /// later by runtime services or other application components.
    ///
    /// Returns an error if metadata cannot be saved.
    fn save_metadata(
        &self,
        metadata: DatasetMetadata,
    ) -> impl Future<Output = Result<(), MetadataStorageError>>;

    /// Loads all known dataset metadata records.
    ///
    /// Usedrequest handling to discover which datasets are available for querying.
    ///
    /// Returns all stored metadata records or error if loading fails.
    fn load_metadata(
        &self,
    ) -> impl Future<Output = Result<Vec<DatasetMetadata>, MetadataStorageError>> + Send;
}

/// Errors returned by artifact storage implementations.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ArtifactStorageError {
    #[error("Failed to prepare artifact storage location")]
    PrepareStorage,

    #[error("Artifact with Id already exists")]
    DuplicateId,

    #[error("Failed to save artifact")]
    Save,
}

/// Persists raster artifacts produced during ingest.
///
/// Implementations are responsible for storing raster files, such as prepared
/// GeoTIFF or COG artifacts, and returning locator that can later be used by
/// raster readers to access stored artifact.
pub trait ArtifactStorage {
    /// Saves source artifact for given dataset id.
    ///
    /// Implementation should store file located at `source_path` and
    /// return [`ArtifactLocator`] identifying where artifact can be read later.
    ///
    /// Returns error if artifact cannot be stored.
    fn save_artifact(
        &self,
        dataset_id: &str,
        source_path: &Path,
    ) -> impl Future<Output = Result<ArtifactLocator, ArtifactStorageError>> + Send;
}

/// Identifies stored artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactLocator(String);

impl ArtifactLocator {
    /// Creates new artifact locator.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl From<ArtifactLocator> for String {
    fn from(value: ArtifactLocator) -> Self {
        value.0
    }
}

impl From<String> for ArtifactLocator {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ArtifactLocator {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<PathBuf> for ArtifactLocator {
    fn from(value: PathBuf) -> Self {
        Self::new(value.to_string_lossy())
    }
}

impl From<&Path> for ArtifactLocator {
    fn from(value: &Path) -> Self {
        Self::new(value.to_string_lossy())
    }
}

impl AsRef<str> for ArtifactLocator {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ArtifactLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ArtifactResolveError {
    #[error("Unsupported artifact locator: {0}")]
    UnsupportedLocator(String),
}

pub trait ArtifactResolver {
    fn resolve(
        &self,
        locator: &ArtifactLocator,
    ) -> Result<ResolvedArtifactPath, ArtifactResolveError>;
}

/// Identifies resolved path to stored artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedArtifactPath(String);

impl ResolvedArtifactPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl AsRef<str> for ResolvedArtifactPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ResolvedArtifactPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
