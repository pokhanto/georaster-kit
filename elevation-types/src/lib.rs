use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum MetadataStorageError {}

pub trait MetadataStorage {
    fn save_metadata(&self, metadata: DatasetMetadata) -> Result<(), MetadataStorageError>;
    fn load_metadata(&self) -> Result<Vec<DatasetMetadata>, MetadataStorageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactStorageError {}

pub trait ArtifactStorage {
    fn save_artifact(
        &self,
        dataset_id: &str,
        source_path: &Path,
    ) -> Result<String, ArtifactStorageError>;
}

pub trait RasterReader {
    // TODO: fix path type
    fn read_pixel(&self, path: &str, col: usize, row: usize) -> f64;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Crs(String);

impl std::fmt::Display for Crs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Crs {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn unknown() -> Self {
        Self::new("Unknown")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    pub dataset_id: String,
    pub artifact_path: String,
    pub raster: RasterMetadata,
    // pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RasterMetadata {
    pub crs: Crs,
    pub width: usize,
    pub height: usize,
    pub geo_transform: GeoTransform,
    pub bounds: Bounds,
    pub nodata: Option<f64>,
    pub block_size: BlockSize,
    pub overview_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoTransform {
    pub origin_lon: f64,
    pub origin_lat: f64,
    pub pixel_width: f64,
    pub pixel_height: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bounds {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSize {
    pub width: usize,
    pub height: usize,
}
