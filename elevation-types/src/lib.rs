use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub trait MetadataStorage {
    fn save_metadata(&self, metadata: &DatasetMetadata);

    fn load_metadata(&self) -> DatasetMetadata;
}

pub trait ArtifactStorage {
    fn save_artifact(&self, bytes: Vec<u8>) -> PathBuf;

    fn load_artifact(&self);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    pub dataset_id: String,
    pub dataset_name: String,
    pub version: String,
    pub artifact_path: PathBuf,
    pub raster: RasterMetadata,
    // pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RasterMetadata {
    pub crs: String,
    pub width: u32,
    pub height: u32,
    pub geo_transform: GeoTransform,
    pub bounds: Bounds,
    pub nodata: Option<f64>,
    pub block_size: Option<BlockSize>,
    pub overview_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoTransform {
    pub origin_x: f64,
    pub origin_y: f64,
    pub pixel_width: f64,
    pub pixel_height: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bounds {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSize {
    pub width: u32,
    pub height: u32,
}
