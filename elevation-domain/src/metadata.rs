//! Dataset and raster metadata models.

use serde::{Deserialize, Serialize};

use crate::spatial::{Bounds, Crs};
use crate::storage::ArtifactLocator;

/// Stored metadata for dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetMetadata {
    /// Dataset identifier.
    pub dataset_id: String,
    /// Locator of the stored raster artifact.
    pub artifact_path: ArtifactLocator,
    /// Raster-specific metadata.
    pub raster: RasterMetadata,
}

/// Raster metadata required for reading and serving elevations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RasterMetadata {
    /// Raster coordinate reference system.
    pub crs: Crs,
    /// Raster width in pixels.
    pub width: usize,
    /// Raster height in pixels.
    pub height: usize,
    /// Transform describing raster placement.
    pub geo_transform: GeoTransform,
    /// Bounding box of raster.
    pub bounds: Bounds,
    /// NoData value, if present.
    pub nodata: Option<f64>,
    /// Native block size of raster.
    pub block_size: BlockSize,
    /// Number of overviews available of raster.
    pub overview_count: u32,
}

/// Geotransform values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoTransform {
    /// Longitude of the top-left origin.
    pub origin_lon: f64,
    /// Latitude of the top-left origin.
    pub origin_lat: f64,
    /// Pixel width in coordinate units.
    pub pixel_width: f64,
    /// Pixel height in coordinate units.
    pub pixel_height: f64,
}

/// Native raster block size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockSize {
    /// Block width in pixels.
    pub width: usize,
    /// Block height in pixels.
    pub height: usize,
}
