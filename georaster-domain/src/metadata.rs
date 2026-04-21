//! Dataset and raster metadata models.

use serde::{Deserialize, Serialize};

use crate::spatial::{Bounds, Crs};
use crate::storage::ArtifactLocator;
use crate::{BandSelection, RasterRepresentation};

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

/// Metadata describing a single raster band.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RasterBandMetadata {
    /// One-based band index in the source raster dataset.
    pub band_index: usize,

    /// Optional `nodata` value for this band.
    pub nodata: Option<f64>,

    /// Internal block size of this band.
    pub block_size: BlockSize,

    /// Color interpretation for this band.
    pub color_interpretation: String,
}

/// Metadata describing a raster dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RasterMetadata {
    /// Coordinate reference system of the raster dataset.
    pub crs: Crs,

    /// Raster width in cells.
    pub width: usize,

    /// Raster height in cells.
    pub height: usize,

    /// Affine transform describing how raster coordinates map to spatial
    /// coordinates.
    pub geo_transform: GeoTransform,

    /// Spatial bounds covered by the raster dataset.
    pub bounds: Bounds,

    /// Number of overviews available for the dataset.
    pub overview_count: usize,

    /// Raster representation.
    pub raster_representation: RasterRepresentation,

    /// Per-band metadata entries.
    pub bands: Vec<RasterBandMetadata>,
}

/// Geotransform values.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BlockSize {
    /// Block width in pixels.
    pub width: usize,
    /// Block height in pixels.
    pub height: usize,
}

impl RasterMetadata {
    // TODO: should it be fallible?
    pub fn resolve_band_indexes(&self, selection: &BandSelection) -> Vec<usize> {
        match selection {
            BandSelection::First => self
                .bands
                .first()
                .map(|band| vec![band.band_index])
                .unwrap_or_default(),

            BandSelection::Indexes(indexes) => {
                let mut resolved: Vec<usize> = indexes
                    .iter()
                    .copied()
                    .filter(|idx| self.bands.iter().any(|band| band.band_index == *idx))
                    .collect();

                resolved.sort_unstable();
                resolved.dedup();
                resolved
            }

            BandSelection::All => self.bands.iter().map(|band| band.band_index).collect(),
        }
    }
}
