use geo::{LineString, Polygon, Rect};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

//TODO: reorganize to separate modules
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Elevation(pub f64);

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

    // TODO:: is it required to have
    #[error("Unknown error")]
    Other(String),
}

pub trait MetadataStorage {
    fn save_metadata(
        &self,
        metadata: DatasetMetadata,
    ) -> impl Future<Output = Result<(), MetadataStorageError>> + Send;
    fn load_metadata(
        &self,
    ) -> impl Future<Output = Result<Vec<DatasetMetadata>, MetadataStorageError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactStorageError {
    #[error("Failed to prepare artifact storage location")]
    PrepareStorage,

    #[error("Failed to save artifact")]
    Save,
}

pub trait ArtifactStorage {
    fn save_artifact(
        &self,
        dataset_id: &str,
        source_path: &Path,
    ) -> impl Future<Output = Result<ArtifactLocator, ArtifactStorageError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactLocator(String);

// TODO: add validation
impl ArtifactLocator {
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Placement {
    column: usize,
    row: usize,
}

impl Placement {
    pub fn new(column: usize, row: usize) -> Self {
        Self { column, row }
    }

    pub fn column(&self) -> usize {
        self.column
    }

    pub fn row(&self) -> usize {
        self.row
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Size {
    width: usize,
    height: usize,
}

impl Size {
    pub fn new(width: usize, height: usize) -> Self {
        Self { width, height }
    }

    pub fn point() -> Self {
        Self {
            width: 1,
            height: 1,
        }
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn height(&self) -> usize {
        self.height
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RasterReadWindow {
    pub placement: Placement,
    pub source_size: Size,
    pub target_size: Size,
}

impl RasterReadWindow {
    pub fn new(placement: Placement, source_size: Size, target_size: Size) -> Self {
        Self {
            placement,
            source_size,
            target_size,
        }
    }

    pub fn new_point(placement: Placement) -> Self {
        Self {
            placement,
            source_size: Size::point(),
            target_size: Size::point(),
        }
    }
}

// TODO: maybe these types related only to service
#[derive(Debug, Clone, PartialEq)]
pub struct ResampleTargetSize {
    pub width: usize,
    pub height: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum RasterWindowDataError {
    #[error("values length does not match window dimensions")]
    InvalidValuesLength,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RasterWindowData<T> {
    window: RasterReadWindow,
    values: Vec<T>,
}

impl<T> RasterWindowData<T> {
    pub fn new(
        window: RasterReadWindow,
        values: impl Into<Vec<T>>,
    ) -> Result<Self, RasterWindowDataError> {
        let values = values.into();
        let target_size = window.target_size.width * window.target_size.height;
        if values.len() != target_size {
            return Err(RasterWindowDataError::InvalidValuesLength);
        }

        Ok(Self { window, values })
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn into_values(self) -> Vec<T> {
        self.values
    }

    pub fn get(&self, col: usize, row: usize) -> Option<&T> {
        if col >= self.window.target_size.width || row >= self.window.target_size.height {
            return None;
        }

        self.values.get(row * self.window.target_size.width + col)
    }

    pub fn target_height(&self) -> usize {
        self.window.target_size.height
    }

    pub fn target_width(&self) -> usize {
        self.window.target_size.width
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RasterReaderError {
    #[error("Failed to open raster")]
    Open,
    #[error("Failed to read raster pixel")]
    Read,
}

pub trait RasterReader<T> {
    fn read_window(
        &self,
        locator: &ArtifactLocator,
        raster_window: RasterReadWindow,
    ) -> impl Future<Output = Result<RasterWindowData<T>, RasterReaderError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Crs(String);

impl std::fmt::Display for Crs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// TODO: validation on creation/conversion
impl Crs {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn unknown() -> Self {
        Self::new("Unknown")
    }
}

impl AsRef<str> for Crs {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionHint {
    Highest,
    Lowest,
    Degrees {
        lon_resolution: f64,
        lat_resolution: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    pub dataset_id: String,
    pub artifact_path: ArtifactLocator,
    pub raster: RasterMetadata,
    // TODO: creation date
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Bounds {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl Bounds {
    pub fn intersection(&self, other: &Bounds) -> Option<Bounds> {
        let min_lon = self.min_lon.max(other.min_lon);
        let min_lat = self.min_lat.max(other.min_lat);
        let max_lon = self.max_lon.min(other.max_lon);
        let max_lat = self.max_lat.min(other.max_lat);

        if min_lon <= max_lon && min_lat <= max_lat {
            Some(Bounds {
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            })
        } else {
            None
        }
    }

    pub fn contains_point(&self, lon: f64, lat: f64) -> bool {
        lon >= self.min_lon && lon <= self.max_lon && lat >= self.min_lat && lat <= self.max_lat
    }
}

impl From<Bounds> for Polygon<f64> {
    fn from(value: Bounds) -> Self {
        let exterior = LineString::from(vec![
            (value.min_lon, value.min_lat),
            (value.max_lon, value.min_lat),
            (value.max_lon, value.max_lat),
            (value.min_lon, value.max_lat),
            (value.min_lon, value.min_lat),
        ]);

        Polygon::new(exterior, vec![])
    }
}

impl From<Rect> for Bounds {
    fn from(value: Rect) -> Self {
        Self {
            min_lon: value.min().x,
            min_lat: value.min().y,
            max_lon: value.max().x,
            max_lat: value.max().y,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSize {
    pub width: usize,
    pub height: usize,
}

// TODO: rework
#[derive(Debug, Clone, PartialEq)]
pub struct BboxElevations {
    pub bbox: Bounds,
    pub width: usize,
    pub height: usize,
    pub values: Vec<Option<Elevation>>,
}
