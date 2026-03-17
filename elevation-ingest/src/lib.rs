use elevation_types::{
    ArtifactStorage, BlockSize, Bounds, DatasetMetadata, GeoTransform, MetadataStorage,
    RasterMetadata,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;
use thiserror::Error;

pub mod artifact_storage_fs;
pub mod metadata_storage_fs;

pub use artifact_storage_fs::FsArtifactStorage;
pub use metadata_storage_fs::FsMetadataStorage;

#[derive(Debug, Error)]
pub enum MetadataReadError {
    #[error("failed to execute gdalinfo: {0}")]
    Spawn(#[source] std::io::Error),

    #[error("gdalinfo failed: {0}")]
    GdalInfoFailed(String),

    #[error("failed to parse gdalinfo output: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("missing first raster band")]
    MissingBand,
}

#[derive(Debug, Deserialize)]
struct GdalInfo {
    size: [u32; 2],
    #[serde(rename = "coordinateSystem")]
    coordinate_system: Option<CoordinateSystem>,
    #[serde(rename = "geoTransform")]
    geo_transform: [f64; 6],
    #[serde(rename = "cornerCoordinates")]
    corner_coordinates: CornerCoordinates,
    bands: Vec<Band>,
}

#[derive(Debug, Deserialize)]
struct CoordinateSystem {
    wkt: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CornerCoordinates {
    #[serde(rename = "lowerLeft")]
    lower_left: [f64; 2],
    #[serde(rename = "upperRight")]
    upper_right: [f64; 2],
}

#[derive(Debug, Deserialize)]
struct Band {
    block: Option<[u32; 2]>,
    #[serde(rename = "noDataValue")]
    no_data_value: Option<serde_json::Value>,
    overviews: Option<Vec<serde_json::Value>>,
}

pub fn read_raster_metadata(path: &Path) -> Result<RasterMetadata, MetadataReadError> {
    let output = Command::new("gdalinfo")
        .arg("-json")
        .arg(path)
        .output()
        .map_err(MetadataReadError::Spawn)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        return Err(MetadataReadError::GdalInfoFailed(stderr));
    }

    let info: GdalInfo = serde_json::from_slice(&output.stdout)?;
    let band = info.bands.first().ok_or(MetadataReadError::MissingBand)?;

    let block_size = band
        .block
        .map(|[width, height]| BlockSize { width, height });

    let nodata = parse_nodata(band.no_data_value.as_ref());

    let overview_count = band
        .overviews
        .as_ref()
        .map(|items| items.len() as u32)
        .unwrap_or(0);

    Ok(RasterMetadata {
        crs: info
            .coordinate_system
            .and_then(|cs| cs.wkt)
            .unwrap_or_else(|| "unknown".to_string()),
        width: info.size[0],
        height: info.size[1],
        geo_transform: GeoTransform {
            origin_x: info.geo_transform[0],
            origin_y: info.geo_transform[3],
            pixel_width: info.geo_transform[1],
            pixel_height: info.geo_transform[5],
        },
        bounds: Bounds {
            min_x: info.corner_coordinates.lower_left[0],
            min_y: info.corner_coordinates.lower_left[1],
            max_x: info.corner_coordinates.upper_right[0],
            max_y: info.corner_coordinates.upper_right[1],
        },
        nodata,
        block_size,
        overview_count,
    })
}

fn parse_nodata(value: Option<&serde_json::Value>) -> Option<f64> {
    match value {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) if s.eq_ignore_ascii_case("nan") => Some(f64::NAN),
        Some(_) => None,
        None => None,
    }
}

pub fn ingest(
    source_path: PathBuf,
    artifact_storage: impl ArtifactStorage,
    metadata_storage: impl MetadataStorage,
) {
    let bytes = std::fs::read(&source_path).unwrap();
    let artifact_path = artifact_storage.save_artifact(bytes);
    let raster_metadata = read_raster_metadata(&source_path)
        .map_err(|e| e.to_string())
        .unwrap();
    let metadata = DatasetMetadata {
        dataset_id: String::from("1"),
        dataset_name: String::from("1"),
        version: String::from("1"),
        artifact_path,
        raster: raster_metadata,
    };

    metadata_storage.save_metadata(&metadata);
}
