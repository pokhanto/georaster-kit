use elevation_types::{
    ArtifactStorage, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform, MetadataStorage,
    RasterMetadata,
};
use gdal::{Dataset, Metadata};
use std::path::{Path, PathBuf};

use crate::gdal_process;

// TODO: should be in settings, now only ingest knows
const BASE_CRS: &str = "EPSG:4326";

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("Failed to reproject source raster.")]
    Reprojection,

    #[error("Failed to convert raster to cloud-optimized geotiff.")]
    CogConversion,

    #[error("Failed to extract raster metadata.")]
    MetadataExtraction,

    #[error("Failed to save artifact.")]
    ArtifactStorage,

    #[error("Failed to save metadata.")]
    MetadataStorage,
}

#[tracing::instrument(skip_all, fields(path = %path.display()))]
pub fn read_raster_metadata(path: &Path) -> Result<RasterMetadata, IngestError> {
    tracing::info!("starting metadata extraction");
    let dataset = Dataset::open(path).map_err(|err| {
        tracing::error!(error = %err, path = %path.display(), "failed to open raster dataset");

        IngestError::MetadataExtraction
    })?;

    let (width, height) = dataset.raster_size();
    tracing::debug!(width = %width, height = %height, "raster size extracted");

    let geo_transform = dataset.geo_transform().map_err(|err| {
        tracing::error!(error = %err, "failed to get geotransform");

        IngestError::MetadataExtraction
    })?;
    tracing::debug!(geo_transform = ?geo_transform, "geotransform extracted");
    let min_lon = geo_transform[0];
    let max_lat = geo_transform[3];
    let max_lon = geo_transform[0] + width as f64 * geo_transform[1];
    let min_lat = geo_transform[3] + height as f64 * geo_transform[5];

    let crs = get_crs(&dataset)?;
    tracing::debug!(crs = ?crs, "crs extracted");

    let band_index = 1;
    let band = dataset.rasterband(band_index).map_err(|err| {
        tracing::error!(error = %err, "failed to get bands from raster");

        IngestError::MetadataExtraction
    })?;
    let (block_width, block_height) = band.block_size();
    tracing::debug!(block_width = %block_width, block_height = %block_height, "block size extracted");
    let nodata = band.no_data_value();
    tracing::debug!(nodata = ?nodata, "nodata value extracted");

    tracing::info!("metadata extraction completed");
    Ok(RasterMetadata {
        crs,
        width,
        height,
        geo_transform: GeoTransform {
            origin_lon: geo_transform[0],
            origin_lat: geo_transform[3],
            pixel_width: geo_transform[1],
            pixel_height: geo_transform[5],
        },
        bounds: Bounds {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        },
        nodata,
        block_size: BlockSize {
            width: block_width,
            height: block_height,
        },
        overview_count: 0,
    })
}

fn is_cog(dataset: &Dataset) -> bool {
    dataset
        .metadata_domain("IMAGE_STRUCTURE")
        .unwrap_or_default()
        .iter()
        .any(|item| item == "LAYOUT=COG")
}

fn get_crs(dataset: &Dataset) -> Result<Crs, IngestError> {
    let crs_string = dataset
        .spatial_ref()
        .and_then(|spatial_ref| spatial_ref.authority())
        .map_err(|err| {
            tracing::error!(error = %err, "failed to get spatial authoruty");

            IngestError::MetadataExtraction
        })?;

    Ok(Crs::new(crs_string))
}

// TODO: should accept target crs
#[tracing::instrument(
    skip(artifact_storage, metadata_storage),
    fields(
        dataset_id = %dataset_id,
        source_path = %source_path.display(),
    )
)]
pub async fn run(
    dataset_id: String,
    source_path: PathBuf,
    artifact_storage: impl ArtifactStorage,
    metadata_storage: impl MetadataStorage,
) -> Result<(), IngestError> {
    tracing::info!("starting ingest");
    let mut current_path = source_path;
    let mut tmp_paths = vec![];

    let dataset = Dataset::open(&current_path).map_err(|err| {
        tracing::error!(error = %err, path = %current_path.display(), "failed to open raster dataset");

        IngestError::MetadataExtraction
    })?;

    // If source have no knonw CRS - still try to reproject
    let source_crs = get_crs(&dataset).unwrap_or_else(|err| {
        tracing::warn!(error = %err, "can't get CRS from dataset, falling back to Unknown CRS");

        Crs::unknown()
    });
    if source_crs != Crs::new(BASE_CRS) {
        tracing::info!("reprojection required");
        let reprojected_path = gdal_process::reproject(&current_path, BASE_CRS).map_err(|err| {
            tracing::error!(error = %err, path = %current_path.display(), crs_to = %BASE_CRS, crs_from = %source_crs, "failed to reproject");

            IngestError::Reprojection
        })?;

        current_path = reprojected_path.clone();
        tmp_paths.push(reprojected_path);
    }

    let dataset = Dataset::open(&current_path).map_err(|err| {
        tracing::error!(error = %err, path = %current_path.display(), "failed to open raster dataset");

        IngestError::MetadataExtraction
    })?;

    if !is_cog(&dataset) {
        tracing::info!("cog conversion required");
        let translated_path = gdal_process::translate_to_cog(&current_path).map_err(|err| {
            tracing::error!(error = %err, path = %current_path.display(), "failed to translate to COG");

            IngestError::CogConversion
        })?;
        current_path = translated_path.clone();
        tmp_paths.push(translated_path);
    }

    let artifact_path = artifact_storage
        .save_artifact(&dataset_id, current_path.as_path())
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %current_path.display(), "failed to save to artifact storage");

            IngestError::ArtifactStorage
        })?;
    tracing::info!(artifact_path = %artifact_path, "artifact stored");

    let raster_metadata = read_raster_metadata(&current_path).map_err(|err| {
        tracing::debug!(error = %err, path = %current_path.display(), "failed to get raster metadata");

        IngestError::MetadataExtraction
    })?;
    let metadata = DatasetMetadata {
        dataset_id,
        artifact_path,
        raster: raster_metadata,
    };
    metadata_storage
        .save_metadata(metadata)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to save to metadata storage");

            IngestError::MetadataStorage
        })?;
    tracing::info!("metadata stored");

    // TODO: clean up on error
    for tmp_path in tmp_paths {
        if let Err(err) = std::fs::remove_file(&tmp_path) {
            tracing::warn!(error = %err, path = %tmp_path.display(), "failed to remove temp file");
        }
    }

    tracing::info!("ingest completed");
    Ok(())
}
