//! Raster ingest pipeline.
//!
//! This module prepares source rasters for use by application by optionally
//! reprojecting them, converting them to COG, extracting metadata,
//! and storing artifacts and metadata.

use elevation_domain::{
    ArtifactStorage, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform, MetadataStorage,
    RasterMetadata,
};
use gdal::{Dataset, Metadata};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use crate::gdal_processor::{GdalProcessSettings, GdalProcessor};

/// Errors returned during dataset ingest.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
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

    #[error("Failed to create temporary workspace.")]
    TempWorkspace,
}

/// Opens GDAL dataset from disk.
fn open_dataset(path: &Path) -> Result<Dataset, IngestError> {
    Dataset::open(path).map_err(|err| {
        tracing::error!(error = %err, path = %path.display(), "failed to open raster dataset");
        IngestError::MetadataExtraction
    })
}

/// Extracts raster metadata from dataset at given path.
#[tracing::instrument(skip_all, fields(path = %path.display()))]
pub fn read_raster_metadata(path: &Path) -> Result<RasterMetadata, IngestError> {
    tracing::info!("starting metadata extraction");

    let dataset = open_dataset(path)?;

    let (width, height) = dataset.raster_size();
    tracing::debug!(width, height, "raster size extracted");

    let geo_transform = dataset.geo_transform().map_err(|err| {
        tracing::error!(error = %err, "failed to get geotransform");
        IngestError::MetadataExtraction
    })?;
    tracing::debug!(?geo_transform, "geotransform extracted");

    let min_lon = geo_transform[0];
    let max_lat = geo_transform[3];
    let max_lon = geo_transform[0] + width as f64 * geo_transform[1];
    let min_lat = geo_transform[3] + height as f64 * geo_transform[5];

    let crs = get_crs(&dataset)?;
    tracing::debug!(crs = ?crs, "crs extracted");

    let band = dataset.rasterband(1).map_err(|err| {
        tracing::error!(error = %err, "failed to get first raster band");
        IngestError::MetadataExtraction
    })?;

    let (block_width, block_height) = band.block_size();
    tracing::debug!(block_width, block_height, "block size extracted");

    let nodata = band.no_data_value();
    tracing::debug!(?nodata, "nodata value extracted");

    tracing::info!("metadata extraction completed");

    let bounds = Bounds::new(min_lon, min_lat, max_lon, max_lat).map_err(|err| {
        tracing::error!(error = %err, min_lon, min_lat, max_lon, max_lat, "provided bounds are not valid");
        IngestError::MetadataExtraction
    })?;

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
        bounds,
        nodata,
        block_size: BlockSize {
            width: block_width,
            height: block_height,
        },
        overview_count: 0,
    })
}

/// Returns `true` if dataset is marked as Cloud Optimized GeoTIFF.
fn is_cog(dataset: &Dataset) -> bool {
    dataset
        .metadata_domain("IMAGE_STRUCTURE")
        .unwrap_or_default()
        .iter()
        .any(|item| item == "LAYOUT=COG")
}

/// Extracts dataset's CRS.
fn get_crs(dataset: &Dataset) -> Result<Crs, IngestError> {
    let crs_string = dataset
        .spatial_ref()
        .and_then(|spatial_ref| spatial_ref.authority())
        .map_err(|err| {
            tracing::error!(error = %err, "failed to get spatial authority");
            IngestError::MetadataExtraction
        })?;

    Ok(Crs::new(crs_string))
}

#[tracing::instrument(
    skip(artifact_storage, metadata_storage),
    fields(
        dataset_id = %dataset_id,
        source_path = %source_path.display(),
        target_crs = %target_crs,
    )
)]
pub async fn run(
    dataset_id: String,
    source_path: PathBuf,
    target_crs: Crs,
    artifact_storage: impl ArtifactStorage,
    metadata_storage: impl MetadataStorage,
) -> Result<(), IngestError> {
    tracing::info!("starting ingest");

    let temp_dir = TempDir::new().map_err(|err| {
        tracing::error!(error = %err, "failed to create temp workspace");
        IngestError::TempWorkspace
    })?;

    let mut current_path = source_path;
    let gdal_processor = GdalProcessor::new(GdalProcessSettings::default());

    let dataset = open_dataset(&current_path)?;

    let source_crs = get_crs(&dataset).unwrap_or_else(|err| {
        tracing::warn!(error = %err, "failed to determine CRS, falling back to unknown CRS");
        Crs::unknown()
    });

    if source_crs != target_crs {
        tracing::info!(from = %source_crs, to = %target_crs, "reprojection required");

        let reprojected_path = temp_dir.path().join("reprojected.tif");

        gdal_processor
            .reproject_to_path(&current_path, target_crs.as_ref(), &reprojected_path)
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    path = %current_path.display(),
                    crs_from = %source_crs,
                    crs_to = %target_crs.as_ref(),
                    "failed to reproject raster"
                );
                IngestError::Reprojection
            })?;

        current_path = reprojected_path;
    }

    let dataset = open_dataset(&current_path)?;

    if !is_cog(&dataset) {
        tracing::info!("cog conversion required");

        let translated_path = temp_dir.path().join("translated.cog.tif");

        gdal_processor
            .translate_to_cog_path(&current_path, &translated_path)
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    path = %current_path.display(),
                    "failed to translate raster to COG"
                );
                IngestError::CogConversion
            })?;

        current_path = translated_path;
    }

    let artifact_path = artifact_storage
        .save_artifact(&dataset_id, current_path.as_path())
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                path = %current_path.display(),
                "failed to save artifact"
            );
            IngestError::ArtifactStorage
        })?;
    tracing::info!(artifact_path = %artifact_path, "artifact stored");

    let raster_metadata = read_raster_metadata(&current_path).map_err(|err| {
        tracing::error!(
            error = %err,
            path = %current_path.display(),
            "failed to extract raster metadata"
        );
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
            tracing::error!(error = %err, "failed to save metadata");
            IngestError::MetadataStorage
        })?;
    tracing::info!("metadata stored");

    tracing::info!("ingest completed");
    Ok(())
}
