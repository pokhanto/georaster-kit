//! Raster ingest pipeline.
//!
//! This module prepares source rasters for use by application by optionally
//! reprojecting them, converting them to COG, extracting metadata,
//! and storing artifacts and metadata.

use gdal::{Dataset, Metadata};
use georaster_domain::{
    ArtifactStorage, ArtifactStorageError, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform,
    MetadataStorage, RasterBandMetadata, RasterMetadata, RasterRepresentation,
};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

use super::gdal_processor::{GdalProcessSettings, GdalProcessor};

/// Errors returned during dataset ingest.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum IngestServiceError {
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

    #[error("Dataset already exists")]
    DuplicatedId,
}

pub struct IngestService<A, M> {
    crs: Crs,
    artifact_storage: A,
    metadata_storage: M,
}

impl<A, M> IngestService<A, M> {
    pub fn new(crs: Crs, artifact_storage: A, metadata_storage: M) -> Self {
        Self {
            crs,
            artifact_storage,
            metadata_storage,
        }
    }
}

impl<A, M> IngestService<A, M>
where
    A: ArtifactStorage,
    M: MetadataStorage,
{
    #[tracing::instrument(
    skip(self, dataset_id),
    fields(
        crs = %self.crs,
        source_path = %source_path.display(),
    )
)]
    pub async fn run(
        &self,
        dataset_id: impl Into<String>,
        source_path: PathBuf,
    ) -> Result<(), IngestServiceError> {
        tracing::info!("starting ingest");

        let dataset_id: String = dataset_id.into();

        tracing::Span::current().record("dataset_id", tracing::field::display(&dataset_id));
        let temp_dir = TempDir::new().map_err(|err| {
            tracing::debug!(error = %err, "failed to create temp workspace");
            IngestServiceError::TempWorkspace
        })?;

        let mut current_path = source_path;
        let gdal_processor = GdalProcessor::new(GdalProcessSettings::default());

        let dataset = open_dataset(&current_path)?;

        let source_crs = get_crs(&dataset).unwrap_or_else(|err| {
            tracing::warn!(error = %err, "failed to determine CRS, falling back to unknown CRS");
            Crs::unknown()
        });

        if source_crs != self.crs {
            tracing::info!(from = %source_crs, to = %self.crs, "reprojection required");

            let reprojected_path = temp_dir.path().join("reprojected.tif");

            gdal_processor
                .reproject_to_path(&current_path, self.crs.as_ref(), &reprojected_path)
                .map_err(|err| {
                    tracing::debug!(
                        error = %err,
                        path = %current_path.display(),
                        crs_from = %source_crs,
                        crs_to = %self.crs.as_ref(),
                        "failed to reproject raster"
                    );
                    IngestServiceError::Reprojection
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
                    tracing::debug!(
                        error = %err,
                        path = %current_path.display(),
                        "failed to translate raster to COG"
                    );
                    IngestServiceError::CogConversion
                })?;

            current_path = translated_path;
        }

        let artifact_path = self
            .artifact_storage
            .save_artifact(&dataset_id, current_path.as_path())
            .await
            .map_err(|err| {
                tracing::debug!(
                    error = %err,
                    path = %current_path.display(),
                    "failed to save artifact"
                );
                if err == ArtifactStorageError::DuplicateId {
                    IngestServiceError::DuplicatedId
                } else {
                    IngestServiceError::ArtifactStorage
                }
            })?;
        tracing::info!(artifact_path = %artifact_path, "artifact stored");

        let raster_metadata = read_raster_metadata(&current_path).map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %current_path.display(),
                "failed to extract raster metadata"
            );
            IngestServiceError::MetadataExtraction
        })?;

        let metadata = DatasetMetadata {
            dataset_id,
            artifact_path,
            raster: raster_metadata,
        };

        self.metadata_storage
            .save_metadata(metadata)
            .await
            .map_err(|err| {
                tracing::debug!(error = %err, "failed to save metadata");
                IngestServiceError::MetadataStorage
            })?;
        tracing::info!("metadata stored");

        tracing::info!("ingest completed");
        Ok(())
    }
}

/// Opens GDAL dataset from disk.
fn open_dataset(path: &Path) -> Result<Dataset, IngestServiceError> {
    Dataset::open(path).map_err(|err| {
        tracing::debug!(error = %err, path = %path.display(), "failed to open raster dataset");
        IngestServiceError::MetadataExtraction
    })
}

/// Extracts raster metadata from dataset at given path.
#[tracing::instrument(skip_all, fields(path = %path.display()))]
pub fn read_raster_metadata(path: &Path) -> Result<RasterMetadata, IngestServiceError> {
    tracing::info!(path = %path.display(), "starting metadata extraction");

    let dataset = open_dataset(path)?;

    let (width, height) = dataset.raster_size();
    tracing::debug!(width, height, "raster size extracted");

    let band_count = dataset.raster_count();
    tracing::debug!(band_count, "raster band count extracted");

    let driver = dataset.driver();
    let format = driver.short_name();
    tracing::debug!(format, "dataset format extracted");

    let geo_transform = dataset.geo_transform().map_err(|err| {
        tracing::debug!(error = %err, "failed to get geotransform");
        IngestServiceError::MetadataExtraction
    })?;
    tracing::debug!(?geo_transform, "geotransform extracted");

    let crs = get_crs(&dataset)?;
    tracing::debug!(crs = ?crs, "crs extracted");

    let bands = (1..=band_count)
        .map(|band_index| {
            let band = dataset.rasterband(band_index).map_err(|err| {
                tracing::debug!(error = %err, band_index, "failed to get raster band");
                IngestServiceError::MetadataExtraction
            })?;

            let (block_width, block_height) = band.block_size();
            let nodata = band.no_data_value();
            let color_interpretation = band.color_interpretation().name();

            let overview_count = band.overview_count().unwrap_or(0);

            tracing::debug!(
                band_index,
                block_width,
                block_height,
                ?nodata,
                color_interpretation,
                overview_count,
                "band metadata extracted"
            );

            Ok::<_, IngestServiceError>(RasterBandMetadata {
                band_index,
                nodata,
                block_size: BlockSize {
                    width: block_width,
                    height: block_height,
                },
                color_interpretation,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let raster_representation = infer_raster_representation(&bands);

    let overview_count = dataset
        .rasterband(1)
        .ok()
        .and_then(|band| band.overview_count().ok())
        .unwrap_or(0) as usize;

    // Assumes north up, nonrotated raster:
    // - pixel_width > 0
    // - pixel_height < 0
    // If this is not true, ingest should reproject/normalize first.
    let [
        origin_lon,
        pixel_width,
        rot_x,
        origin_lat,
        rot_y,
        pixel_height,
    ] = geo_transform;

    if rot_x != 0.0 || rot_y != 0.0 {
        tracing::debug!(
            rot_x,
            rot_y,
            "rotated/skewed rasters are not supported by this metadata extraction path"
        );
        return Err(IngestServiceError::MetadataExtraction);
    }

    let lon_1 = origin_lon;
    let lon_2 = origin_lon + width as f64 * pixel_width;
    let lat_1 = origin_lat;
    let lat_2 = origin_lat + height as f64 * pixel_height;

    let min_lon = lon_1.min(lon_2);
    let max_lon = lon_1.max(lon_2);
    let min_lat = lat_1.min(lat_2);
    let max_lat = lat_1.max(lat_2);

    let bounds = Bounds::try_new(min_lon, min_lat, max_lon, max_lat).map_err(|err| {
        tracing::debug!(
            error = %err,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            "provided bounds are not valid"
        );
        IngestServiceError::MetadataExtraction
    })?;

    tracing::info!("metadata extraction completed");

    Ok(RasterMetadata {
        crs,
        width,
        height,
        geo_transform: GeoTransform {
            origin_lon,
            origin_lat,
            pixel_width,
            pixel_height,
        },
        bounds,
        overview_count,
        raster_representation,
        bands,
    })
}

/// Get raster representation based on bands.
fn infer_raster_representation(bands: &[RasterBandMetadata]) -> RasterRepresentation {
    let has_red = bands.iter().any(|b| b.color_interpretation == "Red");
    let has_green = bands.iter().any(|b| b.color_interpretation == "Green");
    let has_blue = bands.iter().any(|b| b.color_interpretation == "Blue");
    let has_alpha = bands.iter().any(|b| b.color_interpretation == "Alpha");

    if has_red && has_green && has_blue && has_alpha {
        RasterRepresentation::Rgba
    } else if has_red && has_green && has_blue {
        RasterRepresentation::Rgb
    } else if bands.len() == 1 {
        RasterRepresentation::Grayscale
    } else {
        RasterRepresentation::Unknown
    }
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
fn get_crs(dataset: &Dataset) -> Result<Crs, IngestServiceError> {
    let crs_string = dataset
        .spatial_ref()
        .and_then(|spatial_ref| spatial_ref.authority())
        .map_err(|err| {
            tracing::debug!(error = %err, "failed to get spatial authority");
            IngestServiceError::MetadataExtraction
        })?;

    Ok(Crs::new(crs_string))
}
