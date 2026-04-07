//! Adapter implementations for elevation storage and raster access.
//!
//! This crate provides concrete implementations of shared domain traits:
//!
//! - [`FsMetadataStorage`] for filesystem-backed dataset metadata storage
//! - [`FsArtifactStorage`] for filesystem-backed raster artifact storage
//! - [`GdalRasterReader`] for reading raster windows using GDAL
//!
//! These adapters are intended to plug into higher-level services.

mod metadata_storage_fs;
pub use metadata_storage_fs::FsMetadataStorage;

mod artifact_storage_fs;
pub use artifact_storage_fs::FsArtifactStorage;

mod raster_reader_gdal;
pub use raster_reader_gdal::GdalRasterReader;
