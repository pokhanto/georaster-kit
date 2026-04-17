//! Adapter implementations for artifact storages, resolvers,
//! metadata storages and raster readers.
//!
//! These adapters are intended to plug into higher-level services.

mod metadata_storages;
pub use metadata_storages::FsMetadataStorage;

mod raster_readers;
pub use raster_readers::GdalRasterReader;

mod artifact_storages;
pub use artifact_storages::{FsArtifactStorage, S3ArtifactStorage};

mod artifact_resolvers;
pub use artifact_resolvers::{FsArtifactResolver, GdalS3ArtifactResolver};
