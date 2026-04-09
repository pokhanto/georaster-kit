//! Shared types and traits for elevation tools.
//!
//! This crate defines core value types, metadata models, raster window types,
//! storage traits, and small spatial primitives.

mod elevation;
pub use elevation::{BboxElevations, Elevation};

mod metadata;
pub use metadata::{BlockSize, DatasetMetadata, GeoTransform, RasterMetadata};

mod raster;
pub use raster::{
    RasterReadWindow, RasterReader, RasterReaderError, RasterSize, RasterWindowData,
    RasterWindowDataError, ResolutionHint, WindowPlacement,
};

mod spatial;
pub use spatial::{Bounds, BoundsCreateError, Crs};

mod storage;
pub use storage::{
    ArtifactLocator, ArtifactResolveError, ArtifactResolver, ArtifactStorage, ArtifactStorageError,
    MetadataStorage, MetadataStorageError, ResolvedArtifactPath,
};
