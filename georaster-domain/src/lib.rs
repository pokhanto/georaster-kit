//! Shared types and traits for georaster tools.
//!
//! This crate defines core value types, metadata models, raster window types,
//! storage traits, and small spatial primitives.

mod metadata;
pub use metadata::{BlockSize, DatasetMetadata, GeoTransform, RasterBandMetadata, RasterMetadata};

mod raster;
pub use raster::{
    BandSelection, RasterBand, RasterGrid, RasterGridError, RasterPoint, RasterPointBand,
    RasterReadQuery, RasterReader, RasterReaderError, RasterRepresentation, RasterSize,
    WindowPlacement,
};

mod spatial;
pub use spatial::{Bounds, BoundsCreateError, Crs};

mod storage;
pub use storage::{
    ArtifactLocator, ArtifactResolveError, ArtifactResolver, ArtifactStorage, ArtifactStorageError,
    MetadataStorage, MetadataStorageError, ResolvedArtifactPath,
};
