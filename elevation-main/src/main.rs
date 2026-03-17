use clap::Parser;
use elevation_core::ElevationService;
use elevation_core::raster_reader::GdalRasterReader;
use elevation_ingest::{FsArtifactStorage, FsMetadataStorage, ingest};
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    source: PathBuf,
}

fn main() {
    let args = Args::parse();
    let source_path = args.source;
    let metadata_storage = FsMetadataStorage {};
    let artifact_storage = FsArtifactStorage {};

    ingest(source_path, artifact_storage, metadata_storage);

    let metadata_storage = FsMetadataStorage {};
    let raster_reader = GdalRasterReader;

    let service = ElevationService::new(metadata_storage, raster_reader);

    let elevation = service.elevation_at(36.2304, 49.9935);
    println!("elev {:?}", elevation);
}
