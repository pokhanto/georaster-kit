use std::path::PathBuf;

use clap::Parser;
use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::ElevationService;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    lon: f64,
    #[arg(long)]
    lat: f64,
    #[arg(long)]
    base_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let Args { lon, lat, base_dir } = args;

    let metadata_storage = FsMetadataStorage::new(base_dir);
    let raster_reader = GdalRasterReader;

    let service = ElevationService::new(metadata_storage, raster_reader);

    let elevation = service
        .elevation_at_point(lon, lat)
        .await
        .expect("Can't get elevation");
    println!("elev {:?}", elevation);
}
