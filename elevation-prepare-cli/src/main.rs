use clap::Parser;
use elevation_adapters::{FsArtifactStorage, FsMetadataStorage};
use elevation_domain::Crs;
use elevation_ingest::ingest;
use std::path::PathBuf;

mod telemetry;

// TODO: atm this is only supported CRS,
// so every dataset will be translated to it
const CRS: &str = "ESGP:4326";

/// Ingest source DEM dataset into artifact and metadata storage.
#[derive(Debug, Parser)]
#[command(
    name = "elevation-prepare",
    version,
    about = "Ingests source elevation dataset into base directory.",
    long_about = "Reads source elevation dataset, prepares artifacts, and stores metadata \
about ingested dataset in base directory.",
    next_line_help = true
)]
struct Args {
    /// Path to source dataset file to ingest.
    ///
    /// This is typically source DEM or raster file that will be processed and stored
    #[arg(long, value_name = "FILE")]
    source_dataset_path: PathBuf,

    /// Identifier for tdataset being ingested.
    ///
    /// This id is used to reference dataset later from application.
    #[arg(long, value_name = "DATASET_ID")]
    dataset_id: String,

    /// Base directory for metadata and generated artifacts.
    ///
    /// Tool will use this directory for storage backends.
    #[arg(long, value_name = "DIR")]
    base_dir: PathBuf,

    /// Name of metadata storage file.
    ///
    /// This name will be used for metadata storage filesystem implementation
    #[arg(long, value_name = "REGISTRY_NAME")]
    registry_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracing();

    let args = Args::parse();

    let Args {
        source_dataset_path,
        dataset_id,
        base_dir,
        registry_name,
    } = args;

    let metadata_storage = FsMetadataStorage::new(base_dir.to_owned(), registry_name);
    let artifact_storage = FsArtifactStorage::new(base_dir);

    ingest(
        dataset_id,
        source_dataset_path,
        Crs::new(CRS),
        artifact_storage,
        metadata_storage,
    )
    .await?;

    Ok(())
}
