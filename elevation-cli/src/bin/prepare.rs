use clap::Parser;
use elevation_adapters::{FsArtifactStorage, FsMetadataStorage};
use elevation_ingest::ingest;
use std::path::PathBuf;
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    dataset_id: String,
    #[arg(long)]
    base_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let args = Args::parse();
    let Args {
        source,
        dataset_id,
        base_dir,
    } = args;
    let metadata_storage = FsMetadataStorage::new(base_dir.to_owned());
    let artifact_storage = FsArtifactStorage::new(base_dir);

    ingest(dataset_id, source, artifact_storage, metadata_storage)
        .await
        .expect("ingest failed");
}
