//! HTTP server entrypoint.
mod config;
mod telemetry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracing();

    let config::Config {
        storage_dir,
        app_addr,
        file_to_ingest,
        metadata_registry_name,
    } = config::Config::from_env()?;

    elevation_api::run(
        app_addr,
        storage_dir,
        file_to_ingest,
        metadata_registry_name,
    )
    .await?;

    Ok(())
}
