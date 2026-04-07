//! HTTP server entrypoint.
mod config;
mod telemetry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracing();

    let config::Config {
        metadata_dir,
        app_addr,
        tile_cache_max_capacity,
        metadata_registry_name,
    } = config::Config::from_env()?;

    elevation_tiles_http::run(
        app_addr,
        metadata_dir,
        tile_cache_max_capacity,
        metadata_registry_name,
    )
    .await?;

    Ok(())
}
