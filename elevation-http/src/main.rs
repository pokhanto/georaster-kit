use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info")),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    init_tracing();

    let metadata_storage_dir: PathBuf = dotenvy::var("METADATA_STORAGE_DIR")?.into();
    metadata_storage_dir.try_exists().inspect_err(|err| {
        tracing::error!(err = ?err, "metadata storage does not exist");
    })?;
    let app_host = dotenvy::var("APP_HOST")?;
    let app_port = dotenvy::var("APP_PORT")?;
    let listener = TcpListener::bind(format!("{app_host}:{app_port}")).await?;

    tracing::info!(
        address = format!("{app_host}:{app_port}"),
        "starting server at address"
    );

    let server = elevation_http::run(listener, metadata_storage_dir).await?;
    server.await?;

    Ok(())
}
