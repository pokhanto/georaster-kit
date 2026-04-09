//! HTTP server wiring for tile endpoints.
use axum::Router;
use elevation_adapters::{FsArtifactResolver, FsMetadataStorage, GdalRasterReader};
use elevation_core::ElevationService;
use std::{net::SocketAddr, path::PathBuf};
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::application::TileService;

mod application;
mod domain;
mod error;
mod routes;

pub use error::AppError;

/// Shared application state.
#[derive(Debug, Clone)]
pub struct AppState {
    pub tile_service:
        TileService<ElevationService<FsMetadataStorage, GdalRasterReader<FsArtifactResolver>>>,
}

/// Starts the HTTP server.
pub async fn run(
    app_addr: SocketAddr,
    metadata_storage_dir: PathBuf,
    tile_cache_max_capacity: u64,
    metadata_registry_name: String,
) -> Result<(), std::io::Error> {
    metadata_storage_dir.try_exists().inspect_err(|err| {
        tracing::error!(err = ?err, "metadata storage is not ready");
    })?;

    let metadata_storage = FsMetadataStorage::new(metadata_storage_dir, metadata_registry_name);
    let raster_reader = GdalRasterReader::new(FsArtifactResolver);
    let elevation_service = ElevationService::new(metadata_storage, raster_reader);

    let state = AppState {
        tile_service: TileService::new(elevation_service, tile_cache_max_capacity),
    };
    let app = Router::new()
        .nest("/tiles", routes::tiles_router())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = TcpListener::bind(app_addr).await?;

    tracing::info!(address = %app_addr, "starting server at address");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
