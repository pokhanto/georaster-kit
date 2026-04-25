//! HTTP server wiring for elevations endpoints.
use axum::Router;
use georaster_adapters::{
    FsArtifactResolver, FsArtifactStorage, FsMetadataStorage, GdalRasterReader,
};
use georaster_core::{GeorasterService, IngestService};
use georaster_domain::Crs;
use std::{net::SocketAddr, path::PathBuf};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use crate::application::{ElevationService, PreparationService, PreparationServiceError};

mod application;
mod domain;
mod error;
mod routes;

pub use error::AppError;

#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("failed to prepare dataset on startup")]
    Prepare(#[source] PreparationServiceError),

    #[error("failed to bind tcp listener")]
    Bind(#[source] std::io::Error),

    #[error("server failed")]
    Serve(#[source] std::io::Error),
}

pub type AppElevationService =
    GeorasterService<FsMetadataStorage, GdalRasterReader<FsArtifactResolver>>;

/// Shared application state.
#[derive(Debug, Clone)]
pub struct AppState {
    pub elevation_service: ElevationService<AppElevationService>,
}

/// Prepares dataset and starts the HTTP server.
pub async fn run(
    app_addr: SocketAddr,
    storage_dir: PathBuf,
    file_to_ingest: PathBuf,
    metadata_registry_name: String,
) -> Result<(), StartupError> {
    let state = build_state(storage_dir, file_to_ingest, metadata_registry_name).await?;
    let app = build_router(state);

    let listener = TcpListener::bind(app_addr)
        .await
        .map_err(StartupError::Bind)?;

    tracing::info!(address = %app_addr, "starting server at address");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(StartupError::Serve)
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .nest("/elevations", routes::elevations_router())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn build_state(
    storage_dir: PathBuf,
    file_to_ingest: PathBuf,
    metadata_registry_name: String,
) -> Result<AppState, StartupError> {
    tracing::info!(?storage_dir, ?file_to_ingest, "preparing app state");

    // NOTE: CRS is not configurable at the moment
    let target_crs = Crs::new("EPSG:4326");

    let metadata_storage = FsMetadataStorage::new(&storage_dir, metadata_registry_name);
    let artifact_storage = FsArtifactStorage::new(&storage_dir);
    let raster_reader = GdalRasterReader::new(FsArtifactResolver);

    let ingest_service = IngestService::new(target_crs, artifact_storage, metadata_storage.clone());

    let preparation_service = PreparationService::new(ingest_service);
    preparation_service
        .ingest(file_to_ingest)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to prepare dataset.");

            StartupError::Prepare(err)
        })?;

    let georaster_service = GeorasterService::new(metadata_storage, raster_reader);

    Ok(AppState {
        elevation_service: ElevationService::new(georaster_service),
    })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
