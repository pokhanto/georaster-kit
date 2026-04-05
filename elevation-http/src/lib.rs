use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    serve::Serve,
};
use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::ElevationService;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::application::TileService;

mod application;
mod domain;
mod routes;

#[derive(Clone)]
pub struct AppState {
    pub tile_service: TileService<ElevationService<FsMetadataStorage, GdalRasterReader>>,
}

#[derive(serde::Serialize)]
pub struct ErrorResponse {
    pub message: String,
}
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Can't build tile")]
    BuildTile,
    #[error("Can't get tiles from requested bbox")]
    BboxTiles,
}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // TODO: messages
        match self {
            AppError::BboxTiles => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: "Internal server error.".to_string(),
                }),
            )
                .into_response(),
            AppError::BuildTile => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: "Internal server error.".to_string(),
                }),
            )
                .into_response(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RunError {}

pub async fn run(
    listener: TcpListener,
    metadata_storage_dir: PathBuf,
) -> Result<Serve<TcpListener, Router, Router>, RunError> {
    let metadata_storage = FsMetadataStorage::new(metadata_storage_dir);
    let raster_reader = GdalRasterReader;
    let elevation_service = ElevationService::new(metadata_storage, raster_reader);

    let state = AppState {
        tile_service: TileService::new(elevation_service),
    };

    let app = Router::new()
        .nest("/tiles", routes::tiles_router())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    Ok(axum::serve(listener, app))
}
