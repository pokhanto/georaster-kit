//! HTTP-facing application errors and response mapping.
//!
//! Converts internal service errors into API responses.
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::application::TileServiceError;

#[derive(serde::Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("Invalid bounds")]
    InvalidBounds,
    #[error("Invalid zoom level")]
    InvalidZoomLevel,
    #[error("Tile not found")]
    TileNotFound,
    #[error("Failed to resolve tiles for bbox")]
    ResolveTiles,
    #[error("Failed to compute tile elevation")]
    ComputeTile,
}

impl From<TileServiceError> for AppError {
    fn from(value: TileServiceError) -> Self {
        match value {
            TileServiceError::ZoomLevel => AppError::InvalidZoomLevel,
            TileServiceError::BuildTiles => AppError::ResolveTiles,
            TileServiceError::UnknownTile => AppError::TileNotFound,
            TileServiceError::Elevation => AppError::ComputeTile,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::InvalidBounds => (StatusCode::BAD_REQUEST, "Invalid bounds."),
            AppError::InvalidZoomLevel => (StatusCode::BAD_REQUEST, "Invalid zoom level."),
            AppError::TileNotFound => (StatusCode::NOT_FOUND, "Tile not found."),
            AppError::ResolveTiles => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error."),
            AppError::ComputeTile => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error."),
        };

        (
            status,
            Json(ErrorResponse {
                message: message.to_string(),
            }),
        )
            .into_response()
    }
}
