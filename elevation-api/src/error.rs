//! HTTP-facing application errors and response mapping.
//!
//! Converts internal service errors into API responses.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::application::ElevationServiceError;

#[derive(Debug, serde::Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Failed to calculate elevation data")]
    CalculateElevation,
}

impl From<ElevationServiceError> for AppError {
    fn from(value: ElevationServiceError) -> Self {
        match value {
            ElevationServiceError::Elevation(_) => AppError::CalculateElevation,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::CalculateElevation => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(ErrorResponse {
            message: self.to_string(),
        });

        (status, body).into_response()
    }
}
