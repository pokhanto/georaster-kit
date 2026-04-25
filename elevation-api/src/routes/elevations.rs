//! HTTP routes for serving elevations.

use axum::{Json, Router, extract::State, routing::post};

use crate::{
    AppError, AppState,
    domain::{Coord, CoordWithElevation},
};

pub fn router() -> Router<AppState> {
    Router::new().route("/", post(get_elevations))
}

#[tracing::instrument(skip(state))]
async fn get_elevations(
    State(state): State<AppState>,
    Json(payload): Json<Vec<Coord>>,
) -> Result<Json<Vec<CoordWithElevation>>, AppError> {
    tracing::info!("starting handling get elevations");

    let coord_with_elevations = state
        .elevation_service
        .elevations_at_point(&payload)
        .await?;

    Ok(Json(coord_with_elevations))
}
