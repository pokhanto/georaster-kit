//! HTTP routes for tile lookup and tile streaming.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::sse::{Event, Sse},
    routing::get,
};
use elevation_domain::Bounds;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tokio::time::Instant;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use crate::{AppError, AppState, domain::Tile};

/// Query parameters for tile streaming over bounding box.
#[derive(Debug, Deserialize)]
pub struct TilesStreamRequest {
    pub zoom: u8,
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

/// HTTP response for tile.
#[derive(Serialize, Debug, Clone)]
pub struct TileResponse {
    id: String,
    elevation: Option<f64>,
}

impl From<Tile> for TileResponse {
    fn from(value: Tile) -> Self {
        Self {
            id: value.id().to_owned(),
            elevation: value.elevation().map(|e| e.0),
        }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type")]
enum ServerEvent {
    Tile(TileResponse),
    Error,
    Done,
}

/// Builds tile routes.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stream", get(stream_tiles))
        .route("/{id}", get(get_tile))
}

/// Returns a single tile by id.
#[tracing::instrument(skip(state), fields(tile_id = %id))]
pub async fn get_tile(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TileResponse>, AppError> {
    tracing::info!("starting handling get tile");
    let tile = state
        .tile_service
        .get_tile_by_id(id)
        .await
        .inspect_err(|err| {
            tracing::error!(error = ?err, "failed to build tile");
        })?;

    Ok(Json(tile.into()))
}

/// Streams tiles for the requested bounding box.
#[tracing::instrument(
    skip(state),
    fields(
        zoom = request.zoom,
        min_lon = request.min_lon,
        min_lat = request.min_lat,
        max_lon = request.max_lon,
        max_lat = request.max_lat,
    )
)]
pub async fn stream_tiles(
    State(state): State<AppState>,
    Query(request): Query<TilesStreamRequest>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, AppError> {
    tracing::info!("starting handling tiles stream");
    let TilesStreamRequest {
        zoom,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
    } = request;
    let bbox = Bounds::try_new(min_lon, min_lat, max_lon, max_lat).map_err(|err| {
        tracing::error!(error = ?err, "invalid bbox provided in request");
        AppError::InvalidBounds
    })?;

    let tile_ids = state
        .tile_service
        .get_tile_ids_for_bbox(bbox, zoom)
        .inspect_err(|err| {
            tracing::error!(error = ?err, "failed to resolve tile ids for bbox");
        })?;

    tracing::debug!(tile_count = tile_ids.len(), "resolved tile ids for stream");

    let (tx, rx) = tokio::sync::mpsc::channel::<ServerEvent>(32);

    tokio::spawn(async move {
        for tile_id in tile_ids {
            let started_at = Instant::now();

            let tile = match state.tile_service.get_tile_by_id(tile_id.clone()).await {
                Ok(tile) => tile,
                Err(err) => {
                    tracing::error!(tile_id = %tile_id, error = ?err, "failed to build tile in stream");

                    if tx.send(ServerEvent::Error).await.is_err() {
                        tracing::debug!("client disconnected while sending tile error event");
                        return;
                    }

                    continue;
                }
            };

            tracing::info!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "tile resolved"
            );

            if tx.send(ServerEvent::Tile(tile.into())).await.is_err() {
                tracing::debug!("client disconnected while sending tile event");
                return;
            }
        }

        if tx.send(ServerEvent::Done).await.is_err() {
            tracing::debug!("client disconnected before done event");
        }
    });

    let stream = ReceiverStream::new(rx).map(|payload| {
        let event_name = match &payload {
            ServerEvent::Tile(_) => "tile",
            ServerEvent::Error => "error",
            ServerEvent::Done => "done",
        };

        let event = match Event::default().event(event_name).json_data(payload) {
            Ok(event) => event,
            Err(err) => {
                tracing::error!(error = ?err, "failed to serialize SSE event");

                Event::default()
                    .event("error")
                    .data("Failed to serialize event")
            }
        };

        Ok(event)
    });

    Ok(Sse::new(stream))
}
