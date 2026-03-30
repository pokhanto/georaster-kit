pub mod pb {
    tonic::include_proto!("elevation_service");
}

use elevation_adapters::{FsMetadataStorage, GdalRasterReader};
use elevation_core::ElevationService;
use std::{net::ToSocketAddrs, path::PathBuf, pin::Pin, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Server};
use tracing_subscriber::FmtSubscriber;

use pb::{LineStringElevationRequest, LineStringElevationResponse};

type LineStringResult<T> = Result<Response<T>, Status>;
type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<LineStringElevationResponse, Status>> + Send>>;

pub struct ApiServer {
    elevation_service: Arc<ElevationService<FsMetadataStorage, GdalRasterReader>>,
}

impl ApiServer {
    pub fn new(elevation_service: ElevationService<FsMetadataStorage, GdalRasterReader>) -> Self {
        Self {
            elevation_service: Arc::new(elevation_service),
        }
    }
}

#[tonic::async_trait]
impl pb::elevation_server::Elevation for ApiServer {
    type LineStringElevationStreamingStream = ResponseStream;

    async fn line_string_elevation_streaming(
        &self,
        req: Request<LineStringElevationRequest>,
    ) -> LineStringResult<Self::LineStringElevationStreamingStream> {
        let points: Vec<(f64, f64)> = req
            .into_inner()
            .points
            .into_iter()
            .map(|p| (p.lon, p.lat))
            .collect();

        let points = sample_polyline_every_meters(&points, 50.0);

        let elevation_service = Arc::clone(&self.elevation_service);
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            for point in points {
                let value = elevation_service.elevation_at(point.0, point.1).unwrap();
                let response = LineStringElevationResponse {
                    point: Some(pb::Point::default()),
                    elevation: value.map(|e| e.0),
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(output_stream) as ResponseStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let base_dir = PathBuf::from("./");
    let metadata_storage = FsMetadataStorage::new(base_dir);
    let raster_reader = GdalRasterReader;
    let elevation_service = ElevationService::new(metadata_storage, raster_reader);
    let server = ApiServer::new(elevation_service);
    Server::builder()
        .add_service(pb::elevation_server::ElevationServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}

use geo::{Haversine, InterpolatableLine, Length, LineString};

fn sample_polyline_every_meters(coords: &[(f64, f64)], step_meters: f64) -> Vec<(f64, f64)> {
    let line: LineString = coords.iter().copied().collect();

    let total = Haversine.length(&line);
    let mut out = Vec::new();

    let mut distance = 0.0;
    while distance <= total {
        if let Some(p) = line.point_at_distance_from_start(&Haversine, distance) {
            out.push((p.x(), p.y()));
        }
        distance += step_meters;
    }

    if let Some(last) = coords.last()
        && out.last().copied() != Some(*last)
    {
        out.push(*last);
    }

    out
}
