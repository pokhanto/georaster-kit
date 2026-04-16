pub mod pb {
    tonic::include_proto!("elevation_service");
}

use std::fs::File;

use serde::Deserialize;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use pb::{LineStringElevationRequest, elevation_client::ElevationClient};

#[derive(Debug, Deserialize)]
struct CoordinatesFile {
    coordinates: Vec<[f64; 2]>,
}

async fn streaming(
    client: &mut ElevationClient<Channel>,
    points: Vec<pb::Point>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = client
        .line_string_elevation_streaming(LineStringElevationRequest { points })
        .await?
        .into_inner();

    while let Some(item) = stream.next().await {
        let item = item?;

        match (item.point, item.elevation) {
            (Some(point), Some(elevation)) => {
                println!(
                    "received: lon={}, lat={}, elevation={}",
                    point.lon, point.lat, elevation
                );
            }
            (Some(point), None) => {
                println!(
                    "received: lon={}, lat={}, no elevation",
                    point.lon, point.lat
                );
            }
            _ => {
                println!("received incomplete response");
            }
        }
    }

    Ok(())
}

fn load_points_from_json(path: &str) -> Result<Vec<pb::Point>, Box<dyn std::error::Error>> {
    let data: CoordinatesFile = serde_json::from_reader(File::open(path)?)?;

    if data.coordinates.len() < 2 {
        return Err("coordinates must contain at least two points".into());
    }

    let points = data
        .coordinates
        .into_iter()
        .map(|[lon, lat]| pb::Point { lon, lat })
        .collect();

    Ok(points)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./tests/fixtures/points.json".to_string());

    let points = load_points_from_json(&path)?;

    let mut client = ElevationClient::connect("http://0.0.0.0:50051").await?;

    streaming(&mut client, points).await?;

    Ok(())
}
