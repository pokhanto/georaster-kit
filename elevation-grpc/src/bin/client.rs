pub mod pb {
    tonic::include_proto!("elevation_service");
}

use tokio_stream::StreamExt;
use tonic::transport::Channel;

use pb::{LineStringElevationRequest, elevation_client::ElevationClient};

async fn streaming(client: &mut ElevationClient<Channel>) {
    let points = vec![
        (34.401450495938576, 48.582425478008275),
        (34.43395198327741, 48.577146572132875),
        (34.47662555708101, 48.589896138029786),
        (34.511339859349505, 48.63121425589273),
        (34.56260213351922, 48.63615005588619),
        (34.58610555222893, 48.61956524835753),
        (34.58504318681173, 48.58601779259695),
        (34.5625785029691, 48.540074191879995),
        (34.59173289431766, 48.51150159243997),
    ]
    .into_iter()
    .map(|(lon, lat)| pb::Point { lat, lon })
    .collect();

    let mut stream = client
        .line_string_elevation_streaming(LineStringElevationRequest { points })
        .await
        .unwrap()
        .into_inner();

    while let Some(item) = stream.next().await {
        println!("\treceived: {}", item.unwrap().elevation.unwrap());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ElevationClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    streaming(&mut client).await;

    Ok(())
}
