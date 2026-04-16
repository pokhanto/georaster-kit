use elevation_adapters::{FsMetadataStorage, GdalRasterReader, GdalS3ArtifactResolver};
use elevation_core::ElevationService;
use elevation_profile_grpc::{
    Config, ProfileService,
    grpc::{ApiServer, pb},
    telemetry,
};
use std::sync::Arc;
use tonic::transport::Server;

pub type AppElevationService =
    ElevationService<FsMetadataStorage, GdalRasterReader<GdalS3ArtifactResolver>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    telemetry::init_tracing();

    let config = Config::from_env()?;

    let metadata_storage =
        FsMetadataStorage::new(config.metadata_dir, config.metadata_registry_name);
    let raster_reader = GdalRasterReader::new(GdalS3ArtifactResolver);
    let elevation_service = ElevationService::new(metadata_storage, raster_reader);

    let profile_service = Arc::new(ProfileService::new(elevation_service, config.max_samples));
    let api_server = ApiServer::new(Arc::clone(&profile_service), config.sample_step_meters);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<pb::elevation_server::ElevationServer<ApiServer<AppElevationService>>>()
        .await;

    tracing::info!(addr = %config.grpc_addr, "starting gRPC server");

    Server::builder()
        .add_service(health_service)
        .add_service(pb::elevation_server::ElevationServer::new(api_server))
        .serve_with_shutdown(config.grpc_addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
