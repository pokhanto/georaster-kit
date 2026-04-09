use std::{pin::Pin, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

use crate::{
    application::ProfileService,
    grpc::{pb, status::profile_error_to_status},
};

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<pb::LineStringElevationResponse, Status>> + Send>>;

#[derive(Debug, Clone)]
pub struct ApiServer<EP> {
    profile_service: Arc<ProfileService<EP>>,
    sample_step_meters: f64,
}

impl<EP> ApiServer<EP> {
    pub fn new(profile_service: Arc<ProfileService<EP>>, sample_step_meters: f64) -> Self {
        Self {
            profile_service,
            sample_step_meters,
        }
    }
}

#[tonic::async_trait]
impl<EP> pb::elevation_server::Elevation for ApiServer<EP>
where
    EP: crate::application::ElevationProvider + Send + Sync + 'static,
{
    type LineStringElevationStreamingStream = ResponseStream;

    #[tracing::instrument(skip(self, req))]
    async fn line_string_elevation_streaming(
        &self,
        req: Request<pb::LineStringElevationRequest>,
    ) -> Result<Response<Self::LineStringElevationStreamingStream>, Status> {
        let request = req.into_inner();

        let coords: Vec<(f64, f64)> = request.points.into_iter().map(|p| (p.lon, p.lat)).collect();

        let sampled_path = self
            .profile_service
            .sample_points(&coords, self.sample_step_meters)
            .map_err(profile_error_to_status)?;

        let profile_service = Arc::clone(&self.profile_service);
        let (tx, rx) = mpsc::channel(128);
        tracing::info!(
            sample_count = sampled_path.len(),
            "starting elevation profile stream"
        );
        let sampled_points = self
            .profile_service
            .sample_points(&coords, self.sample_step_meters)
            .map_err(profile_error_to_status)?;

        tokio::spawn(async move {
            for (index, (lon, lat)) in sampled_points.into_iter().enumerate() {
                let sampled = match profile_service.sample_point(lon, lat).await {
                    Ok(sampled) => sampled,
                    Err(err) => {
                        tracing::error!(error = ?err, sample_index = index, "failed to sample elevation");
                        let _ = tx.send(Err(profile_error_to_status(err))).await;
                        return;
                    }
                };

                let response = pb::LineStringElevationResponse {
                    point: Some(pb::Point {
                        lon: sampled.lon,
                        lat: sampled.lat,
                    }),
                    elevation: sampled.elevation,
                };

                if tx.send(Ok(response)).await.is_err() {
                    tracing::debug!("client disconnected during stream");
                    return;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elevation_domain::Elevation;
    use tokio_stream::StreamExt;
    use tonic::Code;

    use crate::{
        application::{ElevationProvider, ElevationProviderError},
        grpc::pb::elevation_server::Elevation as _,
    };
    #[derive(Clone, Debug)]
    struct FakeElevationProvider {
        result: Result<Option<Elevation>, ElevationProviderError>,
    }

    impl ElevationProvider for FakeElevationProvider {
        async fn elevation_at_point(
            &self,
            _lon: f64,
            _lat: f64,
        ) -> Result<Option<Elevation>, ElevationProviderError> {
            self.result.clone()
        }
    }

    fn request_with_two_points() -> Request<pb::LineStringElevationRequest> {
        Request::new(pb::LineStringElevationRequest {
            points: vec![
                pb::Point {
                    lon: 34.4,
                    lat: 48.5,
                },
                pb::Point {
                    lon: 34.5,
                    lat: 48.6,
                },
            ],
        })
    }

    #[tokio::test]
    async fn streams_successful_responses() {
        let provider = FakeElevationProvider {
            result: Ok(Some(Elevation(123.0))),
        };
        let profile_service = Arc::new(ProfileService::new(provider, 100));
        let server = ApiServer::new(profile_service, 10000.0);

        let response = server
            .line_string_elevation_streaming(request_with_two_points())
            .await
            .unwrap();

        let mut stream = response.into_inner();

        let first = stream.next().await.unwrap().unwrap();

        assert!(first.point.is_some());
        assert_eq!(first.elevation, Some(123.0));
    }

    #[tokio::test]
    async fn stream_returns_internal_when_provider_fails() {
        let provider = FakeElevationProvider {
            result: Err(ElevationProviderError::Elevation(
                elevation_core::ElevationServiceError::MetadataLoad,
            )),
        };
        let profile_service = Arc::new(ProfileService::new(provider, 100));
        let server = ApiServer::new(profile_service, 10000.0);

        let response = server
            .line_string_elevation_streaming(request_with_two_points())
            .await
            .unwrap();

        let mut stream = response.into_inner();

        let first = stream.next().await.unwrap().unwrap_err();

        assert_eq!(first.code(), Code::Internal);
    }

    #[tokio::test]
    async fn streamed_response_contains_point_data() {
        let provider = FakeElevationProvider {
            result: Ok(Some(Elevation(50.0))),
        };
        let profile_service = Arc::new(ProfileService::new(provider, 100));
        let server = ApiServer::new(profile_service, 10000.0);

        let response = server
            .line_string_elevation_streaming(request_with_two_points())
            .await
            .unwrap();

        let mut stream = response.into_inner();

        let first = stream.next().await.unwrap().unwrap();

        let point = first.point.unwrap();
        assert!(point.lon.is_finite());
        assert!(point.lat.is_finite());
    }
}
