use geo::{Haversine, InterpolatableLine, Length, LineString};
use thiserror::Error;

use crate::application::{ElevationProvider, ElevationProviderError};

#[derive(Debug, Error, PartialEq, Clone)]
pub enum ProfileServiceError {
    #[error("Path must contain at least two points")]
    TooFewPoints,
    #[error("Sample step must be greater than zero")]
    InvalidStep,
    #[error("Invalid coordinate at index {index}")]
    InvalidCoordinate { index: usize },
    #[error("Too many sampled points")]
    TooManySamples,
    #[error("Elevation lookup failed")]
    Elevation(#[from] ElevationProviderError),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SampledPointElevation {
    pub lon: f64,
    pub lat: f64,
    pub elevation: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct ProfileService<EP> {
    elevation_provider: EP,
    max_samples: usize,
}

impl<EP> ProfileService<EP>
where
    EP: ElevationProvider,
{
    pub fn new(elevation_provider: EP, max_samples: usize) -> Self {
        Self {
            elevation_provider,
            max_samples,
        }
    }

    #[tracing::instrument(skip(self, coords), fields(point_count = coords.len(), step_meters))]
    pub fn sample_points(
        &self,
        coords: &[(f64, f64)],
        step_meters: f64,
    ) -> Result<Vec<(f64, f64)>, ProfileServiceError> {
        self.validate_input(coords, step_meters)?;

        let line: LineString = coords.iter().copied().collect();
        let total = Haversine.length(&line);

        let mut points = Vec::new();
        let mut distance = 0.0;

        while distance <= total {
            if let Some(point) = line.point_at_distance_from_start(&Haversine, distance) {
                points.push((point.x(), point.y()));
            }
            distance += step_meters;
        }

        if let Some(last) = coords.last()
            && points.last().copied() != Some(*last)
        {
            points.push(*last);
        }

        if points.len() > self.max_samples {
            return Err(ProfileServiceError::TooManySamples);
        }

        tracing::debug!(sample_count = points.len(), "sampled path points built");

        Ok(points)
    }

    #[tracing::instrument(skip(self), fields(lon, lat))]
    pub async fn sample_point(
        &self,
        lon: f64,
        lat: f64,
    ) -> Result<SampledPointElevation, ProfileServiceError> {
        let elevation = self.elevation_provider.elevation_at_point(lon, lat).await?;

        Ok(SampledPointElevation {
            lon,
            lat,
            elevation: elevation.map(|point| point.into_bands()[0].value()),
        })
    }

    fn validate_input(
        &self,
        coords: &[(f64, f64)],
        step_meters: f64,
    ) -> Result<(), ProfileServiceError> {
        if coords.len() < 2 {
            return Err(ProfileServiceError::TooFewPoints);
        }

        if !(step_meters.is_finite() && step_meters > 0.0) {
            return Err(ProfileServiceError::InvalidStep);
        }

        for (index, (lon, lat)) in coords.iter().enumerate() {
            let valid = lon.is_finite()
                && lat.is_finite()
                && (-180.0..=180.0).contains(lon)
                && (-90.0..=90.0).contains(lat);

            if !valid {
                return Err(ProfileServiceError::InvalidCoordinate { index });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use georaster_core::GeorasterServiceError;
    use georaster_domain::{RasterPoint, RasterPointBand};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    struct FakeElevationProvider {
        result: Result<Option<RasterPoint>, ElevationProviderError>,
        calls: Arc<Mutex<Vec<(f64, f64)>>>,
    }

    impl FakeElevationProvider {
        fn ok(value: Option<RasterPoint>) -> Self {
            Self {
                result: Ok(value),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn err(error: ElevationProviderError) -> Self {
            Self {
                result: Err(error),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(f64, f64)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl ElevationProvider for FakeElevationProvider {
        async fn elevation_at_point(
            &self,
            lon: f64,
            lat: f64,
        ) -> Result<Option<RasterPoint>, ElevationProviderError> {
            self.calls.lock().unwrap().push((lon, lat));
            self.result.clone()
        }
    }

    fn valid_coords() -> Vec<(f64, f64)> {
        vec![
            (34.401450495938576, 48.582425478008275),
            (34.43395198327741, 48.577146572132875),
        ]
    }

    #[test]
    fn sample_points_returns_too_few_points() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);

        let result = service.sample_points(&[(34.4, 48.5)], 50.0);

        assert_eq!(result.unwrap_err(), ProfileServiceError::TooFewPoints);
    }

    #[test]
    fn sample_points_returns_invalid_step_for_zero() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);

        let result = service.sample_points(&valid_coords(), 0.0);

        assert_eq!(result.unwrap_err(), ProfileServiceError::InvalidStep);
    }

    #[test]
    fn sample_points_returns_invalid_step_for_negative() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);

        let result = service.sample_points(&valid_coords(), -10.0);

        assert_eq!(result.unwrap_err(), ProfileServiceError::InvalidStep);
    }

    #[test]
    fn sample_points_returns_invalid_step_for_nan() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);

        let result = service.sample_points(&valid_coords(), f64::NAN);

        assert_eq!(result.unwrap_err(), ProfileServiceError::InvalidStep);
    }

    #[test]
    fn sample_points_returns_invalid_coordinate_for_bad_lon() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 50);
        let coords = vec![(181.0, 48.5), (34.4, 48.6)];

        let result = service.sample_points(&coords, 50.0);

        assert_eq!(
            result.unwrap_err(),
            ProfileServiceError::InvalidCoordinate { index: 0 }
        );
    }

    #[test]
    fn sample_points_returns_invalid_coordinate_for_bad_lat() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);
        let coords = vec![(34.4, 95.0), (34.5, 48.6)];

        let result = service.sample_points(&coords, 50.0);

        assert_eq!(
            result.unwrap_err(),
            ProfileServiceError::InvalidCoordinate { index: 0 }
        );
    }

    #[test]
    fn sample_points_returns_invalid_coordinate_for_nan() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);
        let coords = vec![(f64::NAN, 48.5), (34.5, 48.6)];

        let result = service.sample_points(&coords, 50.0);

        assert_eq!(
            result.unwrap_err(),
            ProfileServiceError::InvalidCoordinate { index: 0 }
        );
    }

    #[test]
    fn sample_points_includes_last_original_point() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);
        let coords = valid_coords();

        let result = service.sample_points(&coords, 10000.0).unwrap();

        assert_eq!(result.last().copied(), coords.last().copied());
    }

    #[test]
    fn sample_points_returns_non_empty_points_for_valid_path() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);

        let result = service.sample_points(&valid_coords(), 50.0).unwrap();

        assert!(!result.is_empty());
    }

    #[test]
    fn sample_points_with_large_step_returns_end_point() {
        let service = ProfileService::new(FakeElevationProvider::ok(None), 500);
        let coords = valid_coords();

        let result = service.sample_points(&coords, 1000000.0).unwrap();

        assert_eq!(result.last().copied(), coords.last().copied());
    }

    #[tokio::test]
    async fn sample_point_returns_sampled_point_with_elevation() {
        let provider =
            FakeElevationProvider::ok(Some(RasterPoint::new(vec![RasterPointBand::new(1, 123.0)])));
        let service = ProfileService::new(provider, 50);

        let result = service.sample_point(34.4, 48.5).await.unwrap();

        assert_eq!(
            result,
            SampledPointElevation {
                lon: 34.4,
                lat: 48.5,
                elevation: Some(123.0),
            }
        );
    }

    #[tokio::test]
    async fn sample_point_returns_sampled_point_with_none_elevation() {
        let provider = FakeElevationProvider::ok(None);
        let service = ProfileService::new(provider, 50);

        let result = service.sample_point(34.4, 48.5).await.unwrap();

        assert_eq!(
            result,
            SampledPointElevation {
                lon: 34.4,
                lat: 48.5,
                elevation: None,
            }
        );
    }

    #[tokio::test]
    async fn sample_point_maps_provider_error() {
        let provider = FakeElevationProvider::err(ElevationProviderError::Elevation(
            GeorasterServiceError::MetadataLoad,
        ));
        let service = ProfileService::new(provider, 500);

        let result = service.sample_point(34.4, 48.5).await;

        assert!(matches!(
            result.unwrap_err(),
            ProfileServiceError::Elevation(_)
        ));
    }

    #[tokio::test]
    async fn sample_point_passes_coordinates_to_provider_unchanged() {
        let provider =
            FakeElevationProvider::ok(Some(RasterPoint::new(vec![RasterPointBand::new(1, 0.1)])));
        let provider_for_assert = provider.clone();
        let service = ProfileService::new(provider, 500);

        let _ = service.sample_point(34.4, 48.5).await.unwrap();

        let calls = provider_for_assert.calls();
        assert_eq!(calls, vec![(34.4, 48.5)]);
    }
}
