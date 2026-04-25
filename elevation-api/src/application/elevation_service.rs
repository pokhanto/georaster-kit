use crate::domain::{Coord, CoordWithElevation, Elevation};

const RASTER_BAND_INDEX_FOR_ELEVATION: usize = 1;

/// Errors returned by [`ElevationService`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum ElevationServiceError {
    /// Elevation provider failed to resolve raster value.
    #[error("Elevation lookup failed")]
    Elevation(#[from] super::elevation_provider::ElevationProviderError),
}

/// Application service for resolving elevations for requested coordinates.
#[derive(Debug, Clone)]
pub struct ElevationService<EP> {
    elevation_provider: EP,
}

impl<EP> ElevationService<EP> {
    /// Creates new elevation service.
    pub fn new(elevation_provider: EP) -> Self {
        Self { elevation_provider }
    }
}

impl<EP> ElevationService<EP>
where
    EP: super::elevation_provider::ElevationProvider,
{
    /// Returns elevations for requested coordinates.
    ///
    /// For each coordinate service queries raster provider and tries to extract
    /// elevation from band `1`.
    ///
    /// Returned vector preserves input order.
    ///
    /// If a point cannot be resolved or requested elevation band is missing,
    /// returned item contains `None` elevation.
    pub async fn elevations_at_point(
        &self,
        coordinates: &[Coord],
    ) -> Result<Vec<CoordWithElevation>, ElevationServiceError> {
        let mut coordinates_with_elevations = Vec::with_capacity(coordinates.len());

        for coordinate in coordinates {
            let Coord { lon, lat } = *coordinate;
            let raster_point = self
                .elevation_provider
                .elevation_at_point(lon.0, lat.0)
                .await?;
            let elevation = raster_point
                // TODO: DX is not great here
                .and_then(|rp| {
                    rp.band(RASTER_BAND_INDEX_FOR_ELEVATION)
                        .map(|rb| Elevation(rb.value()))
                });

            coordinates_with_elevations.push(CoordWithElevation {
                lon,
                lat,
                elevation,
            })
        }

        Ok(coordinates_with_elevations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::application::elevation_provider::{ElevationProvider, ElevationProviderError};
    use crate::domain::{Coord, CoordWithElevation, Elevation, Latitude, Longitude};
    use georaster_core::GeorasterServiceError;
    use georaster_domain::{RasterPoint, RasterPointBand};

    type RasterPointResult = Result<Option<RasterPoint>, ElevationProviderError>;
    #[derive(Debug, Clone)]
    struct FakeElevationProvider {
        calls: Arc<Mutex<Vec<(f64, f64)>>>,
        responses: Vec<((f64, f64), RasterPointResult)>,
    }

    impl FakeElevationProvider {
        fn new(responses: Vec<((f64, f64), RasterPointResult)>) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responses,
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

            self.responses
                .iter()
                .find(|((expected_lon, expected_lat), _)| {
                    *expected_lon == lon && *expected_lat == lat
                })
                .map(|(_, result)| result.clone())
                .unwrap_or_else(|| panic!("unexpected elevation lookup for ({lon}, {lat})"))
        }
    }

    fn coord(lat: f64, lon: f64) -> Coord {
        Coord {
            lat: Latitude(lat),
            lon: Longitude(lon),
        }
    }

    #[tokio::test]
    async fn elevations_at_point_returns_empty_when_input_is_empty() {
        let provider = FakeElevationProvider::new(vec![]);
        let service = ElevationService::new(provider.clone());

        let result = service.elevations_at_point(&[]).await.unwrap();

        assert!(result.is_empty());
        assert!(provider.calls().is_empty());
    }

    #[tokio::test]
    async fn elevations_at_point_returns_coordinates_with_elevation() {
        let coordinates = vec![coord(50.4501, 30.5234), coord(50.4510, 30.5240)];

        let provider = FakeElevationProvider::new(vec![
            (
                (30.5234, 50.4501),
                Ok(Some(RasterPoint::new(vec![RasterPointBand::new(1, 123.0)]))),
            ),
            (
                (30.5240, 50.4510),
                Ok(Some(RasterPoint::new(vec![RasterPointBand::new(1, 456.0)]))),
            ),
        ]);
        let service = ElevationService::new(provider.clone());

        let result = service.elevations_at_point(&coordinates).await.unwrap();

        assert_eq!(
            result,
            vec![
                CoordWithElevation {
                    lat: Latitude(50.4501),
                    lon: Longitude(30.5234),
                    elevation: Some(Elevation(123.0)),
                },
                CoordWithElevation {
                    lat: Latitude(50.4510),
                    lon: Longitude(30.5240),
                    elevation: Some(Elevation(456.0)),
                },
            ]
        );

        assert_eq!(
            provider.calls(),
            vec![(30.5234, 50.4501), (30.5240, 50.4510)]
        );
    }

    #[tokio::test]
    async fn elevations_at_point_returns_none_when_provider_returns_none() {
        let coordinates = vec![coord(50.4501, 30.5234)];

        let provider = FakeElevationProvider::new(vec![((30.5234, 50.4501), Ok(None))]);
        let service = ElevationService::new(provider);

        let result = service.elevations_at_point(&coordinates).await.unwrap();

        assert_eq!(
            result,
            vec![CoordWithElevation {
                lat: Latitude(50.4501),
                lon: Longitude(30.5234),
                elevation: None,
            }]
        );
    }

    #[tokio::test]
    async fn elevations_at_point_returns_none_when_requested_band_is_missing() {
        let coordinates = vec![coord(50.4501, 30.5234)];

        let provider = FakeElevationProvider::new(vec![(
            (30.5234, 50.4501),
            Ok(Some(RasterPoint::new(vec![RasterPointBand::new(2, 999.0)]))),
        )]);
        let service = ElevationService::new(provider);

        let result = service.elevations_at_point(&coordinates).await.unwrap();

        assert_eq!(
            result,
            vec![CoordWithElevation {
                lat: Latitude(50.4501),
                lon: Longitude(30.5234),
                elevation: None,
            }]
        );
    }

    #[tokio::test]
    async fn elevations_at_point_propagates_provider_error() {
        let coordinates = vec![coord(50.4501, 30.5234)];

        let provider = FakeElevationProvider::new(vec![(
            (30.5234, 50.4501),
            Err(ElevationProviderError::Elevation(
                GeorasterServiceError::RasterRead,
            )),
        )]);
        let service = ElevationService::new(provider);

        let result = service.elevations_at_point(&coordinates).await;

        assert!(matches!(
            result.unwrap_err(),
            ElevationServiceError::Elevation(ElevationProviderError::Elevation(
                GeorasterServiceError::RasterRead
            ))
        ));
    }
}
