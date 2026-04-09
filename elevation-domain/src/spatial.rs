//! Spatial primitives and small geometry-related helpers.

use serde::{Deserialize, Serialize};

/// Coordinate reference system identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Crs(String);

impl Crs {
    // TODO: need some validation for CRS
    /// Creates new CRS value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns placeholder CRS used when source CRS is unknown.
    pub fn unknown() -> Self {
        Self::new("Unknown")
    }
}

impl AsRef<str> for Crs {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Crs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors returned when building bounds.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BoundsCreateError {
    #[error("Invalid bounds constraints")]
    InvalidConstraints,
}

// TODO: add constructor to validate proper bounds
/// Axis-aligned geographic bounds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Bounds {
    /// Minimum longitude.
    min_lon: f64,
    /// Minimum latitude.
    min_lat: f64,
    /// Maximum longitude.
    max_lon: f64,
    /// Maximum latitude.
    max_lat: f64,
}

impl Bounds {
    /// Cretes new Bounds, None if Bounds is not valid
    pub fn new(
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> Result<Self, BoundsCreateError> {
        if min_lon < max_lon && min_lat < max_lat {
            Ok(Self {
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            })
        } else {
            Err(BoundsCreateError::InvalidConstraints)
        }
    }

    pub fn min_lon(&self) -> f64 {
        self.min_lon
    }

    pub fn min_lat(&self) -> f64 {
        self.min_lat
    }

    pub fn max_lon(&self) -> f64 {
        self.max_lon
    }

    pub fn max_lat(&self) -> f64 {
        self.max_lat
    }

    /// Returns intersection of two bounding boxes, if any.
    pub fn intersection(&self, other: &Bounds) -> Option<Bounds> {
        let min_lon = self.min_lon.max(other.min_lon);
        let min_lat = self.min_lat.max(other.min_lat);
        let max_lon = self.max_lon.min(other.max_lon);
        let max_lat = self.max_lat.min(other.max_lat);

        if min_lon < max_lon && min_lat < max_lat {
            Some(Bounds {
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            })
        } else {
            None
        }
    }

    /// Returns `true` if bounds contain provided point.
    pub fn contains_point(&self, lon: f64, lat: f64) -> bool {
        lon >= self.min_lon && lon <= self.max_lon && lat >= self.min_lat && lat <= self.max_lat
    }
}

impl From<Bounds> for geo::Polygon<f64> {
    fn from(value: Bounds) -> Self {
        let exterior = geo::LineString::from(vec![
            (value.min_lon, value.min_lat),
            (value.max_lon, value.min_lat),
            (value.max_lon, value.max_lat),
            (value.min_lon, value.max_lat),
            (value.min_lon, value.min_lat),
        ]);

        geo::Polygon::new(exterior, vec![])
    }
}

impl From<geo::Rect> for Bounds {
    fn from(value: geo::Rect) -> Self {
        Self {
            min_lon: value.min().x,
            min_lat: value.min().y,
            max_lon: value.max().x,
            max_lat: value.max().y,
        }
    }
}

impl From<Bounds> for geo::Rect<f64> {
    fn from(value: Bounds) -> Self {
        geo::Rect::new(
            geo::Coord {
                x: value.min_lon,
                y: value.min_lat,
            },
            geo::Coord {
                x: value.max_lon,
                y: value.max_lat,
            },
        )
    }
}
