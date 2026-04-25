use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Latitude(pub f64);

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Longitude(pub f64);

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Elevation(pub f64);

impl<'de> Deserialize<'de> for Latitude {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        if value.is_finite() && (-90.0..=90.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(serde::de::Error::custom(
                "latitude must be between -90 and 90",
            ))
        }
    }
}

impl<'de> Deserialize<'de> for Longitude {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        if value.is_finite() && (-180.0..=180.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(serde::de::Error::custom(
                "longitude must be between -180 and 180",
            ))
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Coord {
    pub lat: Latitude,
    pub lon: Longitude,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CoordWithElevation {
    pub lat: Latitude,
    pub lon: Longitude,
    pub elevation: Option<Elevation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latitude_deserializes_valid_value() {
        let value: Latitude = serde_json::from_str("50.45").unwrap();

        assert_eq!(value.0, 50.45);
    }

    #[test]
    fn latitude_rejects_value_above_90() {
        let err = serde_json::from_str::<Latitude>("91.0").unwrap_err();

        assert!(
            err.to_string()
                .contains("latitude must be between -90 and 90")
        );
    }

    #[test]
    fn longitude_deserializes_valid_value() {
        let value: Longitude = serde_json::from_str("30.52").unwrap();

        assert_eq!(value.0, 30.52);
    }

    #[test]
    fn longitude_rejects_value_below_minus_180() {
        let err = serde_json::from_str::<Longitude>("-181.0").unwrap_err();

        assert!(
            err.to_string()
                .contains("longitude must be between -180 and 180")
        );
    }

    #[test]
    fn coord_deserializes_valid_values() {
        let coord: Coord = serde_json::from_str(
            r#"{
                "lat": 50.4501,
                "lon": 30.5234
            }"#,
        )
        .unwrap();

        assert_eq!(coord.lat.0, 50.4501);
        assert_eq!(coord.lon.0, 30.5234);
    }

    #[test]
    fn coord_rejects_invalid_latitude() {
        let err = serde_json::from_str::<Coord>(
            r#"{
                "lat": 100.0,
                "lon": 30.5234
            }"#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("latitude must be between -90 and 90")
        );
    }

    #[test]
    fn coord_rejects_invalid_longitude() {
        let err = serde_json::from_str::<Coord>(
            r#"{
                "lat": 50.4501,
                "lon": 200.0
            }"#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("longitude must be between -180 and 180")
        );
    }
}
