//! Tile aggregation helpers.
//!
//! This module computes aggregated elevation values for tiles.

use elevation_types::Elevation;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::domain::Tile;

/// Builds tiles from raw elevation samples.
pub struct TileAggregator;

impl TileAggregator {
    /// Computes mean elevation from present values, ignoring `None`.
    fn mean_elevation(elevations: &[Option<Elevation>]) -> Option<Elevation> {
        let (sum, count) = elevations
            .into_par_iter()
            .fold(
                || (0.0, 0),
                |(sum, count), elevation| match elevation {
                    Some(elevation) => (sum + elevation.0, count + 1),
                    None => (sum, count),
                },
            )
            .reduce(
                || (0.0, 0),
                |(sum_a, count_a), (sum_b, count_b)| (sum_a + sum_b, count_a + count_b),
            );

        if count == 0 {
            None
        } else {
            Some(Elevation(sum / count as f64))
        }
    }

    /// Builds a tile with mean elevation computed from the provided samples.
    pub fn build_tile_with_mean_elevation(id: String, elevations: Vec<Option<Elevation>>) -> Tile {
        let elevation = Self::mean_elevation(&elevations);
        Tile::new(id, elevation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_elevation_returns_mean_of_present_values() {
        let elevation = TileAggregator::mean_elevation(&[
            Some(Elevation(10.0)),
            Some(Elevation(20.0)),
            Some(Elevation(30.0)),
        ]);

        assert_eq!(elevation, Some(Elevation(20.0)));
    }

    #[test]
    fn mean_elevation_ignores_none_values() {
        let elevation = TileAggregator::mean_elevation(&[
            Some(Elevation(10.0)),
            None,
            Some(Elevation(30.0)),
            None,
        ]);

        assert_eq!(elevation, Some(Elevation(20.0)));
    }

    #[test]
    fn mean_elevation_returns_none_when_all_values_are_none() {
        let elevation = TileAggregator::mean_elevation(&[None, None, None]);

        assert_eq!(elevation, None);
    }

    #[test]
    fn mean_elevation_returns_none_for_empty_input() {
        let elevation = TileAggregator::mean_elevation(&[]);

        assert_eq!(elevation, None);
    }

    #[test]
    fn mean_elevation_returns_same_value_for_single_present_value() {
        let elevation = TileAggregator::mean_elevation(&[Some(Elevation(42.5))]);

        assert_eq!(elevation, Some(Elevation(42.5)));
    }

    #[test]
    fn build_mean_tile_preserves_tile_id() {
        let tile = TileAggregator::build_tile_with_mean_elevation(
            "tile-1".to_string(),
            vec![Some(Elevation(1.0))],
        );

        assert_eq!(tile.id(), "tile-1");
    }

    #[test]
    fn build_mean_tile_sets_computed_mean_elevation() {
        let tile = TileAggregator::build_tile_with_mean_elevation(
            "tile-1".to_string(),
            vec![
                Some(Elevation(10.0)),
                Some(Elevation(20.0)),
                Some(Elevation(30.0)),
            ],
        );

        assert_eq!(tile.elevation(), Some(Elevation(20.0)));
    }

    #[test]
    fn build_mean_tile_sets_none_when_no_elevations_are_present() {
        let tile =
            TileAggregator::build_tile_with_mean_elevation("tile-1".to_string(), vec![None, None]);

        assert_eq!(tile.elevation(), None);
    }
}
