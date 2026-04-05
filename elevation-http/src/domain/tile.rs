use elevation_types::Elevation;
use rayon::prelude::*;

#[derive(Debug, Clone)]
pub struct Tile {
    id: String,
    elevation: Option<Elevation>,
}

// TODO: testssts
impl Tile {
    pub fn new_with_mean_elevation(id: String, elevations: Vec<Option<Elevation>>) -> Self {
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

        let elevation = if count == 0 {
            None
        } else {
            Some(Elevation(sum / count as f64))
        };

        Self { id, elevation }
    }

    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    pub fn elevation(&self) -> Option<Elevation> {
        self.elevation
    }
}
