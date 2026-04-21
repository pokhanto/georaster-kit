mod mean;
pub use mean::MeanElevationCalculationStrategy;

use crate::domain::Elevation;

pub trait ElevationCalculationStrategy {
    type State;

    fn key(&self) -> &'static str;

    fn new_state(&self) -> Self::State;

    fn update(&self, state: &mut Self::State, value: Elevation);

    fn finalize(&self, state: Self::State) -> Option<Elevation>;
}
