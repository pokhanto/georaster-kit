use crate::{application::elevation_calculation::ElevationCalculationStrategy, domain::Elevation};

#[derive(Debug, Clone, Copy)]
pub struct MeanElevationCalculationStrategy;

impl ElevationCalculationStrategy for MeanElevationCalculationStrategy {
    type State = MeanElevationAccumulator;

    fn key(&self) -> &'static str {
        "mean"
    }

    fn new_state(&self) -> Self::State {
        MeanElevationAccumulator::new()
    }

    fn update(&self, state: &mut Self::State, value: Elevation) {
        state.add(value);
    }

    fn finalize(&self, state: Self::State) -> Option<Elevation> {
        state.mean()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MeanElevationAccumulator {
    sum: f64,
    count: usize,
}

impl MeanElevationAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, value: Elevation) {
        self.sum += value.0;
        self.count += 1;
    }

    pub fn mean(&self) -> Option<Elevation> {
        (self.count > 0).then(|| Elevation(self.sum / self.count as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_accumulator_returns_none_when_empty() {
        let accumulator = MeanElevationAccumulator::new();

        assert_eq!(accumulator.mean(), None);
    }

    #[test]
    fn mean_accumulator_returns_same_value_for_single_input() {
        let mut accumulator = MeanElevationAccumulator::new();
        accumulator.add(Elevation(42.0));

        assert_eq!(accumulator.mean(), Some(Elevation(42.0)));
    }

    #[test]
    fn mean_accumulator_returns_mean_for_multiple_inputs() {
        let mut accumulator = MeanElevationAccumulator::new();
        accumulator.add(Elevation(10.0));
        accumulator.add(Elevation(20.0));
        accumulator.add(Elevation(30.0));

        assert_eq!(accumulator.mean(), Some(Elevation(20.0)));
    }

    #[test]
    fn strategy_finalize_returns_none_for_empty_state() {
        let strategy = MeanElevationCalculationStrategy;
        let state = strategy.new_state();

        assert_eq!(strategy.finalize(state), None);
    }

    #[test]
    fn strategy_update_and_finalize_returns_mean() {
        let strategy = MeanElevationCalculationStrategy;
        let mut state = strategy.new_state();

        strategy.update(&mut state, Elevation(5.0));
        strategy.update(&mut state, Elevation(15.0));
        strategy.update(&mut state, Elevation(25.0));

        assert_eq!(strategy.finalize(state), Some(Elevation(15.0)));
    }
}
