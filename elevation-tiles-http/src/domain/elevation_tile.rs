//! Tile domain model.

use crate::domain::Elevation;

/// Tile with aggregated elevation data.
#[derive(Debug, Clone)]
pub struct ElevationTile {
    id: String,
    elevation: Option<Elevation>,
}

impl ElevationTile {
    /// Creates a new tile.
    pub fn new(id: String, elevation: Option<Elevation>) -> Self {
        Self { id, elevation }
    }

    /// Returns tile id.
    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    /// Returns tile elevation.
    pub fn elevation(&self) -> Option<Elevation> {
        self.elevation
    }
}
