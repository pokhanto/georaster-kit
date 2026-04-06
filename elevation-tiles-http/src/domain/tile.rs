//! Tile domain model.

use elevation_types::Elevation;

/// Tile with aggregated elevation data.
#[derive(Debug, Clone)]
pub struct Tile {
    id: String,
    elevation: Option<Elevation>,
}

impl Tile {
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
