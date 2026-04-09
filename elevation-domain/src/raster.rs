//! Raster window, raster reading, and raster payload types.

use crate::storage::ArtifactLocator;

/// Position of window inside raster.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindowPlacement {
    column: usize,
    row: usize,
}

impl WindowPlacement {
    /// Creates new placement.
    pub fn new(column: usize, row: usize) -> Self {
        Self { column, row }
    }

    /// Returns starting column.
    pub fn column(&self) -> usize {
        self.column
    }

    /// Returns starting row.
    pub fn row(&self) -> usize {
        self.row
    }
}

/// Raster size in pixels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RasterSize {
    width: usize,
    height: usize,
}

impl RasterSize {
    /// Creates new size.
    pub fn new(width: usize, height: usize) -> Self {
        Self { width, height }
    }

    /// Returns size of single pixel.
    pub fn point() -> Self {
        Self {
            width: 1,
            height: 1,
        }
    }

    /// Returns width.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns height.
    pub fn height(&self) -> usize {
        self.height
    }
}

/// Window describing raster read operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RasterReadWindow {
    /// Placement of window inside source raster.
    placement: WindowPlacement,
    /// Size of source window.
    source_size: RasterSize,
    /// Size of returned target data.
    target_size: RasterSize,
}

impl RasterReadWindow {
    /// Creates new raster read window.
    pub fn new(
        placement: WindowPlacement,
        source_size: RasterSize,
        target_size: RasterSize,
    ) -> Self {
        Self {
            placement,
            source_size,
            target_size,
        }
    }

    /// Creates point read window.
    pub fn new_point(placement: WindowPlacement) -> Self {
        Self {
            placement,
            source_size: RasterSize::point(),
            target_size: RasterSize::point(),
        }
    }

    /// Returns placement of window.
    pub fn placement(&self) -> WindowPlacement {
        self.placement
    }

    /// Returns source size of window.
    pub fn source_size(&self) -> RasterSize {
        self.source_size
    }

    /// Returns target size of window.
    pub fn target_size(&self) -> RasterSize {
        self.target_size
    }
}

/// Errors returned when building raster window data.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RasterWindowDataError {
    #[error("values length does not match window dimensions")]
    InvalidValuesLength,
}

/// Raster values returned for window.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterWindowData<T> {
    window: RasterReadWindow,
    values: Vec<T>,
}

impl<T> RasterWindowData<T> {
    /// Creates new raster window payload.
    pub fn try_new(
        window: RasterReadWindow,
        values: impl Into<Vec<T>>,
    ) -> Result<Self, RasterWindowDataError> {
        let values = values.into();
        let target_size = window.target_size.width * window.target_size.height;

        if values.len() != target_size {
            return Err(RasterWindowDataError::InvalidValuesLength);
        }

        Ok(Self { window, values })
    }

    /// Returns all values as slice.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Consumes payload and returns inner values.
    pub fn into_values(self) -> Vec<T> {
        self.values
    }

    /// Returns value by target column and row.
    pub fn get(&self, col: usize, row: usize) -> Option<&T> {
        if col >= self.window.target_size.width || row >= self.window.target_size.height {
            return None;
        }

        self.values.get(row * self.window.target_size.width + col)
    }

    /// Returns target height.
    pub fn target_height(&self) -> usize {
        self.window.target_size.height
    }

    /// Returns target width.
    pub fn target_width(&self) -> usize {
        self.window.target_size.width
    }
}

/// Hint used to select an output raster resolution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResolutionHint {
    /// Use highest available resolution.
    Highest,
    /// Use lowest available resolution.
    Lowest,
    /// Use explicit target resolution in degrees.
    Degrees {
        /// Target longitudinal resolution.
        lon_resolution: f64,
        /// Target latitudinal resolution.
        lat_resolution: f64,
    },
}

/// Errors returned by raster readers.
#[derive(Debug, thiserror::Error)]
pub enum RasterReaderError {
    #[error("Failed to open raster")]
    Open,
    #[error("Failed to read raster pixel")]
    Read,
}

/// Reads raster data from stored artifact.
///
/// Implementations are responsible for opening raster artifact identified by
/// an [`ArtifactLocator`] and returning data for requested read window.
/// This trait is used by higher-level services to fetch raster samples without
/// depending on specific raster library or file format implementation.
///
/// Generic parameter `T` represents value type returned from raster.
pub trait RasterReader<T> {
    /// Reads raster window from artifact.
    ///
    /// Returned [`RasterWindowData`] must match requested target window
    /// dimensions and contain values in row-major order.
    ///
    /// Returns error if raster cannot be opened or window cannot be read.
    fn read_window(
        &self,
        locator: &ArtifactLocator,
        raster_window: RasterReadWindow,
    ) -> impl Future<Output = Result<RasterWindowData<T>, RasterReaderError>> + Send;
}
