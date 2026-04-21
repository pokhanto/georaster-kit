//! Raster window, raster reading, and raster payload types.

use serde::{Deserialize, Serialize};

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

/// Query describing raster read operation.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterReadQuery {
    /// Placement of window inside source raster.
    placement: WindowPlacement,
    /// Size of source window.
    source_size: RasterSize,
    /// Size of returned target data.
    target_size: RasterSize,
    /// Bands to read
    bands: Vec<usize>,
}

impl RasterReadQuery {
    /// Creates new raster read window.
    pub fn new(
        placement: WindowPlacement,
        source_size: RasterSize,
        target_size: RasterSize,
        bands: Vec<usize>,
    ) -> Self {
        Self {
            placement,
            source_size,
            target_size,
            bands,
        }
    }

    /// Creates point read window.
    pub fn new_point(placement: WindowPlacement, bands: Vec<usize>) -> Self {
        Self {
            placement,
            source_size: RasterSize::point(),
            target_size: RasterSize::point(),
            bands,
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

    /// Returns requested bands.
    pub fn bands(&self) -> &[usize] {
        self.bands.as_ref()
    }
}

/// Errors returned when building raster grid data.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RasterGridError {
    #[error("values length does not match window dimensions")]
    InvalidValuesLength,
}

/// Grid of raster band data for single read operation.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterGrid {
    width: usize,
    height: usize,
    data: Vec<RasterBand>,
}

/// Values for single raster band inside [`RasterGrid`].
#[derive(Debug, Clone, PartialEq)]
pub struct RasterBand {
    band_index: usize,
    data: Vec<f64>,
}

impl RasterGrid {
    /// Creates new raster grid payload.
    ///
    /// Each band payload must contain exactly `target_width * target_height`
    /// values in row-major order.
    pub fn try_new(
        width: usize,
        height: usize,
        bands: impl Into<Vec<RasterBand>>,
    ) -> Result<Self, RasterGridError> {
        let bands = bands.into();
        let expected_len = width * height;

        if bands.iter().any(|band| band.data.len() != expected_len) {
            return Err(RasterGridError::InvalidValuesLength);
        }

        Ok(Self {
            height,
            width,
            data: bands,
        })
    }

    /// Returns all band payloads.
    pub fn bands(&self) -> &[RasterBand] {
        &self.data
    }

    /// Consumes payload and returns inner band data.
    pub fn into_bands(self) -> Vec<RasterBand> {
        self.data
    }

    /// Returns band payload by band index.
    pub fn band(&self, band_index: usize) -> Option<&RasterBand> {
        self.data.iter().find(|band| band.band_index == band_index)
    }

    /// Returns grid height.
    pub fn height(&self) -> usize {
        self.height
    }

    /// Returns grid width.
    pub fn width(&self) -> usize {
        self.width
    }
}

impl RasterBand {
    /// Creates new band payload.
    pub fn new(band_index: usize, data: impl Into<Vec<f64>>) -> Self {
        Self {
            band_index,
            data: data.into(),
        }
    }

    /// Returns band index.
    pub fn band_index(&self) -> usize {
        self.band_index
    }

    /// Returns all band data as slice.
    pub fn data(&self) -> &[f64] {
        &self.data
    }

    /// Consumes payload and returns inner data.
    pub fn into_data(self) -> Vec<f64> {
        self.data
    }
}

/// Values for single raster point across one or more bands.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterPoint {
    data: Vec<RasterPointBand>,
}

/// Value for one raster band at single point.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterPointBand {
    band_index: usize,
    value: f64,
}

impl RasterPoint {
    /// Creates new raster point payload.
    pub fn new(data: impl Into<Vec<RasterPointBand>>) -> Self {
        Self { data: data.into() }
    }

    /// Returns all band values.
    pub fn bands(&self) -> &[RasterPointBand] {
        &self.data
    }

    /// Consumes payload and returns inner band values.
    pub fn into_bands(self) -> Vec<RasterPointBand> {
        self.data
    }

    /// Returns point value for requested band.
    pub fn band(&self, band_index: usize) -> Option<&RasterPointBand> {
        self.data.iter().find(|band| band.band_index == band_index)
    }

    /// Returns true if point has no band values.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns number of bands in point payload.
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl RasterPointBand {
    /// Creates new point value for band.
    pub fn new(band_index: usize, value: f64) -> Self {
        Self { band_index, value }
    }

    /// Returns band index.
    pub fn band_index(&self) -> usize {
        self.band_index
    }

    /// Returns numeric value.
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// Errors returned by raster readers.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RasterReaderError {
    #[error("Failed to resolve path")]
    Path,
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
pub trait RasterReader {
    /// Reads raster window from artifact.
    ///
    /// Returned [`RasterWindowData`] must match requested target window
    /// dimensions and contain values in row-major order.
    ///
    /// Returns error if raster cannot be opened or window cannot be read.
    fn read_window(
        &self,
        locator: &ArtifactLocator,
        raster_read_query: RasterReadQuery,
    ) -> impl Future<Output = Result<RasterGrid, RasterReaderError>> + Send;
}

/// Band selection variants.
pub enum BandSelection {
    First,
    Indexes(Vec<usize>),
    All,
}

/// Colors representation of raster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RasterRepresentation {
    Grayscale,
    Rgb,
    Rgba,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_placement_returns_column_and_row() {
        let placement = WindowPlacement::new(3, 7);

        assert_eq!(placement.column(), 3);
        assert_eq!(placement.row(), 7);
    }

    #[test]
    fn raster_size_returns_width_and_height() {
        let size = RasterSize::new(10, 20);

        assert_eq!(size.width(), 10);
        assert_eq!(size.height(), 20);
    }

    #[test]
    fn raster_size_point_is_one_by_one() {
        let size = RasterSize::point();

        assert_eq!(size.width(), 1);
        assert_eq!(size.height(), 1);
    }

    #[test]
    fn raster_read_query_returns_parts() {
        let placement = WindowPlacement::new(2, 4);
        let source_size = RasterSize::new(5, 6);
        let target_size = RasterSize::new(7, 8);
        let bands = vec![1, 3];

        let query = RasterReadQuery::new(placement, source_size, target_size, bands.clone());

        assert_eq!(query.placement(), placement);
        assert_eq!(query.source_size(), source_size);
        assert_eq!(query.target_size(), target_size);
        assert_eq!(query.bands(), &bands);
    }

    #[test]
    fn raster_read_query_new_point_creates_one_by_one_query() {
        let placement = WindowPlacement::new(9, 11);
        let bands = vec![2];

        let query = RasterReadQuery::new_point(placement, bands.clone());

        assert_eq!(query.placement(), placement);
        assert_eq!(query.source_size(), RasterSize::point());
        assert_eq!(query.target_size(), RasterSize::point());
        assert_eq!(query.bands(), &bands);
    }

    #[test]
    fn raster_band_returns_band_index_and_data() {
        let band = RasterBand::new(3, vec![10.0, 11.0, 12.0]);

        assert_eq!(band.band_index(), 3);
        assert_eq!(band.data(), &[10.0, 11.0, 12.0]);
    }

    #[test]
    fn raster_band_into_data_returns_inner_vector() {
        let band = RasterBand::new(1, vec![42.0]);

        assert_eq!(band.into_data(), vec![42.0]);
    }

    #[test]
    fn raster_grid_try_new_accepts_matching_band_values() {
        let grid = RasterGrid::try_new(
            2,
            3,
            vec![
                RasterBand::new(1, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
                RasterBand::new(2, vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0]),
            ],
        )
        .unwrap();

        assert_eq!(grid.width(), 2);
        assert_eq!(grid.height(), 3);
        assert_eq!(grid.bands().len(), 2);
        assert_eq!(
            grid.band(1).unwrap().data(),
            &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
        assert_eq!(
            grid.band(2).unwrap().data(),
            &[7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        );
    }

    #[test]
    fn raster_grid_try_new_rejects_invalid_values_length() {
        let err =
            RasterGrid::try_new(2, 3, vec![RasterBand::new(1, vec![1.0, 2.0, 3.0])]).unwrap_err();

        assert_eq!(err, RasterGridError::InvalidValuesLength);
    }

    #[test]
    fn raster_grid_returns_bands() {
        let grid = RasterGrid::try_new(
            5,
            6,
            vec![
                RasterBand::new(1, vec![0.0; 30]),
                RasterBand::new(4, vec![1.0; 30]),
            ],
        )
        .unwrap();

        assert_eq!(grid.width(), 5);
        assert_eq!(grid.height(), 6);
        assert_eq!(grid.bands().len(), 2);
        assert_eq!(grid.bands()[0].band_index(), 1);
        assert_eq!(grid.bands()[1].band_index(), 4);
    }

    #[test]
    fn raster_grid_band_returns_matching_band() {
        let grid = RasterGrid::try_new(
            1,
            1,
            vec![
                RasterBand::new(1, vec![10.0]),
                RasterBand::new(2, vec![20.0]),
            ],
        )
        .unwrap();

        assert_eq!(grid.band(1).unwrap().data(), &[10.0]);
        assert_eq!(grid.band(2).unwrap().data(), &[20.0]);
        assert_eq!(grid.band(3), None);
    }

    #[test]
    fn raster_grid_into_bands_returns_inner_band_vector() {
        let grid = RasterGrid::try_new(
            1,
            1,
            vec![
                RasterBand::new(1, vec![10.0]),
                RasterBand::new(2, vec![20.0]),
            ],
        )
        .unwrap();

        let bands = grid.into_bands();

        assert_eq!(bands.len(), 2);
        assert_eq!(bands[0].band_index(), 1);
        assert_eq!(bands[0].data(), &[10.0]);
        assert_eq!(bands[1].band_index(), 2);
        assert_eq!(bands[1].data(), &[20.0]);
    }

    #[test]
    fn band_selection_first_can_be_constructed() {
        let selection = BandSelection::First;
        assert!(matches!(selection, BandSelection::First));
    }

    #[test]
    fn band_selection_indexes_can_be_constructed() {
        let selection = BandSelection::Indexes(vec![1, 2, 4]);

        match selection {
            BandSelection::Indexes(indexes) => assert_eq!(indexes, vec![1, 2, 4]),
            _ => panic!("expected indexes selection"),
        }
    }

    #[test]
    fn band_selection_all_can_be_constructed() {
        let selection = BandSelection::All;
        assert!(matches!(selection, BandSelection::All));
    }

    #[test]
    fn raster_representation_variants_can_be_compared() {
        assert_eq!(
            RasterRepresentation::Grayscale,
            RasterRepresentation::Grayscale
        );
        assert_ne!(RasterRepresentation::Rgb, RasterRepresentation::Rgba);
        assert_ne!(
            RasterRepresentation::Unknown,
            RasterRepresentation::Grayscale
        );
    }
}
