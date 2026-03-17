use std::path::Path;

use gdal::Dataset;

pub trait RasterReader {
    fn read_pixel(&self, path: &Path, col: u32, row: u32) -> f64;
}

pub struct GdalRasterReader;

impl RasterReader for GdalRasterReader {
    fn read_pixel(&self, path: &Path, col: u32, row: u32) -> f64 {
        // TODO: preload/cache dataset
        let dataset = Dataset::open(path).unwrap();
        let band = dataset.rasterband(1).unwrap();

        // TODO: reowrk casting
        let buf = band
            .read_as::<f64>((col as isize, row as isize), (1, 1), (1, 1), None)
            .unwrap();

        buf.data()[0]
    }
}
