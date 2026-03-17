use elevation_types::{DatasetMetadata, MetadataStorage};

use crate::raster_reader::RasterReader;

pub mod raster_reader;

// TODO: rename
pub struct PixelPlace {
    pub col: u32,
    pub row: u32,
}

// TODO: support more crs, add conversion
fn world_to_pixel(metadata: &DatasetMetadata, x: f64, y: f64) -> Option<PixelPlace> {
    let gt = &metadata.raster.geo_transform;

    let col = ((x - gt.origin_x) / gt.pixel_width).floor();
    let row = ((y - gt.origin_y) / gt.pixel_height).floor();

    if !col.is_finite() || !row.is_finite() {
        return None;
    }

    let col = col as i64;
    let row = row as i64;

    if col < 0 || row < 0 {
        return None;
    }

    let col = col as u32;
    let row = row as u32;

    if col >= metadata.raster.width || row >= metadata.raster.height {
        return None;
    }

    Some(PixelPlace { col, row })
}

pub struct ElevationService<M, R> {
    metadata: M,
    raster: R,
}

impl<M, R> ElevationService<M, R> {
    pub fn new(metadata: M, raster: R) -> Self {
        Self { metadata, raster }
    }
}

impl<M, R> ElevationService<M, R>
where
    M: MetadataStorage,
    R: RasterReader,
{
    pub fn elevation_at(&self, x: f64, y: f64) -> Option<f64> {
        let dataset = self.metadata.load_metadata();

        // if !contains(&dataset, request.x, request.y) {
        //     return Ok(None);
        // }

        let pixel = world_to_pixel(&dataset, x, y).unwrap();

        let value = self
            .raster
            .read_pixel(&dataset.artifact_path, pixel.col, pixel.row);

        // TODO: check nodata

        Some(value)
    }
}
