use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use georaster_core::{GeorasterSampling, GeorasterService};
use georaster_domain::{
    ArtifactLocator, BandSelection, BlockSize, Bounds, Crs, DatasetMetadata, GeoTransform,
    MetadataStorage, MetadataStorageError, RasterBand, RasterBandMetadata, RasterGrid,
    RasterMetadata, RasterReadQuery, RasterReader, RasterReaderError, RasterRepresentation,
};
use tokio::runtime::Runtime;

fn bench_raster_data_in_bbox(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    // bounds to request, changing will affect result
    let bbox = Bounds::try_new(30.0, 50.0, 30.3, 50.3).unwrap();

    // sampling to request, changing will affect result
    let sampling = GeorasterSampling::Resolution {
        x_resolution: 0.0005,
        y_resolution: 0.0005,
    };

    let total_datasets = 60;
    // how many datasets service needs to merge to get raster data in bbox
    let overlapping_datasets_count = [1, 3, 5, 10, 20, 50];

    let mut group = c.benchmark_group("raster_data_in_bbox_overlap");

    for overlapping_datasets in overlapping_datasets_count {
        let datasets = make_datasets(total_datasets, overlapping_datasets, bbox);
        let metadata = InMemoryMetadataStorage::new(datasets);
        let raster = FakeRasterReader;
        let service = GeorasterService::new(metadata, raster);

        group.bench_with_input(
            BenchmarkId::new(
                format!("total_{total_datasets}"),
                format!("overlap_{overlapping_datasets}"),
            ),
            &(total_datasets, overlapping_datasets),
            |b, _| {
                b.iter(|| {
                    let result = runtime.block_on(async {
                        service
                            .raster_data_in_bbox(
                                bbox,
                                Some(sampling),
                                BandSelection::First,
                                RasterRepresentation::Grayscale,
                            )
                            .await
                            .unwrap()
                    });

                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_raster_data_in_bbox);
criterion_main!(benches);

// Fake in-memory implementations

#[derive(Debug, Clone)]
struct InMemoryMetadataStorage {
    datasets: Vec<DatasetMetadata>,
}

impl InMemoryMetadataStorage {
    fn new(datasets: Vec<DatasetMetadata>) -> Self {
        Self { datasets }
    }
}

impl MetadataStorage for InMemoryMetadataStorage {
    async fn save_metadata(&self, _metadata: DatasetMetadata) -> Result<(), MetadataStorageError> {
        Ok(())
    }

    async fn load_metadata(&self) -> Result<Vec<DatasetMetadata>, MetadataStorageError> {
        Ok(self.datasets.clone())
    }
}

#[derive(Debug, Clone, Default)]
struct FakeRasterReader;

impl RasterReader for FakeRasterReader {
    async fn read_window(
        &self,
        locator: &ArtifactLocator,
        raster_query: RasterReadQuery,
    ) -> Result<RasterGrid, RasterReaderError> {
        let fill_value = if locator.as_ref().contains("high") {
            100.0
        } else if locator.as_ref().contains("mid") {
            50.0
        } else {
            10.0
        };

        let target_size = raster_query.target_size();
        let len = target_size.width() * target_size.height();

        let bands = raster_query
            .bands()
            .iter()
            .map(|band_index| RasterBand::new(*band_index, vec![fill_value; len]))
            .collect::<Vec<_>>();

        RasterGrid::try_new(target_size.width(), target_size.height(), bands)
            .map_err(|_| RasterReaderError::Read)
    }
}

fn make_datasets(
    total_datasets: usize,
    overlapping_datasets: usize,
    bbox: Bounds,
) -> Vec<DatasetMetadata> {
    let mut datasets = Vec::with_capacity(total_datasets);

    for i in 0..overlapping_datasets {
        let pixel_size = 0.01 / (i as f64 + 1.0);

        datasets.push(fake_dataset(
            &format!("overlap-{i}"),
            if i == 0 {
                "low"
            } else if i == 1 {
                "mid"
            } else {
                "high"
            },
            bbox,
            pixel_size,
            256,
            256,
        ));
    }

    for i in overlapping_datasets..total_datasets {
        datasets.push(fake_dataset(
            &format!("artifact-{i}"),
            "artifact",
            Bounds::try_new(
                100.0 + i as f64,
                100.0 + i as f64,
                101.0 + i as f64,
                101.0 + i as f64,
            )
            .unwrap(),
            0.01,
            256,
            256,
        ));
    }

    datasets
}

fn fake_dataset(
    dataset_id: &str,
    artifact: &str,
    bounds: Bounds,
    pixel_size: f64,
    width: usize,
    height: usize,
) -> DatasetMetadata {
    DatasetMetadata {
        dataset_id: dataset_id.to_string(),
        artifact_path: ArtifactLocator::new(artifact.to_string()),
        raster: RasterMetadata {
            crs: Crs::new("EPSG:4326"),
            width,
            height,
            geo_transform: GeoTransform {
                origin_lon: bounds.min_lon(),
                origin_lat: bounds.max_lat(),
                pixel_width: pixel_size,
                pixel_height: -pixel_size,
            },
            bounds,
            overview_count: 0,
            raster_representation: RasterRepresentation::Grayscale,
            bands: vec![RasterBandMetadata {
                band_index: 1,
                nodata: None,
                block_size: BlockSize {
                    width: 256,
                    height: 256,
                },
                color_interpretation: "Gray".to_string(),
            }],
        },
    }
}
