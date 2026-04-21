# georaster-kit

`georaster-kit` is toolkit for turning raw geospatial rasters into structured, queryable dataset library.

## Motivation

Geospatial raster datasets are often large, low-level, and inconvenient to use directly in applications. `georaster-kit` provides structured way to prepare them for runtime use and access them through consistent core service.

## Main idea

`georaster-kit` is composable and extensible workspace.

Its core crates provide shared domain types, raster query logic, and infrastructure abstractions. Runnable applications, such as `elevation-prepare-cli`, `elevation-tiles-http`, and `elevation-profile-grpc` - are specific example compositions built on top of these core crates.

## Workspace structure

### Core crates

- `georaster-domain` - shared domain types and traits
- `georaster-core` - raster query and ingest logic
- `georaster-adapters` - concrete infrastructure implementations

### Example applications

- `elevation-prepare-cli` - example ingest application for elevation datasets
- `elevation-tiles-http` - example HTTP service for tile-based elevation queries
- `elevation-profile-grpc` - example gRPC service for elevation profile queries

## Core capabilities

`georaster-kit` is centered around two core workflows:

### 1. Dataset preparation

Prepare raw geospatial rasters for runtime use by ingesting source data, extracting metadata, and storing metadata and raster artifacts through interchangeable storage abstractions.

For example ingest application can be composed with file-based metadata storage and AWS S3 artifact storage:

```rust
use std::path::PathBuf;

// Local file system metadata storage
let metadata_dir = PathBuf::from("metadata");
let metadata_registry_name = "registry".to_string();
let metadata_storage = FsMetadataStorage::new(metadata_dir, metadata_registry_name);

// AWS S3 artifact storage
let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
let s3_client = Client::new(&aws_config);
let bucket_name = "georaster-artifacts";
let artifact_storage = S3ArtifactStorage::new(s3_client, bucket_name, None);

// Ingest service
let target_crs = Crs::new("EPSG:4326");
let ingest_service =
    IngestService::new(target_crs, artifact_storage, metadata_storage);

let dataset_id = "unique_dataset_id";
let source_raster_path = PathBuf::from("data/file.tif");

ingest_service
    .run(dataset_id, source_raster_path)
    .await?;
```

### 2. Raster querying

Query prepared datasets through shared core service that depends on interchangeable metadata storage and raster reader abstractions.

For example, runtime service can be composed with file-based metadata storage and S3-backed GDAL raster reader:

```rust
use std::path::PathBuf;
use georaster_domain::{BandSelection, RasterRepresentation};

// Local file system metadata storage
let metadata_dir = PathBuf::from("metadata");
let metadata_registry_name = "registry".to_string();
let metadata_storage = FsMetadataStorage::new(metadata_dir, metadata_registry_name);

// Raster reader using GDAL to read raster on S3
let raster_reader = GdalRasterReader::new(GdalS3ArtifactResolver);

// Core georaster service
let georaster_service = GeorasterService::new(metadata_storage, raster_reader);

let raster_point = georaster_service
    .raster_data_at_point(
        30.5234,
        50.4501,
        BandSelection::First,
        RasterRepresentation::Grayscale,
    )
    .await?;
```

## GDAL

Most of `georaster-kit` relies on [GDAL](https://gdal.org/) under the hood for raster access and preprocessing. GDAL is used both through Rust bindings and, in some cases, through command-line tools such as `gdalwarp` and `gdal_translate` to reproject datasets, prepare Cloud Optimized GeoTIFFs, and read raster windows efficiently.

## Quick start for elevation-tiles-http as example

This example shows full flow:

1. prepare source GeoTIFF with `elevation-prepare-cli`
2. start `elevation-tiles-http`
3. request tiles from HTTP API

### 1. Prepare dataset

Build CLI image:

```bash
docker build -f elevation-prepare-cli/Dockerfile -t elevation-prepare-cli .
```

Run ingest:

```bash
docker run --rm \
  --user "$(id -u)":"$(id -g)" \
  -v "$(pwd)/data_input:/input:ro" \
  -v "$(pwd)/data:/data" \
  elevation-prepare-cli \
  --source-dataset-path /input/sample.tif \
  --dataset-id sampleid \
  --base-dir /data \
  --registry-name registry
```

After this step, output data directory may look like:

```text
data/
├── sampleid.tif
└── registry.json
```

### 2. Start HTTP service

Build HTTP image:

```bash
docker build -f elevation-tiles-http/Dockerfile -t elevation-tiles-http .
```

Run service:

```bash
docker run --rm \
  -p 3000:3000 \
  --env-file elevation-tiles-http/.env \
  -v "$(pwd)/data:/data" \
  elevation-tiles-http
```

Service uses prepared dataset and metadata from `/data`.

### 3. Request tiles

Get one tile by H3 cell id:

```bash
curl "http://127.0.0.1:3000/tiles/8a1e23fffffffff"
```

Stream tiles for bounding box:

```bash
curl -N "http://127.0.0.1:3000/tiles/stream?min_lon=36.20&min_lat=49.96&max_lon=36.30&max_lat=50.02&zoom=10"
```

### Demo UI for elevation-tiles-http

![Elevation Tiles demo](./elevation-tiles-http/demo/demo.gif)

### Notes

- Docker examples allows avoid installing GDAL locally.
- Mounted `./data` directory is shared between prepare CLI and HTTP service.
- HTTP service must be configured to use `/data` as its metadata/artifact base directory inside container.

## Main runnable components, provided as examples

- `elevation-prepare-cli` - prepares source datasets and writes metadata
- `elevation-tiles-http` - serves tile-based elevation data over HTTP/SSE
- `elevation-profile-grpc` - serves elevation profiles over gRPC

## TODO

- Add more tests
- Add more benchmarks
- Add discovery mode: app that run in directory or S3 bucket and creates metadata storage based on files
- Add more dataset resolving strategies: only high/low quality
- Consider using primitives from [geo](https://docs.rs/geo/latest/geo/) library
- Replace intersection processing with proper grid merge algorithm in core
- Implement raster reader without GDAL

## License

Licensed under the MIT License.
