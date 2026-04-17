# elevation-kit

`elevation-kit` is a toolkit for turning raw elevation rasters into structured, queryable dataset library.

## Motivation

Raw DEM and raster datasets are large, low-level, and inconvenient to use directly in applications. `elevation-kit` provides structured way to prepare them for runtime use and access them through consistent core service.

## Main idea

`elevation-kit` is a composable and extensible workspace.

Its core crates provide shared domain types, elevation query logic, and infrastructure abstractions. Runnable applications (such as `elevation-prepare-cli`, `elevation-tiles-http`, and `elevation-profile-grpc` provided as examples) compose these building blocks into user facing tools and services.

## Core capabilities

`elevation-kit` is centered around two core workflows:

### 1. Dataset preparation

Prepare raw elevation rasters for runtime use by ingesting source data, extracting metadata, and storing metadata and raster artifacts through interchangeable storage abstractions.

For example, an ingest application can be composed with file-based metadata storage and AWS S3 artifact storage:

```rust
// local file system metadata storage
let metadata_dir = PathBuf::from("metadata");
let metadata_registry_name = "registry";
let metadata_storage = FsMetadataStorage::new(metadata_dir, metadata_registry_name);
// AWS S3 artifact storage
let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
let s3_client = Client::new(&aws_config);
let bucket_name = "elevation_geotiffs";
let artifact_storage = S3ArtifactStorage::new(s3_client, bucket_name, None);

let dataset_id = "unique_dataset_id";
let source_raster_path = PathBuf::from("data/file.tif");
let target_crs = Crs::new("EPSG:4326");

ingest(
  dataset_id, 
  source_raster_path, 
  target_crs, 
  metadata_storage, 
  artifact_storage
)?;
```

### 2. Elevation querying

Query prepared datasets through shared core service that depends on interchangeable metadata storage and raster reader abstractions.

For example, runtime service can be composed with file-based metadata storage and S3 raster reader:

```rust
// local file system metadata storage
let metadata_dir = PathBuf::from("metadata");
let metadata_registry_name = "registry";
let metadata_storage = FsMetadataStorage::new(metadata_dir, metadata_registry_name);

// S3 Raster reader using GDAL vsis3
let raster_reader = GdalRasterReader::new(GdalS3ArtifactResolver);

let elevation_service = ElevationService::new(metadata_storage, raster_reader);
let elevation_at_point = elevation_service.elevation_at_point(30.5234, 50.4501)?;
```

## GDAL

Most of `elevation-kit` relies on [GDAL](https://gdal.org/) under the hood for raster access and preprocessing. GDAL is used both through Rust bindings and, in some cases, through command-line tools such as `gdalwarp` and `gdal_translate` to reproject datasets, prepare Cloud Optimized GeoTIFFs, and read raster windows efficiently.

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

### Notes

- Docker examples allows avoid installing GDAL locally.
- Mounted `./data` directory is shared between prepare CLI and HTTP service.
- HTTP service must be configured to use `/data` as its metadata/artifact base directory inside container.

## Main runnable components, provided as examples

- `elevation-prepare-cli` — prepares source datasets and writes metadata
- `elevation-tiles-http` — serves tile-based elevation data over HTTP/SSE
- `elevation-profile-grpc` — serves elevation profiles over gRPC

## TODO

- Add more tests
- Add more benchmarks
- Add discovery mode: app that run in directory or S3 bucket and creates metadata storage based on files
- Add more dataset resolving strategies: only high/low quality
- Consider using primitives from [geo](https://docs.rs/geo/latest/geo/) library
- Replace intersection processing with proper grid merge algorithm in core
- Implement raster reader without GDAL
