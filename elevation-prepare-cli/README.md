# elevation-prepare-cli

CLI tool for preparing elevation datasets for use in `elevation-kit`.

It takes source GeoTIFF, optionally reprojects it, converts it to a Cloud Optimized GeoTIFF (COG) when needed, stores artifact, and writes dataset metadata.

## What it does

- reads source raster dataset
- reprojects it to target CRS when needed
- converts it to COG when needed
- stores prepared artifact
- writes metadata registry entry

## Requirements

- GDAL must be available when running locally
- or use provided Docker image

## Usage

```bash
elevation-prepare-cli \
  --source-dataset-path /path/to/source.tif \
  --dataset-id datasetid \
  --base-dir /path/to/data \
  --registry-name registry
```

## Docker
Build

```
docker build -f elevation-prepare-cli/Dockerfile -t elevation-prepare-cli .
```

Run
```
docker run --rm \
  -v "$(pwd)/data_input:/input:ro" \
  -v "$(pwd)/data:/data" \
  elevation-prepare-cli \
  --source-dataset-path /input/sample.tif \
  --dataset-id my-dataset \
  --base-dir /data \
  --registry-name registry
```
