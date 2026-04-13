# elevation-kit

`elevation-kit` is set of tools for preparing elevation datasets and serving elevation data over different transports.

## Main idea
`elevation-kit` is composable and extensible workspace.  
Core crates provide shared domain types, query logic and infrastructure adapters, while runnable applications compose these building blocks into different user-facing tools and services.

```mermaid
flowchart TD
    subgraph Prepare["Preparation"]
        PrepareCli["elevation-prepare-cli"]
    end

    subgraph Shared["Shared workspace crates"]
        Domain["elevation-domain"]
        Core["elevation-core"]
        Adapters["elevation-adapters<br/>storage, raster readers, artifact resolvers"]
    end

    subgraph Runtime["Runtime apps"]
        Http["elevation-tiles-http"]
        Grpc["elevation-profile-grpc"]
    end

    PrepareCli --> Domain
    PrepareCli --> Adapters

    Http --> Domain
    Http --> Core
    Http --> Adapters

    Grpc --> Domain
    Grpc --> Core
    Grpc --> Adapters
```

## GDAL
Most of `elevation-kit` relies on [GDAL](https://gdal.org/) under the hood for raster access and preprocessing. GDAL is used both through Rust bindings and, in some cases, through command-line tools such as `gdalwarp` and `gdal_translate` to reproject datasets, prepare Cloud Optimized GeoTIFFs, and read raster windows efficiently.

