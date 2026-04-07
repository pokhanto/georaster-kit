FROM lukemathwalker/cargo-chef:latest-rust-1.94-bookworm AS chef
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    lld \
    clang \
    pkg-config \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --bin elevation-tiles-http

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        libgdal32 \
        gdal-bin \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/elevation-tiles-http /app/elevation-tiles-http

EXPOSE 3000

ENTRYPOINT ["./elevation-tiles-http"]
