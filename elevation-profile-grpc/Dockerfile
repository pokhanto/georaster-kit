FROM lukemathwalker/cargo-chef:latest-rust-1.94-bookworm AS chef
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    lld \
    clang \
    pkg-config \
    protobuf-compiler \
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
RUN cargo build --release --bin elevation-profile-grpc

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        libgdal32 \
        ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/elevation-profile-grpc /app/elevation-profile-grpc

EXPOSE 50051

ENTRYPOINT ["./elevation-profile-grpc"]
