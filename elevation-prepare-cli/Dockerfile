FROM lukemathwalker/cargo-chef:latest-rust-alpine3.23 AS chef
WORKDIR /app

RUN apk add --no-cache \
    build-base \
    musl-dev \
    clang \
    lld \
    pkgconfig \
    gdal-dev \
    gdal \
    gdal-tools

ENV RUSTFLAGS="-C target-feature=-crt-static"

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --bin elevation-prepare-cli

FROM alpine:3.23 AS runtime
WORKDIR /app

RUN apk add --no-cache \
    gdal \
    gdal-tools

COPY --from=builder /app/target/release/elevation-prepare-cli /app/elevation-prepare-cli

ENTRYPOINT ["./elevation-prepare-cli"]
