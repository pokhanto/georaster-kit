FROM rust:1.86-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release -p elevation-main

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal32 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/elevation-main /usr/local/bin/elevation-main

ENTRYPOINT ["/usr/local/bin/elevation-main"]
CMD []
