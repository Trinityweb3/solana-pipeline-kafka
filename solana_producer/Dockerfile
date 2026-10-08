FROM rust:1.83-slim AS builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml ./

# for deps caching
RUN mkdir src && echo "fn main() {}" > src/main.rs \ 
    && cargo build --release \
    && rm -rf src

    
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/solana_producer /usr/local/bin/solana_producer
ENTRYPOINT ["/usr/local/bin/solana_producer"]
