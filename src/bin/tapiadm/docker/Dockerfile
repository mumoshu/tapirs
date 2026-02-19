# Stage 1: Cache dependency compilation
FROM rust:1.93-bookworm AS deps
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin/tapi src/bin/tapictl src/bin/tapiadm/docker && \
    echo 'fn main(){}' > src/bin/tapi/main.rs && \
    echo 'fn main(){}' > src/bin/tapictl/main.rs && \
    echo 'fn main(){}' > src/bin/tapiadm/main.rs && \
    echo '' > src/lib.rs && \
    cargo build --release --no-default-features 2>/dev/null || true

# Stage 2: Build actual binaries
FROM deps AS build
COPY src/ src/
RUN cargo build --release --no-default-features --bin tapi --bin tapictl

# Stage 3: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/tapi /usr/local/bin/tapi
COPY --from=build /app/target/release/tapictl /usr/local/bin/tapictl
ENTRYPOINT ["tapi"]
