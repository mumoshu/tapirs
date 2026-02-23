# Stage 1: Cache dependency compilation
FROM rust:1.93-bookworm AS deps
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin/tapi src/bin/tapictl src/bin/tapiadm/docker crates/maelstrom/src && \
    echo 'fn main(){}' > src/bin/tapi/main.rs && \
    echo 'fn main(){}' > src/bin/tapictl/main.rs && \
    echo 'fn main(){}' > src/bin/tapiadm/main.rs && \
    echo '' > src/lib.rs && \
    echo 'fn main(){}' > crates/maelstrom/src/main.rs
COPY crates/maelstrom/Cargo.toml crates/maelstrom/Cargo.toml
ARG FEATURES=""
RUN cargo build --release --no-default-features --features="${FEATURES}" 2>/dev/null || true

# Stage 2: Build actual binaries
FROM deps AS build
ARG FEATURES=""
COPY src/ src/
COPY crates/ crates/
# Touch source files to invalidate cargo's mtime cache — BuildKit COPY
# preserves host mtimes, so the deps-stage placeholder binaries may appear
# newer than the real source files, causing cargo to skip recompilation.
RUN find src/ crates/ -name '*.rs' -exec touch {} +
RUN cargo build --release --no-default-features --features="${FEATURES}" --bin tapi --bin tapictl

# Stage 3: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/tapi /usr/local/bin/tapi
COPY --from=build /app/target/release/tapictl /usr/local/bin/tapictl
ENTRYPOINT ["tapi"]
