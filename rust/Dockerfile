# Licensed under Apache License, Version 2.0.
# Multi-stage production build for Cassandra Rust.

# ── Stage 1: Builder ──────────────────────────────────────────────
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Cache dependencies by building a dummy project first
COPY Cargo.toml Cargo.lock ./
COPY crates/cassandra-common/Cargo.toml crates/cassandra-common/Cargo.toml
COPY crates/cassandra-config/Cargo.toml crates/cassandra-config/Cargo.toml
COPY crates/cassandra-types/Cargo.toml crates/cassandra-types/Cargo.toml
COPY crates/cassandra-schema/Cargo.toml crates/cassandra-schema/Cargo.toml
COPY crates/cassandra-native-protocol/Cargo.toml crates/cassandra-native-protocol/Cargo.toml
COPY crates/cassandra-cql/Cargo.toml crates/cassandra-cql/Cargo.toml
COPY crates/cassandra-storage/Cargo.toml crates/cassandra-storage/Cargo.toml
COPY crates/cassandra-cluster-metadata/Cargo.toml crates/cassandra-cluster-metadata/Cargo.toml
COPY crates/cassandra-messaging/Cargo.toml crates/cassandra-messaging/Cargo.toml
COPY crates/cassandra-coordinator/Cargo.toml crates/cassandra-coordinator/Cargo.toml
COPY crates/cassandra-repair/Cargo.toml crates/cassandra-repair/Cargo.toml
COPY crates/cassandra-streaming/Cargo.toml crates/cassandra-streaming/Cargo.toml
COPY crates/cassandra-security/Cargo.toml crates/cassandra-security/Cargo.toml
COPY crates/cassandra-admin/Cargo.toml crates/cassandra-admin/Cargo.toml
COPY crates/cassandra-server/Cargo.toml crates/cassandra-server/Cargo.toml
COPY crates/cassandra-tools/Cargo.toml crates/cassandra-tools/Cargo.toml
COPY crates/cassandra-diff-tests/Cargo.toml crates/cassandra-diff-tests/Cargo.toml
COPY xtask/Cargo.toml xtask/Cargo.toml

# Create dummy source files for dependency caching
RUN find crates -name Cargo.toml -exec sh -c \
    'dir=$(dirname {}); mkdir -p "$dir/src" && echo "" > "$dir/src/lib.rs"' \; && \
    mkdir -p crates/cassandra-server/src && echo 'fn main() {}' > crates/cassandra-server/src/main.rs && \
    mkdir -p crates/cassandra-tools/src && echo 'fn main() {}' > crates/cassandra-tools/src/main.rs && \
    mkdir -p xtask/src && echo 'fn main() {}' > xtask/src/main.rs

RUN cargo build --release --workspace 2>/dev/null || true

# Copy real source and build
COPY crates/ crates/
COPY xtask/ xtask/

# Bust the cache for actual source
RUN find crates -name lib.rs -newer Cargo.toml -exec touch {} + 2>/dev/null || true
RUN cargo build --release -p cassandra-server -p cassandra-tools

# ── Stage 2: Runtime ──────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libc6 \
    && rm -rf /var/lib/apt/lists/*

# Create cassandra user
RUN groupadd -r cassandra && useradd -r -g cassandra -d /var/lib/cassandra cassandra

# Standard Cassandra directory layout
RUN mkdir -p /var/lib/cassandra/data \
             /var/lib/cassandra/commitlog \
             /var/lib/cassandra/hints \
             /var/lib/cassandra/saved_caches \
             /var/log/cassandra \
             /etc/cassandra && \
    chown -R cassandra:cassandra /var/lib/cassandra /var/log/cassandra /etc/cassandra

# Copy binaries
COPY --from=builder /build/target/release/cassandra-server /usr/local/bin/cassandra-server
COPY --from=builder /build/target/release/cassandra-tools /usr/local/bin/cassandra-tools

# Default configuration
# TODO: Copy default cassandra.yaml when config crate is ready
# COPY conf/cassandra.yaml /etc/cassandra/cassandra.yaml

EXPOSE 9042 7000 7001 9160 9142

USER cassandra
WORKDIR /var/lib/cassandra

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD cassandra-tools status || exit 1

ENTRYPOINT ["cassandra-server"]
