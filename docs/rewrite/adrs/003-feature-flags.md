# ADR-003: Feature Flags Strategy

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

The rewrite will proceed incrementally over many months. At any given time,
some subsystems will be fully implemented, some partially, and some not at all.
We need a mechanism to:
- Compile only what's ready.
- Guard incomplete code paths.
- Enable/disable experimental features at runtime.
- Provide clear signals about what's production-ready.

## Decision

We use a **two-layer feature flag system**:

### Layer 1: Compile-time (`Cargo.toml` features)

Each crate exposes Cargo features for major subsystems:

```toml
[features]
default = ["storage-read"]
storage-read = []
storage-write = []
compaction = ["storage-read", "storage-write"]
sai = ["compaction"]  # Storage Attached Indexes
```

**Naming convention**: lowercase-kebab, matching domain IDs from
`feature_matrix.yaml`.

**Rules**:
- `default` includes only what's production-ready.
- All tests compile and pass with `default` features.
- CI also runs `--all-features` to catch bitrot.

### Layer 2: Runtime (`cassandra.yaml` or environment variables)

For subsystems that are compiled but not production-ready:

```yaml
experimental_features:
  rust_storage_write: false
  rust_compaction: false
```

**Rules**:
- Runtime flags default to **off** for experimental features.
- Runtime flags default to **on** for stable features (matching Java behavior).
- All runtime flags are logged at startup.
- Unknown flags produce a warning, not an error (forward compatibility).

### Feature Lifecycle

```
not-started → stub → experimental → stable → default
                                        ↑
                                   gate: diff tests pass
```

## Consequences

- Every PR must declare which features it touches.
- CI matrix grows: `default`, `--all-features`, per-subsystem.
- Clear communication to operators about what's ready.
