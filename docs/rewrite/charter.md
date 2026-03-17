# Cassandra Rust Rewrite – Project Charter

## 1. Vision

Rewrite the core of Apache Cassandra from Java to Rust, producing a
**binary-compatible, protocol-compatible, and operationally equivalent**
replacement. The goal is *not* a line-by-line translation but a functionally
equivalent system that preserves CQL semantics, native protocol wire format,
on-disk data compatibility, and operational tooling within each phase's scope.

## 2. Baseline

### Primary Baseline (Oracle)

| Field              | Value                                                  |
|--------------------|--------------------------------------------------------|
| **Branch**         | `trunk`                                                |
| **Commit**         | `076c6f11364645bbb43360f013bee6f50a099185`              |
| **Date frozen**    | 2026-03-15                                             |
| **Cassandra ver.** | trunk (post-5.0, pre-6.0)                              |
| **Accord module**  | included (`modules/accord` submodule)                  |

### Stable Reference Baseline

| Field              | Value                                                  |
|--------------------|--------------------------------------------------------|
| **Branch**         | `cassandra-5.0`                                        |
| **Tag (pinned)**   | `cassandra-5.0.3`                                      |
| **Purpose**        | Distinguish stable vs trunk-only features              |

### Git Tag

The freeze is tagged as `cassandra-rewrite-baseline-v1` in the Java oracle repo.
Verify with: `bash scripts/verify_baseline.sh`

See [ADR-016](adrs/016-baseline-freeze-policy.md) for re-freeze policy.

### Trunk-only / Unstable Components

Features present only in trunk are classified `trunk-only` in the
[gap matrix](final_gap_matrix.yaml) and gated behind feature flags.
See [ADR-017](adrs/017-experimental-features-policy.md).

| Component                     | Status            | Feature Flag        |
|-------------------------------|--------------------|---------------------|
| `modules/accord`              | experimental       | `experimental`      |
| `service/consensus`           | experimental       | `experimental`      |
| `tcm` (Transactional Cluster) | trunk-only         | `trunk_only`        |
| `journal`                     | trunk-only         | `trunk_only`        |
| `fql` (Full Query Logging)    | missing            | —                   |
| `profiler`                    | baseline-excluded  | —                   |
| `triggers`                    | stub               | `triggers`          |
| `db/virtual`                  | partial            | —                   |

### Mixed-Cluster Posture

Mixed Java+Rust clusters are **NOT feasible** in the current phase.
Gossip wire format and internode messaging are not binary-compatible.
Migration strategy: dual-cluster with shadow traffic validation.
See [ADR-014](adrs/014-migration-strategy.md).

### JMX / Management Plane Strategy

Java Cassandra uses JMX for all admin operations. The Rust implementation
replaces JMX with:
- **HTTP Admin API** (Prometheus-compatible metrics endpoint)
- **CLI tool** (`cassandra-tools`) using HTTP to the admin API
- No JMX dependency; existing JMX-based tools will not work directly.

See [ADR-025](adrs/025-jmx-replacement-strategy.md) for the full JMX replacement strategy.

### Completeness Criteria

A feature is considered "rewrite-complete" when it meets all criteria defined in
[ADR-026](adrs/026-rewrite-completeness-criteria.md). The coverage audit enforces
that every Java package is classified in the gap matrix.

## 3. Scope & Principles

### In scope (Phase 1 – this document)

- Freeze baseline and document commit/branch.
- Classify all Java packages into functional domains.
- Create Rust workspace skeleton (compilable, no logic yet).
- Write foundational ADRs.
- Prepare CI pipeline skeleton.
- Produce feature compatibility matrix.

### In scope (subsequent phases)

1. **Native Protocol** – wire-compatible CQL binary protocol v4/v5.
2. **CQL Parser & Planner** – ANTLR grammar → Rust parser, type system.
3. **Storage Engine** – SSTable read/write, MemTable, CommitLog.
4. **Cluster Metadata** – Gossip, Snitch, Token Ring.
5. **Coordinator Path** – Read/Write coordinators with consistency levels.
6. **Repair & Streaming** – Merkle trees, anti-entropy repair.
7. **Security** – Authentication, authorization, TLS.
8. **Admin & Tooling** – nodetool-compatible CLI, JMX bridge (or replacement).

### Out of scope (deferred indefinitely)

- Thrift protocol support.
- Triggers (Java plugin system).
- MaterializedViews (deprecated upstream).

## 4. Compatibility Guarantees

| Layer                | Guarantee                                              |
|----------------------|--------------------------------------------------------|
| **CQL**              | Byte-identical responses for same queries              |
| **Native Protocol**  | Wire-compatible with existing drivers (v4, v5)         |
| **SSTable format**   | Read-compatible with existing SSTables (write may differ internally but must be cross-readable) |
| **Config**           | `cassandra.yaml` parsed and honored (superset allowed) |
| **Operational**      | `nodetool`-compatible subset; metrics via Prometheus    |
| **Gossip**           | Wire-compatible with Java nodes for mixed clusters     |

## 5. Engineering Standards

- **Incremental delivery** – each phase leaves repo compilable + testable.
- **No premature Java deletion** – Java stays as oracle until Rust parity proven.
- **ADR-documented decisions** – see `docs/rewrite/adrs/`.
- **Feature flags** – `cfg` features guard incomplete subsystems.
- **Differential testing** – golden tests compare Java vs Rust output.
- **`unsafe` budget** – minimized, justified, tested (see ADR-002).

## 6. Repository Layout

```
cassandra/
├── src/java/          # Java source (untouched, oracle)
├── test/              # Java tests (oracle)
├── rust/              # Rust workspace root
│   ├── Cargo.toml     # workspace manifest
│   ├── crates/        # one dir per crate
│   ├── tests/         # workspace-level integration tests
│   ├── Makefile       # build/test shortcuts
│   └── deny.toml      # cargo-deny config
├── docs/rewrite/      # rewrite-specific documentation
│   ├── charter.md
│   ├── feature_matrix.yaml
│   ├── adrs/
│   └── reports/
└── .github/workflows/ # CI (existing + rust-ci.yml)
```

## 7. Phases (High-Level Roadmap)

| Phase | Name                      | Key Deliverable                    |
|-------|---------------------------|------------------------------------|
| 1     | Baseline & Scaffold       | This charter, workspace, ADRs, CI |
| 2     | Types & Protocol Skeleton | Wire types, codec, frame parser   |
| 3     | CQL Parser                | ANTLR-equivalent grammar in Rust  |
| 4     | Storage Engine Core       | SSTable reader, basic MemTable    |
| 5     | Coordinator MVP           | Simple read/write path, 1-node    |
| 6     | Cluster Aware             | Gossip, ring, multi-node          |
| 7     | Production Hardening      | Compaction, repair, streaming     |
| 8     | Operational Parity        | Admin tools, metrics, config      |

## 8. Licensing

This project is part of the Apache Cassandra repository and follows the
Apache License 2.0. All new Rust source files must include the standard
Apache 2.0 header. The NOTICE file must be updated when new dependencies
are added.
