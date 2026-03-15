# Cassandra Rust Rewrite – Project Charter

## 1. Vision

Rewrite the core of Apache Cassandra from Java to Rust, producing a
**binary-compatible, protocol-compatible, and operationally equivalent**
replacement. The goal is *not* a line-by-line translation but a functionally
equivalent system that preserves CQL semantics, native protocol wire format,
on-disk data compatibility, and operational tooling within each phase's scope.

## 2. Baseline

| Field              | Value                                                  |
|--------------------|--------------------------------------------------------|
| **Branch**         | `trunk`                                                |
| **Commit**         | `076c6f11364645bbb43360f013bee6f50a099185`              |
| **Date frozen**    | 2026-03-15                                             |
| **Cassandra ver.** | trunk (post-5.0, pre-6.0)                              |
| **Accord module**  | included (`modules/accord` submodule)                  |

### Trunk-only / Unstable Components (excluded from v1 parity target)

These subsystems exist in trunk but are experimental, incomplete, or are
known to change significantly before a stable release. They should **not**
be used as parity targets for the initial Rust reimplementation.

| Component                     | Reason for exclusion                           |
|-------------------------------|------------------------------------------------|
| `modules/accord`              | New consensus engine, API still fluid          |
| `service/consensus`           | Experimental consensus subsystem               |
| `db/virtual`                  | Virtual tables — auxiliary, low priority        |
| `tcm` (Transactional Cluster Metadata) | Trunk-only, under active iteration    |
| `fql` (Full Query Logging)    | Diagnostic feature, not core data path         |
| `journal`                     | New journaling subsystem, still evolving        |
| `profiler`                    | Dev-time profiling, non-critical               |
| `triggers`                    | Plugin system, rarely used in production       |

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
