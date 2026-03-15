# Phase 3 – Advanced Features Report

**Date**: 2026-03-15
**Status**: Complete

## Summary

Implemented advanced features for the Cassandra Rust rewrite:
- Paxos/LWT (Lightweight Transactions)
- Counter context (distributed CRDT)
- Secondary Indexes (legacy + SAI)
- Vector type with similarity functions
- Feature-flag stubs for deferred subsystems (MV, triggers, UDF/UDA)

## Changes by Subsystem

### Paxos / LWT (`cassandra-coordinator/src/paxos/`)

| File | Description | Tests |
|------|-------------|-------|
| `ballot.rs` | Timeuuid-based ballot with total ordering | 6 |
| `state.rs` | Per-partition prepare/propose/commit state machine | 7 |
| `messages.rs` | Inter-node Paxos message types (serde) | 3 |
| `coordinator.rs` | CAS coordinator with contention retry | 5 |
| `mod.rs` | Module root with re-exports | — |

**Key decisions**: ADR-008 (Paxos design)

### Counter Context (`cassandra-storage/src/counter.rs`)

- Shard-vector CRDT: `(node_id, clock, count)` per contributing node
- CRDT merge: commutative, associative, idempotent (13 tests)
- Binary format compatible with Java's `CounterContext`
- **Key decisions**: ADR-009 (Counter CRDT)

### Secondary Indexes (`cassandra-storage/src/index/`)

| Module | Description | Tests |
|--------|-------------|-------|
| `mod.rs` | `SecondaryIndex` trait + `IndexManager` | 2 |
| `legacy.rs` | BTreeMap inverted index | 7 |
| `sai/mod.rs` | SAI with in-memory posting lists | 5 |
| `sai/posting.rs` | Sorted posting list (merge, intersect) | 6 |
| `sai/query.rs` | Query execution (Eq, Range, AND/OR) | 3 |
| `sai/builder.rs` | Segment builder + merger for compaction | 3 |

**Key decisions**: ADR-010 (SAI architecture)

### Vector Type (`cassandra-types/src/vector.rs`)

- `VectorValue`: fixed-dimension f32 array
- Similarity functions: cosine, euclidean, dot product
- Serialization: big-endian IEEE 754 (Java-compatible)
- `CqlType::Vector(inner, dims)` variant added to type system
- 11 tests

### Native Protocol (`cassandra-native-protocol/src/message.rs`)

- Added `ColumnType::Vector(Box<ColumnType>, u32)` variant
- Updated `from_cql_type` for exhaustive matching

### Feature-Flag Stubs

| Module | Feature Flag | Status |
|--------|-------------|--------|
| `materialized_views.rs` | `materialized-views` | Stub — upstream deprecated |
| `triggers.rs` | `triggers` | Stub — needs WASM/FFI sandbox |
| `udf.rs` | `udfs` | Stub — needs WASM sandbox |

## Test Results

- **Total tests**: 605 passed, 0 failed
- **Feature-flag build**: compiles with all flags enabled
- **Pre-existing warnings**: 12 (from other crates, unchanged)

## Known Gaps

1. Paxos state is in-memory only (not persisted to system.paxos table)
2. SAI segments are in-memory only (not persisted to disk)
3. Vector search is brute-force (no ANN/HNSW)
4. Counter tombstones not implemented
5. SAI not wired to compaction lifecycle hooks
6. No exponential backoff in Paxos contention retry

## ADRs Created

- `docs/rewrite/adrs/008-paxos-design.md`
- `docs/rewrite/adrs/009-counter-crdt.md`
- `docs/rewrite/adrs/010-sai-architecture.md`
