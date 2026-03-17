# ADR-030: Final Validation Approach

**Status**: Accepted
**Date**: 2026-03-17
**Context**: Phase 26 — Final validation before RC gate assessment

## Decision

Adopt a four-tier validation pyramid to assess release readiness for the Cassandra Rust rewrite.

## Validation Pyramid

```
         ┌──────────┐
         │  E2E     │  Tier 4: Cluster (3-node Docker)
         │ Cluster  │  Status: PARTIAL (single-node only)
        ┌┴──────────┴┐
        │  Chaos /   │  Tier 3: Operational resilience
        │  Soak      │  Status: PASS (10 chaos + 6 soak)
       ┌┴────────────┴┐
       │ Integration   │  Tier 2: Cross-crate, storage engine
       │ (diff-tests)  │  Status: PASS (golden, migration, backup)
      ┌┴──────────────┴┐
      │   Unit Tests    │  Tier 1: Per-crate correctness
      │  (3,300+ fns)   │  Status: PASS
      └────────────────┘
```

### Tier 1: Unit Tests

- **Scope**: Every crate in the workspace
- **Proves**: Individual function correctness, type safety, error handling
- **Gate**: `cargo test --workspace` — zero failures
- **Coverage**: 3,300+ test functions across 20 crates

### Tier 2: Integration Tests (cassandra-diff-tests)

- **Scope**: Cross-crate interactions, storage engine lifecycle
- **Proves**: Protocol codec round-trips match Java oracle, storage engine survives write/flush/compact/replay cycles, migration/rollback procedures work
- **Gate**: Golden tests, fuzz tests, migration validation — all pass
- **Key suites**: `golden.rs`, `fuzz.rs`, `chaos_tests.rs`, `backup_restore_tests.rs`, `rollback_tests.rs`, `cdc_tests.rs`

### Tier 3: Chaos & Soak Tests

- **Scope**: Failure injection, long-running stability
- **Proves**: Engine survives crashes, corruption, rapid restarts, disk pressure, concurrent operations
- **Gate**: 10 chaos scenarios + 6 soak scenarios pass
- **Key scenarios**: Crash recovery via commitlog replay, SSTable corruption tolerance, tombstone GC after compaction, large partition stress, rapid restart cycles

### Tier 4: E2E Cluster

- **Scope**: Multi-node deployment, CQL connectivity
- **Proves**: Server starts, accepts CQL connections, admin API responds
- **Current limitation**: Single-node only. Multi-node requires gossip loop and StorageProxy.

## Why Single-Node-Only Is Accepted for Beta

1. **Storage engine is production-quality**: Commitlog, memtable, SSTable, compaction — all battle-tested through chaos/soak.
2. **CQL protocol is complete**: Native protocol v4/v5 codec, STARTUP handshake, query execution for single-node.
3. **Operational tooling exists**: 60+ nodetool commands, admin API, Prometheus metrics.
4. **Migration path is validated**: Snapshot/restore, CDC continuity, rollback drill — all pass.

Single-node beta gives real-world exposure while multi-node is developed.

## What Multi-Node Requires

### Gossip Loop (P0)
- Periodic gossip round (send/receive GossipDigest messages)
- Failure detection (Phi accrual failure detector)
- Cluster membership management (join, leave, decommission)
- Endpoint state propagation

### StorageProxy (P0)
- Coordinator read path: token → replica selection → read from replicas → merge → respond
- Coordinator write path: token → replica selection → write to replicas → await acks
- Consistency level enforcement (ONE, QUORUM, ALL, LOCAL_*)
- Speculative retry, read repair

### Supporting Infrastructure
- Inter-node messaging (already scaffolded in `cassandra-messaging`)
- Token ring awareness (already in `cassandra-cluster-metadata`)
- Hint storage and replay (schema exists, storage not wired)

## Performance Budget Compliance

Per ADR-015, the following budgets are enforced:
- Single mutation write: < 1ms (p99)
- Memtable flush: < 100ms for 64KB
- Commitlog sync: < 10ms
- SSTable read (cached): < 500μs

All budgets pass via `perf_budget_tests`.

## Consequences

- **Beta scope is clear**: Single-node CQL server with full storage engine
- **RC blockers are documented**: Gossip + StorageProxy are explicit prerequisites
- **Evidence is reproducible**: `capture-evidence.sh` generates all gate artifacts
- **No false confidence**: RC gate checklist honestly reports FAIL for multi-node and Java interop
