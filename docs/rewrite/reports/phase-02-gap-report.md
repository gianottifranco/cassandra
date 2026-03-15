# Phase 2 – Gap Report: Java Baseline vs Rust Implementation

**Date**: 2026-03-15
**Baseline**: trunk @ `076c6f11` (frozen 2026-03-15)
**Rust workspace**: 18 crates (16 original + `cassandra-diff-tests` + `xtask`)

---

## Summary

This report documents the gap between the Java baseline behavior and the
current Rust implementation for each functional domain. Since Phase 1
produced only stub crates (no logic), **all domains show 0% functional
parity**. The value of this report is establishing the **measurement
baseline** — what we can now automatically compare.

## Gap Matrix

| Domain | Priority | Rust Status | Golden Tests | Live Tests | Gap |
|--------|----------|-------------|-------------|------------|-----|
| Common Utilities | P0 | stub | error codes ✅ | – | 100% |
| Configuration | P0 | stub | – | – | 100% |
| Type System | P0 | stub | serialization reference ✅ | – | 100% |
| Schema | P0 | stub | – | DDL XFAIL | 100% |
| Native Protocol | P0 | stub | frame parsing ✅ | protocol XFAIL | 100% |
| CQL Language | P0 | stub | – | query XFAIL | 100% |
| Storage Engine | P0 | stub | SSTable manifest | – | 100% |
| Cluster Metadata | P1 | stub | topology manifest | – | 100% |
| Inter-node Messaging | P1 | stub | – | – | 100% |
| Coordinator Path | P1 | stub | – | CL XFAIL | 100% |
| Repair & Anti-Entropy | P2 | stub | – | – | 100% |
| Streaming | P2 | stub | – | – | 100% |
| Paxos / LWT | P2 | stub | – | – | 100% |
| Secondary Indexes | P2 | stub | – | – | 100% |
| Hints & Batchlog | P1 | stub | hints manifest | – | 100% |
| Security | P2 | stub | – | – | 100% |
| Observability | P2 | stub | – | – | 100% |
| Admin & Tooling | P2 | stub | – | – | 100% |

### Deferred (no gap tracking)

- Materialized Views (deprecated upstream)
- Accord (trunk-only, fluid API)
- TCM (trunk-only)
- FQL (diagnostic)
- Triggers (plugin system)

## What This Phase Enables

### Automated Measurement Available Now

1. **Error code compliance**: All 20 protocol error codes verified against golden fixture.
2. **Type serialization**: 15 CQL type serialization formulas verified against golden reference.
3. **Protocol frame parsing**: Header parse + body comparison for v4 frames.
4. **Protocol opcodes**: All 16 opcodes enumerated and named.
5. **Tombstone/TTL comparison**: TTL tolerance-based comparison ready.
6. **Query result comparison**: Ordered + unordered comparison available.

### Automated Measurement Pending Rust Logic

| Capability | Blocked On | Expected Phase |
|-----------|-----------|----------------|
| Protocol handshake diff | Protocol codec in `cassandra-native-protocol` | Phase 2-3 |
| CQL query diff | CQL parser in `cassandra-cql` | Phase 3 |
| Schema DDL diff | Schema engine in `cassandra-schema` | Phase 3-4 |
| Read/write path diff | Storage engine in `cassandra-storage` | Phase 4-5 |
| CL enforcement diff | Coordinator in `cassandra-coordinator` | Phase 5-6 |
| Clustering order diff | Storage + coordinator | Phase 5 |
| TTL/tombstone diff | Storage engine | Phase 4 |
| SSTable compatibility | Storage engine | Phase 4 |
| CommitLog replay | Storage engine | Phase 4 |
| Gossip wire compat | Cluster metadata | Phase 6 |
| Repair diff | Repair subsystem | Phase 7 |
| Streaming diff | Streaming subsystem | Phase 7 |

## Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| Golden fixtures go stale | Tests pass incorrectly | Medium | Regeneration in CI, freshness metadata |
| Docker build time too long | Slow CI, developer friction | Medium | Layer caching, pre-built base images |
| Rust stub never replaced | XFAIL tests stay forever | Low | Progress tracked via XFAIL→XPASS count |
| Java oracle has bugs | Rust copies Java bugs | Medium | ADR-006 bug handling protocol |
| Type serialization edge cases | Silent data corruption | High | Exhaustive golden reference + proptest |
