# ADR-015: Performance Budgets

**Status**: Accepted
**Date**: 2026-03-15
**Deciders**: Principal Engineer

## Context

The Rust implementation must demonstrate competitive performance with Java Cassandra before GA. Without explicit budgets, optimization work lacks direction and regressions go undetected.

## Decision

Establish measurable performance budgets for critical paths, enforced by automated benchmarks in CI.

## Budgets

### Latency Budgets (single-node, no network)

| Operation | p50 | p99 | p999 |
|-----------|-----|-----|------|
| Single-partition read (memtable hit) | < 50μs | < 200μs | < 1ms |
| Single-partition read (SSTable, warm cache) | < 200μs | < 1ms | < 5ms |
| Single-partition read (SSTable, cold cache) | < 2ms | < 10ms | < 50ms |
| Single-row write (CL → memtable) | < 20μs | < 100μs | < 500μs |
| Protocol frame parse (QUERY) | < 1μs | < 5μs | < 20μs |
| Protocol frame encode (RESULT) | < 2μs | < 10μs | < 50μs |
| Bloom filter lookup | < 100ns | < 500ns | < 2μs |

### Throughput Budgets (single-node)

| Workload | Target |
|----------|--------|
| Sequential writes (1KB rows) | > 100K ops/s |
| Sequential reads (memtable) | > 200K ops/s |
| Mixed 50/50 read/write | > 80K ops/s |
| SSTable compaction (STCS merge-2) | > 50 MB/s |

### Resource Budgets

| Metric | Budget |
|--------|--------|
| Steady-state memory (10M rows) | < 2 GB |
| Memory growth over 1hr soak | < 5% |
| SSTable disk amplification (STCS) | < 2× |
| CommitLog disk usage (steady state) | < 2× memtable size |

## Measurement

- **Benchmarks**: Criterion-based, run via `cargo bench`.
- **Soak tests**: 1-hour workloads with memory tracking.
- **CI gate**: Benchmark results compared against budgets; regression alerts if p99 exceeds budget by >20%.

## Consequences

- Budgets may need adjustment after real workload data.
- Some budgets (cold-cache SSTable read) depend on underlying storage hardware.
- Budgets are for single-node; distributed latency is out of scope for Phase 5.

## Related

- [Phase 5 Report](../reports/phase-05-report.md)
