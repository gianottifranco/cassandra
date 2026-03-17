# Phase 25 Report: Metrics, Exceptions, Utils, Concurrency Stages, TODO/Stub Cleanup & Gap Guard Reactivation

**Date:** 2026-03-17
**Status:** Complete

## Summary

Phase 25 closes cross-cutting debt: metrics gaps, missing error variants, utility types, observable concurrency stages, TODO cleanup, and gap guard reactivation.

## Work Units Completed

### Units 1-3: Prometheus Metrics Integration

**File:** `cassandra-admin/src/prometheus_metrics.rs`

Added 28 new metrics to MetricsRegistry:

| Category | Metrics | Type |
|----------|---------|------|
| Commitlog | bytes_written, segments_active, pending_tasks | IntCounter, IntGauge×2 |
| Compaction | bytes_compacted_total, tasks_completed_total, pending_tasks | IntCounter×2, IntGauge |
| Streaming | bytes_sent/received, sessions active/completed/failed, retries, checksum_failures | IntGauge×7 |
| Messaging | sent_total, received_total, dropped_total | IntCounter×3 |
| Table-level | read_latency, write_latency (HistogramVec), tombstones_scanned (IntCounterVec) | Labeled by keyspace+table |
| Paxos | propose_total, commit_total, contention_total | IntCounter×3 |
| Tracing | sessions_total, active_sessions | IntCounter, IntGauge |
| TCM | epoch, commits_total | IntGauge, IntCounter |

Sync methods: `sync_from_commitlog_stats()`, `sync_from_compaction_stats()`, `sync_from_streaming_stats()`, `sync_from_messaging_stats()`

**Tests added:** 7 new tests (commitlog, compaction, streaming, messaging, paxos, tracing/tcm, table-level)

### Unit 4: Error Variants

Added 4 new CassandraError variants with correct protocol codes:

| Variant | Code | Wire Detail |
|---------|------|-------------|
| ReadFailure | 0x1300 | consistency, received, block_for, num_failures, data_present |
| FunctionFailure | 0x1400 | keyspace, function, arg_types |
| WriteFailure | 0x1500 | consistency, received, block_for, num_failures, write_type |
| CDCWriteFailure | 0x1600 | None |

**Files modified:** error.rs, error_codes.rs, message.rs, response.rs
**Tests added:** 4 dedicated mapping tests + exhaustive error code test updated

### Unit 5: EstimatedHistogram

**File:** `cassandra-common/src/estimated_histogram.rs` (172 lines)

Log-scale bucket histogram matching Java's `EstimatedHistogram`:
- Lock-free via AtomicI64 (Ordering::Relaxed)
- Default 164 buckets covering 1 to ~2^63
- API: new(), with_default_buckets(), add(), count(), min(), max(), mean(), percentile(), merge(), bucket_offsets()

**Tests:** 5 (empty defaults, single add, percentile, merge, default bucket count)

### Unit 6: Stage Enum with Observable Metrics

**File:** `cassandra-common/src/stages.rs` (207 lines)

- `Stage` enum: 15 variants matching Java's `org.apache.cassandra.concurrent.Stage`
- `StageMetrics`: AtomicU64-based active/pending/completed counters
- `StageRegistry`: per-stage metrics with snapshot_all()

**Tests:** 4 (names, variant count, metrics inc/dec, registry)

### Unit 7: TODO Cleanup

Converted 18 TODOs across 16 files to formalized GAP references:
`// GAP(gap_guard_xxx): description — tracked in gap_guards.rs`

**Before:** 18 scattered TODO comments with inconsistent formatting
**After:** 0 TODOs, 18 GAP references linked to specific gap guards

### Unit 8: Gap Guard Verification — Closed Guards

Added real verification to 3 previously-empty closed guards:
- `gap_guard_trie_index`: verifies `InMemoryTrie` constructibility
- `gap_guard_db_filters`: verifies `ColumnFilter` and `RowFilter` constructibility
- `gap_guard_row_transformations`: verifies `Transformation` trait existence

### Unit 9: Gap Guard Reactivation — Concurrency Stages

Removed `#[ignore]` from `gap_guard_concurrency_stages`. Now verifies:
- Stage::all() returns 15 variants
- StageRegistry tracks metrics correctly
- Active count increments are observable

**Gap guard status:** 4 CLOSED (trie_index, db_filters, row_transformations, concurrency_stages), 17 still #[ignore]d

### Unit 10: This Report

## Test Coverage

| Crate | Tests | Status |
|-------|-------|--------|
| cassandra-common | 16 | All pass |
| cassandra-admin | 14 | All pass |
| cassandra-native-protocol | 24 | All pass |
| cassandra-diff-tests | lib compiles | Pre-existing rollback_tests.rs errors |

**Total new tests:** 20+
**Pre-existing failures:** `no_unsafe_in_phase1` (cassandra-io), rollback_tests.rs type inference errors
