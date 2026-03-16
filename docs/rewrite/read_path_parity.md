# Read Path Parity — Rust vs Java

> Status: **Phase 1 Complete** — Modular read coordinator, digest/data resolution, speculative retry, short-read protection, paging state, tombstone tracking, read repair, range reads.

## Overview

This document tracks the parity status of the Cassandra Rust read path
against the Java baseline (`StorageProxy.fetchRows()`, `AbstractReadExecutor`).
The goal is functional equivalence, not line-by-line translation.

## Architecture Comparison

| Component | Java Class | Rust Module | Status |
|-----------|-----------|-------------|--------|
| Read coordinator | `StorageProxy.fetchRows()` | `cassandra-coordinator::read` | ✅ Implemented |
| Read command model | `ReadCommand`, `SinglePartitionReadCommand` | `read::command` | ✅ Implemented |
| Read response | `ReadResponse` | `read::response` | ✅ Implemented |
| Digest resolver | `DigestResolver` | `read::resolver::DigestResolver` | ✅ Implemented |
| Data resolver | `DataResolver` | `read::resolver::DataResolver` | ✅ Implemented |
| Read executors | `AbstractReadExecutor` subclasses | `read::executor` | ✅ Implemented |
| Speculative retry | `SpeculativeRetryPolicy` | `read::speculative_retry` | ✅ Implemented |
| Short-read protection | `ShortReadProtection` | `read::short_read` | ✅ Implemented |
| Paging state | `PagingState` | `read::paging` | ✅ Implemented |
| Tombstone tracking | `TombstoneCounter` | `read::response::TombstoneTracker` | ✅ Implemented |
| Read repair | `BlockingReadRepair`, `ReadRepairStrategy` | `read::repair` | ✅ Implemented |
| Consistency levels | `ConsistencyLevel` | `cassandra-coordinator::consistency` | ✅ Complete |

## Error Code Mapping

| Error | Java Exception | Rust Variant | Protocol Code |
|-------|---------------|--------------|---------------|
| Unavailable | `UnavailableException` | `ReadError::Unavailable` | `0x1000` |
| Timeout | `ReadTimeoutException` | `ReadError::Timeout` | `0x1200` |
| Read Failure | `ReadFailureException` | `ReadError::ReadFailure` | `0x1300` |
| Tombstone Overwhelming | `TombstoneOverwhelmingException` | `ReadError::TombstoneOverwhelming` | `0x1300` |
| Digest Mismatch | — (internal, triggers data read) | `ReadError::DigestMismatch` | `0x1200` |
| Query Cancelled | — | `ReadError::QueryCancelled` | `0x1200` |
| Coordinator Behind | — | `ReadError::CoordinatorBehind` | `0x1200` |

## Read Execution Strategy

| Strategy | Java Class | Rust Type | Trigger |
|----------|-----------|-----------|---------|
| Never speculating | `NeverSpeculatingReadExecutor` | `ReadExecutorType::NeverSpeculating` | `speculative_retry = 'NONE'` |
| Speculating | `SpeculatingReadExecutor` | `ReadExecutorType::Speculating` | `speculative_retry = '99PERCENTILE'` or `'50ms'` |
| Always speculating | `AlwaysSpeculatingReadExecutor` | `ReadExecutorType::AlwaysSpeculating` | `speculative_retry = 'ALWAYS'` |

## Consistency Level Behavior (Read)

All standard CLs implemented with correct `block_for` semantics:

| CL | block_for | Data replicas | Digest replicas |
|----|----------|---------------|-----------------|
| ONE | 1 | 1 | 0 |
| TWO | 2 | 1 | 1 |
| THREE | 3 | 1 | 2 |
| QUORUM | ⌊RF/2⌋+1 | 1 | ⌊RF/2⌋ |
| ALL | RF | 1 | RF-1 |
| LOCAL_ONE | 1 | 1 | 0 |
| LOCAL_QUORUM | ⌊local_RF/2⌋+1 | 1 | ⌊local_RF/2⌋ |
| SERIAL | N/A (CAS only) | — | — |
| LOCAL_SERIAL | N/A (CAS only) | — | — |

## Digest Mismatch Resolution

1. Coordinator sends 1 data + N-1 digest requests
2. DigestResolver compares data digest against all digest responses
3. On mismatch: DataResolver re-reads full data from all replicas
4. Merge reconciliation via timestamp-based LWW
5. Read repair mutations sent to stale replicas
6. Merged result returned to client

## Short-Read Protection

- Detects when a replica returns fewer rows than expected due to tombstones
- Re-queries the replica starting after the last returned clustering key
- Maximum 3 retries to prevent infinite loops
- Transparently integrates with paging

## Paging State

- Serialized as: `[pk_len:4][pk:N][rm_len:4][rm:M][remaining:4][remaining_in_partition:4]`
- Round-trip serialization/deserialization for native protocol
- Tracks per-partition and per-query remaining counts

## Tombstone Guardrails

| Guardrail | Default | Action |
|-----------|---------|--------|
| Tombstone warn threshold | 1,000 | Log warning |
| Tombstone fail threshold | 100,000 | Abort read with `TombstoneOverwhelming` |

## Read Repair

| Strategy | Behavior | Default |
|----------|----------|---------|
| BLOCKING | Repair before returning to client | ✅ Default |
| NONE | No read repair | Per-table config |

## Known Gaps

| Gap | Severity | Closure Plan |
|-----|----------|-------------|
| Async messaging integration | Medium | Wire `MessagingService.send()` in read fan-out |
| Real latency percentiles | Low | Wire read metrics into speculative retry delay |
| Index/SAI read path | Medium | Implement via index executor pattern |
| Range read concurrency limiter | Low | Add configurable concurrency per range query |
| DC-aware read executor | Medium | Add `LOCAL_QUORUM`/`LOCAL_ONE` DC filtering |
| MV read repair filtering | Low | Skip repair mutations that would trigger MV updates |

## Test Coverage

| Module | Tests | Coverage |
|--------|-------|----------|
| `read/mod.rs` | 12 | CL ONE/QUORUM/ALL, unavailable, dead nodes, speculative always, error codes, metrics, range reads |
| `read/command.rs` | 12 | Single partition, range, limits, slices, column filter, display, reversed |
| `read/response.rs` | 8 | Digest determinism, digest difference, tombstone thresholds, data response, partition filtering |
| `read/resolver.rs` | 8 | Digest match/mismatch, data merge by timestamp, repair generation, tombstone wins, multi-row merge |
| `read/executor.rs` | 6 | Never/always/speculating executors, no speculation when not enough replicas, CL=ONE, type from policy |
| `read/speculative_retry.rs` | 10 | Parse NONE/ALWAYS/percentile/fixed, may_speculate, delay computation, display |
| `read/paging.rs` | 7 | State roundtrip, empty row mark, has_more, deserialize too short, page size control |
| `read/short_read.rs` | 6 | Detection with tombstones, fully satisfied, no tombstones, max retries, disabled, bounds |
| `read/repair.rs` | 5 | Strategy parse, blocking stages, none skips, execute clears, is_enabled |
| **Total (read modules)** | **74** | — |
