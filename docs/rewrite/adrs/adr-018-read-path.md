# ADR-018: Read Path Architecture

## Status

Accepted

## Context

The read path is the second major coordinator-level feature after the write path.
It must support digest reads, speculative retry, short-read protection, paging,
tombstone tracking, and read repair — all with correct consistency level
enforcement and native protocol error semantics.

The Java implementation spans several classes:
- `StorageProxy.fetchRows()` / `getRangeSlice()`
- `AbstractReadExecutor` and subclasses
- `DigestResolver` / `DataResolver`
- `ShortReadProtection`
- `PagingState`
- `ReadRepairStrategy`

## Decision

### Modular Design

Replace the single `read.rs` stub with a `read/` module directory containing:

| Module | Responsibility |
|--------|---------------|
| `mod.rs` | ReadCoordinator, ReadResult, ReadError, ReadMetrics |
| `command.rs` | ReadCommand hierarchy (SinglePartition, PartitionRange) |
| `response.rs` | ReadResponse, Digest, TombstoneTracker |
| `resolver.rs` | DigestResolver, DataResolver, RepairMutation |
| `executor.rs` | ReadExecutionPlan, ReadExecutorType |
| `speculative_retry.rs` | SpeculativeRetryPolicy |
| `paging.rs` | PagingState serialization |
| `short_read.rs` | ShortReadProtection |
| `repair.rs` | ReadRepairHandler, ReadRepairStrategy |

### Key Design Choices

1. **Digest as fast hash, not MD5**: Java uses MD5 for digests. We use
   `DefaultHasher` (SipHash) which is faster and sufficient for comparison
   purposes. The digest is never persisted or sent cross-version.

2. **Speculative retry policy as enum**: Rather than trait objects, we use
   a simple enum (`None`, `Always`, `Percentile(f64)`, `FixedDelay(Duration)`)
   that can be parsed from CQL strings and serialized.

3. **ReadRepairStrategy simplified**: Java 4.x deprecated `dc_local_read_repair_chance`
   and `read_repair_chance` in favor of a `BLOCKING`/`NONE` strategy enum.
   We implement only the modern strategy but preserve the deprecated fields
   in the handler for schema compatibility.

4. **Tombstone tracking as builder pattern**: `TombstoneTracker` is passed
   mutably through the filtering pipeline, accumulating counts and emitting
   warnings. This avoids global state and makes testing deterministic.

5. **Short-read retries bounded**: Maximum 3 retries to prevent infinite
   loops when a partition is entirely tombstoned.

## Consequences

- Clean separation of concerns enables independent testing of each component
- 74 unit tests cover all major code paths
- Legacy `CoordinatedRead` struct preserved for backward compatibility
- Async messaging integration deferred (TODO in coordinator)
- Real latency percentiles for speculative retry deferred
