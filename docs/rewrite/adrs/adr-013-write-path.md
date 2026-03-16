# ADR-013: Write Path Design

## Status

**Accepted** — 2026-03-16

## Context

The Cassandra Rust rewrite needs a write path that matches the observable
behavior of Java's `StorageProxy.mutate()`. This includes:

- Consistency level enforcement for all standard CLs
- Logged/unlogged batch semantics with batchlog protocol
- Hinted handoff for temporarily unavailable replicas
- Materialized view fanout on the write path
- Correct error mapping to native protocol codes
- Timestamp/TTL/tombstone semantics

## Decision

### Write Coordinator Architecture

We implement the write path as a synchronous coordinator (with an async
preparation path for future messaging integration):

1. **`WriteCoordinator::coordinate_write()`** — synchronous, simulates
   immediate acks from all live replicas. This is the current path.
2. **`WriteCoordinator::prepare_async_write()`** — returns a `WritePlan`
   and `WriteResponseHandler` for use with real async messaging.

This dual approach allows incremental migration: the synchronous path
works for single-node and testing, while the async path is ready for
multi-node deployment once messaging integration is wired.

### Error Type Design

`WriteError` variants map 1:1 to native protocol error codes:
- `Unavailable` → `0x1000`
- `Timeout` → `0x1100` (includes `WriteType` for client diagnosis)
- `WriteFailure` → `0x1500` (includes per-endpoint failure map)
- `Overloaded` → `0x1001`
- `IsBootstrapping` → `0x1002`

### Batch Protocol

Logged batches follow the 3-phase protocol:
1. Store to batchlog replicas (non-local DC preferred)
2. Execute all mutations
3. Remove batchlog entry

Batchlog replay runs periodically for crash recovery, using CL=ANY
to maximize success probability.

### Hint Lifecycle

Hints are in-memory with configurable limits. Key design choices:
- Per-endpoint queue with oldest-drop on overflow
- Global capacity limit to prevent OOM
- 3-hour default window (matching Java)
- Delivery paused during topology changes
- Automatic purge of expired hints

Disk-based persistence is a documented gap with clear interface boundaries.

### Materialized View Integration

MV fanout is integrated at the write-path level:
- `ViewManager` stores definitions
- `generate_view_updates()` computes delta mutations
- Generated mutations are applied after base table commit

WHERE clause evaluation and view PK recomputation are documented TODOs
with clear interfaces for implementation.

## Consequences

- The synchronous write path works for testing and single-node use
- Async path is ready for multi-node via `WriteResponseHandler`
- Error codes match Java's native protocol exactly
- Batch guardrails prevent abuse (configurable thresholds)
- Hint lifecycle is complete except for disk persistence
- MV fanout works for simple cases; complex WHERE clauses need extension
