# Write Path Parity — Rust vs Java

> Status: **Phase 2 Complete** — DC-aware coordination, MV fanout, hint persistence, guardrails.

## Overview

This document tracks the parity status of the Cassandra Rust write path
against the Java baseline (`StorageProxy.mutate()`). The goal is functional
equivalence, not line-by-line translation.

## Architecture Comparison

| Component | Java Class | Rust Module | Status |
|-----------|-----------|-------------|--------|
| Write coordinator | `StorageProxy.performWrite()` | `cassandra-coordinator::write` | ✅ Implemented |
| Response handler | `AbstractWriteResponseHandler` | `cassandra-coordinator::write_response_handler` | ✅ Implemented |
| DC-aware handler | `DatacenterWriteResponseHandler` | `cassandra-coordinator::write::DatacenterWritePlan` | ✅ Phase 2 |
| Consistency levels | `ConsistencyLevel` | `cassandra-coordinator::consistency` | ✅ Complete |
| Batch coordinator | `StorageProxy.mutateWithTriggers()` | `cassandra-coordinator::batch` | ✅ Implemented |
| Batchlog | `BatchlogManager` | `cassandra-coordinator::batch::BatchLogManager` | ✅ Implemented |
| Hint store | `HintsStore` + `HintsService` | `cassandra-coordinator::hints` | ✅ Implemented |
| Hint persistence | `HintsWriter` + `HintsReader` | `cassandra-coordinator::hint_segment` | ✅ Phase 2 |
| Hint dispatcher | `HintsDispatcher` | `cassandra-coordinator::hints::HintedHandoffManager` | ✅ Implemented |
| MV fanout | `ViewManager` + `ViewUpdateGenerator` | `cassandra-storage::materialized_views` | ✅ Implemented |
| MV write hook | `StorageProxy.mutateWithTriggers()` | `write::coordinate_write_with_hooks()` | ✅ Phase 2 |
| Triggers | `TriggerExecutor` | `cassandra-storage::triggers` | ⚠️ Stub (WASM deferred) |
| Write guardrails | `Guardrails` + yaml config | `write::WriteGuardrails` | ✅ Phase 2 |

## Error Code Mapping

| Error | Java Exception | Rust Variant | Protocol Code |
|-------|---------------|--------------|---------------|
| Unavailable | `UnavailableException` | `WriteError::Unavailable` | `0x1000` |
| Timeout | `WriteTimeoutException` | `WriteError::Timeout` | `0x1100` |
| Write Failure | `WriteFailureException` | `WriteError::WriteFailure` | `0x1500` |
| Overloaded | `OverloadedException` | `WriteError::Overloaded` | `0x1001` |
| Bootstrapping | `IsBootstrappingException` | `WriteError::IsBootstrapping` | `0x1002` |
| Truncate race | — | `WriteError::TruncateInProgress` | `0x1003` |
| Schema disagreement | `SchemaDisagreementException` | `WriteError::SchemaDisagreement` | `0x2200` |
| Mutation too large | `MutationExceededMaxSizeException` | `WriteError::MutationTooLarge` | `0x2200` |

## WriteType Mapping

| Java | Rust | Protocol Name |
|------|------|--------------| 
| `SIMPLE` | `WriteType::Simple` | `SIMPLE` |
| `BATCH` | `WriteType::Batch` | `BATCH` |
| `UNLOGGED_BATCH` | `WriteType::UnloggedBatch` | `UNLOGGED_BATCH` |
| `COUNTER` | `WriteType::Counter` | `COUNTER` |
| `VIEW` | `WriteType::View` | `VIEW` |
| `CAS` | `WriteType::Cas` | `CAS` |
| `BATCH_LOG` | `WriteType::BatchLog` | `BATCH_LOG` |

## Consistency Level Behavior

All standard CLs implemented with correct `block_for` semantics:

| CL | block_for | Hints count? | DC-aware? |
|----|----------|-------------|-----------|
| ANY | 1 | ✅ Yes | No |
| ONE | 1 | No | No |
| TWO | 2 | No | No |
| THREE | 3 | No | No |
| QUORUM | ⌊RF/2⌋+1 | No | No |
| ALL | RF | No | No |
| LOCAL_ONE | 1 | No | ✅ Yes |
| LOCAL_QUORUM | ⌊local_RF/2⌋+1 | No | ✅ Yes |
| EACH_QUORUM | per-DC quorum sum | No | ✅ Yes |
| SERIAL | N/A (CAS only) | No | No |
| LOCAL_SERIAL | N/A (CAS only) | No | ✅ Yes |

## DC-Aware Coordination (Phase 2)

- `DatacenterWritePlan`: per-DC replica grouping with `DcReplicaPlan`
- `compute_dc_aware_write_plan()`: groups replicas by DC using node metadata
- `LOCAL_QUORUM`/`LOCAL_ONE`: only considers replicas in coordinator's DC
- `EACH_QUORUM`: each DC must independently reach `⌊dc_rf/2⌋+1`

## Batch Semantics

- **Logged**: batchlog store → execute → remove protocol
- **Unlogged**: direct execution, no batchlog
- **Counter**: validated to contain only counter mutations
- **Guardrails**: warn (5KB default) and fail (50KB default) thresholds
- **Max mutations per batch**: 65535 (configurable)
- **Cross-keyspace detection**: warns for unlogged batches spanning multiple keyspaces
- **Replay**: expired batchlog entries replayed at CL=ANY

## Write Guardrails (Phase 2)

| Guardrail | Default | Action |
|-----------|---------|--------|
| Max mutation size | 16 MiB | Reject with `MutationTooLarge` |
| Timestamp drift threshold | 1 second | Log warning |
| Max batch size (warn) | 5 KiB | Log warning |
| Max batch size (fail) | 50 KiB | Reject |
| Max partition count per batch | 128 | Log warning |
| Max mutations per batch | 65535 | Reject |
| Cross-keyspace unlogged batch | — | Log warning |

## Hint Lifecycle

1. Stored when replica is unreachable during write
2. Configurable max-per-endpoint (default: 100K) and total limits (default: 1M)
3. Expiration window: 3 hours (matching Java default)
4. Oldest hints dropped when per-endpoint limit reached
5. Delivery paused during topology changes
6. Automatic purge of expired hints
7. Deletion on node removal (decommission)

### Hint Persistence (Phase 2)

- Append-only segment files per target: `{target_id}-{timestamp}.hints`
- Format: `[CRC32:4B][length:4B][JSON hint:LEN bytes]` per entry
- Segment rotation at 128 MiB (configurable)
- `HintSegmentWriter` / `HintSegmentReader` / `HintSegmentManager`
- CRC32 validation with skip-on-corruption semantics
- ADR: `docs/rewrite/adrs/adr-013-hint-persistence-format.md`

## Materialized View Fanout (Phase 2)

- View definitions registered via `ViewManager`
- Write-path hook: `coordinate_write_with_hooks()` generates deltas per view
- Column filtering respects `included_columns`
- Delete propagation generates view tombstones
- View mutations applied at CL=ONE (best-effort, non-cascading)
- Failures logged + tracked in `ViewFanoutMetrics` but don't fail base write
- **TODO**: WHERE clause evaluation, view PK recomputation, MV builder (backfill)

## Trigger Hooks (Phase 2)

- `TriggerManager` wired into `coordinate_write_with_hooks()`
- Currently stub — no triggers fire
- Path exercised via test `coordinate_write_with_hooks_trigger_stub`
- **Deferred**: WASM sandbox or Lua scripting engine for actual trigger execution

## Known Gaps

| Gap | Severity | Closure Plan |
|-----|----------|-------------|
| Async messaging integration | Medium | Wire `MessagingService.send()` in write fan-out |
| Trigger execution | Low | Implement WASM sandbox or Lua scripting engine |
| MV WHERE clause filter | Low | Implement CQL expression evaluator |
| MV view PK recomputation | Medium | Extract PK from base columns per view definition |
| MV builder (backfill) | Low | Background task to populate view from existing data |
| Counter write path | Medium | Separate prompt (counter leader election) |
| CAS/LWT write path | Medium | Separate prompt (Paxos integration) |
| Hint recovery from disk on startup | Medium | Read segments on coordinator boot |

## Test Coverage

| Module | Tests | Coverage |
|--------|-------|----------|
| `write.rs` | 28 | CL ONE/TWO/THREE/QUORUM/ALL/LOCAL_ONE/ANY, unavailable, bootstrapping, serial rejection, error codes, write type names, timestamps, TTL, tombstones, estimated size, DC-aware plans (LOCAL_QUORUM/EACH_QUORUM/LOCAL_ONE), mutation too large, schema disagreement, MV fanout hooks, trigger stub |
| `write_response_handler.rs` | 7 | CL satisfaction, quorum acking, write failure, timeout, CL=ANY with hints, failure codes, early exit |
| `consistency.rs` | 9 | block_for all CLs, is_satisfied, protocol codes, serial/local, requires_write, each_quorum multi-DC |
| `batch.rs` | 10 | store/remove, logged/unlogged execution, empty batch, size guardrails, counter validation, metrics, cross-keyspace |
| `hints.rs` | 12 | store/drain, max limits, multiple endpoints, pause/resume, delete for node, disabled, total capacity, metrics, stats, manager lifecycle |
| `hint_segment.rs` | 7 | CRC32, write/read roundtrip, corruption skip, segment rotation, manager list/delete, descriptor filename, disk usage |
| `materialized_views.rs` | 8 | register/unregister, duplicate rejection, insert/delete fanout, column filtering, include_all, no-views case |
| Other (read, paxos, tracing) | 36 | — |
| **Total** | **117** | — |
