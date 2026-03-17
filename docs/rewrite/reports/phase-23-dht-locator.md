# Phase 23 Report: DHT, Locator, Snitches, Partitioners, Token Allocator, Range Streamer & Transient Replication

## Summary

Phase 23 fills the remaining gaps in the DHT/locator subsystem, adding token allocation, range streaming, cloud snitches, typed replication factors, endpoint collections, replica plans, and completing transient replication support.

## Changes

### cassandra-common

| File | Change |
|------|--------|
| `partitioner.rs` | Added `split()`, `preserves_order()`, `random_token()`, `describe_ownership()` to `Partitioner` trait. Added `TokenFactory` trait + `LongTokenFactory`. Added `LocalPartitioner`. |
| `bounds.rs` | **NEW** — `AbstractBounds<T>` enum with `Range`, `Bounds`, `IncludingExcluding`, `Excluding` variants. Methods: `contains()`, `intersects()`, `unwrap()`, `subtract()`. |
| `lib.rs` | Added `bounds` module and updated re-exports. |

### cassandra-cluster-metadata

| File | Change |
|------|--------|
| `replica_collection.rs` | **NEW** — `ReplicationFactor` (typed, with transient support, `parse("3/1")`), `EndpointsForToken`, `EndpointsForRange`, `RangesAtEndpoint`. |
| `replica_plan.rs` | **NEW** — `ReplicaPlanForRead`, `ReplicaPlanForWrite`, `ReplicaPlanForRangeRead` with sufficiency checks. |
| `token_allocator.rs` | **NEW** — `TokenAllocator` trait, `NoReplicationTokenAllocator` (gap-splitting), `ReplicationAwareTokenAllocator` (topology-aware). |
| `range_streamer.rs` | **NEW** — `RangeStreamer` orchestration, `SourceFilter` trait, `FetchReplica`, source selection with proximity sorting. |
| `cloud_metadata.rs` | **NEW** — Cloud metadata parsing for EC2, GCE, Azure, Alibaba, CloudStack. |
| `snitch.rs` | Added `AzureSnitch`, `AlibabaCloudSnitch`, `CloudstackSnitch`. Added `from_metadata()` constructors for EC2/GCE snitches. Updated factory. |
| `replication.rs` | Added `OldNetworkTopologyStrategy`, `Replica::promote()`. Updated `create_strategy()` to parse `"3/1"` transient format. Improved `TransientReplicationStrategy` with full-before-transient ordering, `full_rf_for_dc()`, `transient_rf_for_dc()`, `calculate_read_endpoints()`. |
| `lib.rs` | Added all new modules and updated re-exports. |

### Integration Tests

| File | Tests |
|------|-------|
| `tests/placement_tests.rs` | Multi-DC NTS rack awareness, token allocation balance, cloud snitch parsing, transient replica placement, range streaming coverage, strategy factory with transient format, partitioner enhancements, OldNTS. |

## Test Results

- **cassandra-common**: 86 tests passing
- **cassandra-cluster-metadata**: 366 tests passing (lib) + 13 integration tests
- All pre-existing tests continue to pass

## Gap Analysis Coverage

| Gap | Status |
|-----|--------|
| IPartitioner full abstraction | Done |
| LocalPartitioner | Done |
| AbstractBounds / Range variants | Done |
| ReplicationFactor typed (full vs transient) | Done |
| EndpointsForToken/Range collections | Done |
| ReplicaPlan / ReplicaPlans | Done |
| Token allocator (vnode-aware) | Done |
| RangeStreamer | Done |
| Ec2Snitch / GoogleCloudSnitch (real) | Done (metadata parsers + from_metadata constructors) |
| AzureSnitch, AlibabaCloudSnitch, CloudstackSnitch | Done |
| Cloud metadata HTTP client | Done (parsing layer; HTTP requires reqwest at runtime) |
| OldNetworkTopologyStrategy | Done |
| Transient replication completion | Done |
