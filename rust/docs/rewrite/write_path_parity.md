# Write Path Parity Status

Tracks implementation status of the Cassandra write path in the Rust rewrite
relative to the Java oracle (`org.apache.cassandra.service.StorageProxy`).

## Status Legend

- DONE: Implemented and tested
- PARTIAL: Core logic present, some features missing
- STUB: Interface exists, no real implementation
- TODO: Not started

## Write Coordination

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| Write coordinator | StorageProxy.mutate() | DONE | Sync + async paths |
| DC-aware writes | DatacenterWriteResponseHandler | DONE | LOCAL_QUORUM, EACH_QUORUM |
| Write response handler | AbstractWriteResponseHandler | DONE | Timeout, ack tracking |
| Guardrails | Guardrails | DONE | Mutation size, drift |
| Concurrency control | — | DONE | Semaphore, truncation |

## Consistency Levels

| CL | Status | Notes |
|----|--------|-------|
| ONE | DONE | |
| TWO | DONE | |
| THREE | DONE | |
| QUORUM | DONE | |
| ALL | DONE | |
| ANY | DONE | Hints count toward satisfaction |
| LOCAL_ONE | DONE | DC-aware plan |
| LOCAL_QUORUM | DONE | DC-aware plan |
| EACH_QUORUM | DONE | Per-DC quorum |
| SERIAL | DONE | Routed to Paxos path |
| LOCAL_SERIAL | DONE | Routed to Paxos path |

## Hints

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| In-memory hint store | HintsStore | DONE | Per-endpoint queuing |
| Hint delivery | HintsDispatcher | DONE | Replay to recovered nodes |
| Hint segments (disk) | HintsWriter/Reader | PARTIAL | Segment format, no compression |
| Hint expiration | HintsCatalog | DONE | TTL-based |

## Batch

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| Batch coordinator | StorageProxy.mutateWithTriggers() | DONE | Logged/unlogged/counter |
| Batchlog manager | BatchlogManager | DONE | Store/remove/replay |
| Batchlog replay | BatchlogManager.replayBatchlog() | DONE | Exponential backoff |
| Batch guardrails | Guardrails | DONE | Size, partition count |

## Materialized Views

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| View manager | ViewManager | DONE | Register/unregister/lookup |
| View update generation | ViewUpdateGenerator | DONE | With existing row delta |
| MV fanout in write path | StorageProxy (view scheduling) | DONE | CL=ONE best-effort |
| Read-before-write | SinglePartitionReadCommand | STUB | TODO placeholder |
| Backpressure | ViewManager.getViewUpdateBacklog() | DONE | Threshold=10000 |
| View builder (backfill) | ViewBuilder | TODO | |

## Triggers

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| Trigger definition | TriggerMetadata | DONE | Serde model |
| Trigger trait | ITrigger | DONE | augment() interface |
| Trigger executor | TriggerExecutor | PARTIAL | Behind feature flag, no runtime loading |
| Trigger manager | TriggerExecutor (singleton) | STUB | augment_mutation() returns empty |
| Write-path hook | StorageProxy.mutateWithTriggers() | DONE | cfg(feature = "triggers") |

## Counters

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| Counter coordinator | CounterMutation | DONE | Counter leader routing |
| Counter replica | — | DONE | Local apply |

## Lightweight Transactions (Paxos)

| Component | Java Class | Status | Notes |
|-----------|-----------|--------|-------|
| Paxos coordinator | StorageProxy.cas() | DONE | Prepare/propose/commit |
| Paxos replica | PaxosState | DONE | In-memory state |
| Consensus router | — | DONE | Paxos/Accord selection |

## Error Handling

| Error | Protocol Code | Status | Golden Fixture |
|-------|--------------|--------|----------------|
| WriteTimeout | 0x1100 | DONE | write_timeout.json |
| WriteFailure | 0x1500 | DONE | write_failure.json |
| Unavailable | 0x1000 | DONE | unavailable.json |
| Overloaded | 0x1001 | DONE | overloaded.json |
| IsBootstrapping | 0x1002 | DONE | is_bootstrapping.json |
| MutationTooLarge | 0x2200 | DONE | — |
| SchemaDisagreement | 0x2200 | DONE | — |
