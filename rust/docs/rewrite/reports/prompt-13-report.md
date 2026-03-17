# Prompt 13 Report: Write Path Standard (Mutations, CL, Batchlog, Hints, MV, Triggers)

## Summary

Prompt 13 covers the complete write path: standard mutations, consistency level
enforcement, batchlog protocol, hinted handoff, materialized view fanout, and
trigger hooks. Work units WU-01 through WU-22 were implemented across multiple
prompts, with WU-18 through WU-22 completing the final areas (MV/Triggers and
Testing/Docs).

## Work Units Implemented

### AREA 1: Core Write Coordination (WU-01 to WU-05)
- **WU-01**: WriteCoordinator with synchronous and async write paths
- **WU-02**: DC-aware write plans (LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE)
- **WU-03**: WriteResponseHandler for async ack collection with timeout
- **WU-04**: Guardrails (mutation size, timestamp drift, tombstone warnings)
- **WU-05**: Concurrency control (write semaphore, truncation blocking)

### AREA 2: Hints & Batch (WU-06 to WU-10)
- **WU-06**: HintStore with per-endpoint hint storage
- **WU-07**: HintDeliveryService for replaying hints to recovered nodes
- **WU-08**: HintSegmentWriter/Reader for on-disk hint persistence
- **WU-09**: BatchLogManager with store/remove/replay lifecycle
- **WU-10**: BatchCoordinator with logged/unlogged/counter batch support

### AREA 3: Counters & LWT (WU-11 to WU-14)
- **WU-11**: CounterCoordinator with counter leader routing
- **WU-12**: PaxosCoordinator with prepare/propose/commit phases
- **WU-13**: StorageProxy unifying read/write/batch/Paxos paths
- **WU-14**: Verb handlers for internode mutation/read/hint messages

### AREA 4: Advanced Write Path (WU-15 to WU-17)
- **WU-15**: Write metrics and latency tracking
- **WU-16**: ViewFanoutMetrics for MV write tracking
- **WU-17**: ConsensusRouter for Paxos/Accord path selection

### AREA 5: MV/Triggers (WU-18 to WU-19)
- **WU-18**: MV write-path ordering with read-before-write placeholder,
  `generate_view_updates_with_existing()` for delta computation,
  `view_update_backlog` backpressure counter on WriteCoordinator
- **WU-19**: TriggerExecutor struct behind `triggers` feature flag,
  `augment_mutation()` on TriggerManager, trigger hook in
  `coordinate_write_with_hooks()`

### AREA 6: Testing & Documentation (WU-20 to WU-22)
- **WU-20**: Differential write-path tests (CL variations, hint storage,
  batch atomicity logged/unlogged, timeout behavior, failure map)
- **WU-21**: Golden JSON fixtures for write errors (write_timeout,
  write_failure, unavailable, overloaded, is_bootstrapping)
- **WU-22**: This report and write path parity documentation

## Observability Metrics

### Write Metrics (`WriteMetrics`)
- `writes_total`, `writes_succeeded`, `writes_failed`
- `writes_timed_out`, `writes_unavailable`
- `hints_in_flight`, `write_latency_us_sum`

### Hint Metrics (`HintMetrics`)
- `hints_stored`, `hints_delivered`, `hints_failed`
- `hints_expired`, `hints_in_flight`

### Batch Metrics (`BatchLogMetrics`)
- `batches_stored`, `batches_removed`
- `batches_replayed`, `replay_failures`

### MV Fanout Metrics (`ViewFanoutMetrics`)
- `view_mutations_generated`, `view_mutations_applied`
- `view_mutations_failed`
- `view_update_backlog` (backpressure counter, threshold=10000)

## Remaining Gaps

| Gap | Description | Target |
|-----|-------------|--------|
| Read-before-write for MV | Needs local storage engine integration to read existing row state | prompt-14+ |
| Trigger plugin system | TriggerExecutor exists but no WASM/FFI runtime loading | prompt-20+ |
| Async MV fanout | Currently synchronous; Java schedules async | prompt-15+ |
| Hints file persistence | On-disk format for hint segments | prompt-14+ |
| Counter leader election | Stub routing, no real counter leader protocol | prompt-15+ |

## Files Modified/Created

### Modified
- `crates/cassandra-coordinator/src/write.rs` — backpressure, trigger hooks
- `crates/cassandra-storage/src/materialized_views.rs` — `generate_view_updates_with_existing()`
- `crates/cassandra-storage/src/triggers.rs` — TriggerExecutor, augment_mutation()
- `crates/cassandra-diff-tests/Cargo.toml` — dev-deps for coordinator
- `crates/cassandra-diff-tests/src/gap_guards.rs` — write path gap guards

### Created
- `crates/cassandra-diff-tests/tests/write_path_tests.rs`
- `crates/cassandra-diff-tests/tests/write_error_golden_tests.rs`
- `diff-tests/golden/write_errors/write_timeout.json`
- `diff-tests/golden/write_errors/write_failure.json`
- `diff-tests/golden/write_errors/unavailable.json`
- `diff-tests/golden/write_errors/overloaded.json`
- `diff-tests/golden/write_errors/is_bootstrapping.json`
- `docs/rewrite/reports/prompt-13-report.md`
- `docs/rewrite/write_path_parity.md`
