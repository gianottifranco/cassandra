# Read Path Parity

## Scope closed in ISS-1

This slice closes the client-visible single-partition read-path semantics that were still bypassing the Rust coordinator stack in `cassandra-server`.

Delivered in this change:

- native-protocol `page_size` and `paging_state` now flow through `QueryProcessor -> QueryExecutor -> server` for single-partition `SELECT`
- result metadata now sets `HAS_MORE_PAGES` and emits a stable serialized paging cursor
- tombstone scan warnings are surfaced as native-protocol warnings on read responses
- tombstone overwhelming reads now fail through the read-path error mapping instead of being hidden as generic executor failures
- `PER PARTITION LIMIT` is preserved in the planned `SelectPlan` and enforced on the single-partition local path
- resumed pages now respect the remaining global `LIMIT` budget instead of reapplying the full limit on every page
- local reads now project single-column partition/clustering keys back into `SELECT` results, which fixes reversed-order and paging assertions on primary-key-only projections
- index-backed reads keep a guard-rail: paging requests return a warning and a single page until partition-aware cursors are implemented for index/range coordinators

## Java parity notes

Primary Java references for this slice:

- `org.apache.cassandra.service.reads.*`
- `org.apache.cassandra.service.pager.*`
- `org.apache.cassandra.db.filter.DataLimits`
- `org.apache.cassandra.service.StorageProxy`

Rust now mirrors the following observable behaviors on the closed path:

- request `page_size` truncates the current page without losing the continuation position
- the continuation token is deterministic for the same partition/key ordering
- resumed pages preserve both `LIMIT` and `PER PARTITION LIMIT` budgets
- primary-key-only projections (`SELECT pk`, `SELECT ck`) survive paging and reversed ordering on the single-partition local path
- tombstone warning thresholds propagate to the client warning channel
- read-path failures map to protocol-level read errors instead of generic server errors

## Intentional guard-rails still open

These are not hidden. They remain explicit until the full distributed read coordinator is wired into `cassandra-server`:

- range reads and token walks still do not expose stable cross-partition paging cursors from the native server path
- index-backed reads return a warning when paging is requested; the backend still needs partition-aware resume tokens
- speculative retry, digest mismatch repair, and replica fan-out are implemented in `cassandra-coordinator`, but `cassandra-server` still executes local reads directly for query serving
- tracing currently wraps the response correctly, but per-stage read-coordinator trace events are not yet emitted from the local query path

## Validation

Focused coverage added here:

- `cassandra-server::query_processor::tests::select_exposes_paging_state_across_pages`
- `cassandra-server::query_processor::tests::select_respects_global_limit_across_pages`
- `cassandra-server::query_processor::tests::select_respects_reversed_per_partition_limit_before_paging`
- `cassandra-server::query_processor::tests::select_surfaces_tombstone_warnings`
- `cassandra-server::server::tests::encode_query_result_sets_has_more_pages_and_returns_warnings`
- `cassandra-server::error_mapping::tests::{read_timeout_matches_golden_fixture,read_failure_matches_golden_fixture,tombstone_overwhelming_matches_golden_fixture}`
- offline golden fixtures under `rust/diff-tests/golden/read_errors/` pin the visible read-timeout/read-failure/tombstone-failure contract
- `cassandra-coordinator` benchmark `read_path`

Suggested commands:

```bash
cd rust
cargo test -p cassandra-server query_processor::tests::select_exposes_paging_state_across_pages
cargo test -p cassandra-server query_processor::tests::select_respects_global_limit_across_pages
cargo test -p cassandra-server query_processor::tests::select_respects_reversed_per_partition_limit_before_paging
cargo test -p cassandra-server query_processor::tests::select_surfaces_tombstone_warnings
cargo test -p cassandra-server server::tests::encode_query_result_sets_has_more_pages_and_returns_warnings
cargo test -p cassandra-server error_mapping::tests::read_timeout_matches_golden_fixture
cargo test -p cassandra-server error_mapping::tests::read_failure_matches_golden_fixture
cargo test -p cassandra-server error_mapping::tests::tombstone_overwhelming_matches_golden_fixture
cargo bench -p cassandra-coordinator --bench read_path --no-run
```

## Next logical cut

Wire `cassandra-server` reads through `StorageProxy` / `ReadCoordinator` for real replica fan-out and reuse the same paging/warning/error contracts already exercised on the local path. That unlocks digest mismatch handling, speculative retry, read repair, and range-read parity without changing the client-facing response contract introduced in this slice.
