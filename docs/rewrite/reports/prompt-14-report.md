# Prompt 14 Report

## 1. Summary of changes

- Wired native-protocol query parameters through the active `cassandra-server` read path so `page_size`, `paging_state`, warnings, and read-path errors are preserved on single-partition `SELECT`.
- Preserved `PER PARTITION LIMIT` in `SelectPlan` and enforced it together with resumed global `LIMIT` budgets on paged reads.
- Fixed local read projection for single-column partition/clustering keys so reversed-order paging over `SELECT ck ...` and other primary-key-only projections return concrete values instead of `null`.
- Mapped `ReadError` variants into `CassandraError` so tombstone overwhelming reads surface as protocol read failures rather than generic server failures.
- Added protocol-level response coverage for `HAS_MORE_PAGES`, serialized paging state, and warnings, plus a read-path benchmark target in `cassandra-coordinator`.
- Added offline golden fixtures for `ReadTimeout`, `ReadFailure`, and tombstone-overwhelming error mapping so the client-visible read error contract is pinned independently of live server tests.

## 2. Files created / modified

- Modified `docs/rewrite/read_path_parity.md`
- Added `docs/plans/2026-03-18-read-path-parity.md`
- Added `docs/rewrite/reports/prompt-14-report.md`
- Modified `rust/crates/cassandra-cql/src/planner.rs`
- Modified `rust/crates/cassandra-coordinator/Cargo.toml`
- Added `rust/crates/cassandra-coordinator/benches/read_path.rs`
- Added `rust/diff-tests/golden/read_errors/read_timeout.json`
- Added `rust/diff-tests/golden/read_errors/read_failure.json`
- Added `rust/diff-tests/golden/read_errors/tombstone_overwhelming.json`
- Modified `rust/crates/cassandra-server/src/error_mapping.rs`
- Modified `rust/crates/cassandra-server/src/executor.rs`
- Modified `rust/crates/cassandra-server/src/query_processor.rs`
- Modified `rust/crates/cassandra-server/src/server.rs`

## 3. Tests added / executed

### Added

- `query_processor::tests::select_exposes_paging_state_across_pages`
- `query_processor::tests::select_respects_global_limit_across_pages`
- `query_processor::tests::select_respects_reversed_per_partition_limit_before_paging`
- `query_processor::tests::select_surfaces_tombstone_warnings`
- `query_processor::tests::executor_error_mappings` extended with tombstone read-failure coverage
- `server::tests::encode_query_result_sets_has_more_pages_and_returns_warnings`
- `error_mapping::tests::read_timeout_matches_golden_fixture`
- `error_mapping::tests::read_failure_matches_golden_fixture`
- `error_mapping::tests::tombstone_overwhelming_matches_golden_fixture`
- Criterion benchmark `cassandra-coordinator/benches/read_path.rs`

### Executed

```bash
cd rust
cargo test -p cassandra-server query_processor::tests::
cargo test -p cassandra-server server::tests::encode_query_result_sets_has_more_pages_and_returns_warnings
cargo test -p cassandra-server error_mapping::tests::read_timeout_matches_golden_fixture
cargo test -p cassandra-server error_mapping::tests::read_failure_matches_golden_fixture
cargo test -p cassandra-server error_mapping::tests::tombstone_overwhelming_matches_golden_fixture
cargo bench -p cassandra-coordinator --bench read_path --no-run
```

## 4. Features closed / still open

### Closed in this slice

- Native-server-visible paging state propagation for single-partition reads
- `HAS_MORE_PAGES` metadata emission
- Tombstone warnings on read responses
- Tombstone overwhelming -> protocol read failure mapping
- Stable paging over reversed ordering with `PER PARTITION LIMIT`
- Resumed paging that preserves the remaining global `LIMIT`
- Primary-key projection for single-column `pk` / `ck` on the local read path

### Still open

- `cassandra-server` still serves queries through the local executor path instead of `StorageProxy` / `ReadCoordinator`
- Range reads do not yet expose stable cross-partition paging state from the native server path
- Replica fan-out semantics still sit behind the standalone coordinator modules and are not exercised by native query serving
- Index-backed paging remains guarded with warnings until partition-aware resume tokens exist
- Per-stage tracing from the distributed read coordinator is not yet emitted on the native server path

## 5. Immediate risks

- Composite partition/clustering key projection is still constrained by the current local row representation, which concatenates key components and does not yet rebuild multi-component values for result materialization.
- The native server still bypasses replica coordination, so digest mismatch, speculative retry, and read repair are documented but not yet exercised end-to-end from client queries.
- Index/range reads still need full parity work for resumable paging semantics.

## 6. Technical decisions

- Reused the existing `ReadError -> CassandraError` mapping as the single conversion point and had `QueryProcessor` delegate to it instead of duplicating local error translation logic.
- Kept the serving-path work scoped to `cassandra-server` because that is the active query path today; the distributed coordinator modules remain the next integration cut instead of introducing a second partially-wired serving flow.
- Added response-contract assertions at two levels: query-processor tests for semantic behavior and a server encoder test for protocol flags/warnings.
- Added a coordinator benchmark for the hot local filtering and paging-state serialization helpers before pushing further read-path work into the fan-out path.

## 7. Next logical cut

Route native `SELECT` execution through `StorageProxy` / `ReadCoordinator` so the existing digest reads, speculative retry, short-read protection, and read repair modules become the actual serving path. That should land together with multi-partition/range paging state and differential Java-vs-Rust validations on the native query surface.
