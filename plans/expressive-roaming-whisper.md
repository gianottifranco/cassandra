# Plan: QueryProcessor, ClientState/QueryState, QueryOptions, ResultSet — Phase 02

## Context

This is Phase 02 of the Cassandra Java→Rust rewrite series. The goal is to close the gap between the native protocol server and actual query execution. Currently:
- CQL parser, planner, and basic executor exist
- Native protocol frame codec handles QUERY messages
- Prepared statement cache exists but is NOT wired to protocol (PREPARE/EXECUTE messages unhandled)
- SELECT results return Void stub instead of actual rows (server.rs line ~271)
- No ClientState/QueryState/QueryOptions abstractions exist
- No ResultSet/UntypedResultSet types

### Gap Analysis Items Being Closed
From `final_gap_matrix.md`:
- **Prepared Statement Cache** (partial → functional): Wire to protocol layer
- **CQL Native Protocol v4/v5** (stub → partial): Handle PREPARE/EXECUTE/BATCH messages, return SELECT rows
- **Paging** (missing → partial): Paging state in results
- **Query Monitoring** (missing → stub): Basic query state tracking

## Work Units

### Unit 1: CQL Result Types (cassandra-cql)
**Files:** `rust/crates/cassandra-cql/src/result_set.rs` (NEW), `rust/crates/cassandra-cql/src/untyped_result_set.rs` (NEW), `rust/crates/cassandra-cql/src/bind.rs` (NEW), modify `rust/crates/cassandra-cql/src/lib.rs`

**Description:** Implement ResultSet (column metadata + typed rows + paging state + warnings), UntypedResultSet (typed accessor Row wrapper for internal system queries), and bind parameter substitution (replace BindMarker nodes in AST with literal values). ResultSet provides `to_rows_result()` to convert to wire format `RowsResult`. UntypedResultSet.Row provides `get_string()`, `get_int()`, `get_boolean()`, `get_uuid()`, `get_bytes()`. Bind module provides `bind_values(statement, values) -> Result<Statement>`.

**Why independent:** All new files in cassandra-cql. Only adds 3 `pub mod` lines to lib.rs. No other unit modifies this crate.

**Complexity:** M

---

### Unit 2: Error Mapping Improvements (cassandra-native-protocol)
**Files:** Modify `rust/crates/cassandra-native-protocol/src/error_codes.rs`, modify `rust/crates/cassandra-common/src/error.rs`

**Description:** Ensure all Java error categories map correctly to protocol error codes. Add missing variants: ReadFailure (0x1300), WriteFailure (0x1500), FunctionFailure (0x1400), CDCWriteFailure (0x1600). Ensure Unprepared error includes statement ID bytes. Add `From<ExecutorError>` impl for CassandraError if not present. Ensure error detail structs (UnavailableDetail, TimeoutDetail) are complete.

**Why independent:** Only modifies cassandra-native-protocol and cassandra-common. No other unit touches these files.

**Complexity:** S

---

### Unit 3: Query Context + Processor + Server Integration (cassandra-server)
**Files:** `rust/crates/cassandra-server/src/client_state.rs` (NEW), `rust/crates/cassandra-server/src/query_state.rs` (NEW), `rust/crates/cassandra-server/src/query_options.rs` (NEW), `rust/crates/cassandra-server/src/query_processor.rs` (NEW), modify `rust/crates/cassandra-server/src/main.rs`, modify `rust/crates/cassandra-server/src/server.rs`, modify `rust/crates/cassandra-server/src/executor.rs`

**Description:** The core unit. Implements:
1. **ClientState**: authenticated user, keyspace, driver info, client address, monotonic timestamp generator (AtomicI64 CAS), `set_keyspace()`, `ensure_logged_in()`, `for_internal_calls()`, `for_external_calls()`
2. **QueryState**: wraps ClientState ref + lazy per-query timestamp/now_in_seconds
3. **QueryOptions**: wraps protocol QueryParams, provides `consistency()`, `values()`, `page_size()`, `paging_state()`, `serial_consistency()`, `timestamp()`, `skip_metadata()`
4. **QueryProcessor**: owns PreparedCache + references executor and schema. Methods: `process_query()` (parse→plan→execute→format), `process_prepare()` (parse→cache→return prepared metadata), `process_execute()` (lookup→bind→plan→execute→format), `process_batch()`, `execute_internal()` (for system queries)
5. **Server integration**: Update `handle_query()` in server.rs to create ClientState/QueryState from ConnectionContext, dispatch QUERY/PREPARE/EXECUTE/BATCH to QueryProcessor, convert QueryResult::Rows to ResultMessage::Rows (add `result_to_message()` in executor.rs)
6. **Prepared statement wiring**: PREPARE message → parse + cache + return statement ID + bind metadata + result metadata. EXECUTE message → lookup by ID + bind values + execute + return rows.

**Why independent:** All new files in cassandra-server plus modifications to server.rs/executor.rs/main.rs. No other unit touches these files.

**Complexity:** XL

---

### Unit 4: Paging Integration (cassandra-coordinator)
**Files:** Modify `rust/crates/cassandra-coordinator/src/read/paging.rs`, possibly add `rust/crates/cassandra-coordinator/src/read/query_pager.rs` (NEW)

**Description:** Add QueryPager abstraction that tracks page boundaries during read execution. Wire PagingState into result generation so that SELECT results include continuation tokens. Implement page size enforcement (stop reading after N rows). Handle paging state in EXECUTE messages (resume from previous page). Add `encode_paging_state()` for wire format in responses.

**Why independent:** Only modifies cassandra-coordinator. No other unit touches this crate's files.

**Complexity:** M

---

### Unit 5: E2E and Differential Tests (cassandra-diff-tests)
**Files:** Add test files in `rust/crates/cassandra-diff-tests/`, possibly add integration test files in `rust/crates/cassandra-server/tests/`

**Description:** Write tests covering:
1. Parse→plan→execute→result for basic SELECT, INSERT, UPDATE, DELETE
2. PREPARE/EXECUTE round-trip with parameter binding
3. Paging state in multi-page SELECT results
4. Error mapping (syntax error → SyntaxError, auth failure → Unauthorized, etc.)
5. Prepared statement cache invalidation on schema change
6. Differential tests comparing Rust output vs Java baseline for simple queries
7. ResultSet metadata correctness (column names, types, keyspace)

Note: If QueryProcessor (Unit 3) hasn't landed yet, write tests against the Java baseline and add `#[ignore]` tests for Rust-side verification with TODO markers.

**Why independent:** Only adds new test files. No overlap with other units.

**Complexity:** M

---

### Unit 6: ADR Documentation (docs/rewrite)
**Files:** `docs/rewrite/adr-022-query-processor.md` (NEW), `docs/rewrite/phase02-query-execution.md` (NEW)

**Description:** Document:
1. ADR for QueryProcessor design decisions: why QueryProcessor lives in cassandra-server (not a new crate), why ClientState is separate from ConnectionContext, why parameter binding modifies AST, prepared statement ID computation (MD5 of keyspace+query matching Java), cache eviction strategy
2. Phase 02 status document: gaps closed, gaps remaining, integration points for Phase 03 (coordinator wiring, distributed execution)

**Why independent:** Only creates new doc files.

**Complexity:** S

---

## Codebase Conventions (for all workers)
- License header: `// Licensed under Apache License, Version 2.0.` on first line, then blank line
- Module doc comments use `//!`
- Java oracle references in module docs: `//! ## Java Oracle` followed by `//! - org.apache.cassandra.xxx`
- Error types use `thiserror::Error` derive macro
- Tests in `#[cfg(test)] mod tests { ... }` at bottom of file
- Concurrent structures: `DashMap` for lock-free maps, `parking_lot::RwLock` for shared state
- Async: `tokio` runtime, `async fn` for I/O operations
- Type conversions: `cassandra_types::CqlType` is the canonical type enum
- Wire format: `cassandra_native_protocol::message` defines all protocol message types
- Existing crate deps already include: `md-5`, `dashmap`, `parking_lot`, `tokio`, `tracing`, `bytes`, `byteorder`, `thiserror`, `anyhow`

## Key Files Reference
- `rust/crates/cassandra-server/src/server.rs` — TCP server, message routing (346 lines)
- `rust/crates/cassandra-server/src/executor.rs` — Query execution (994 lines)
- `rust/crates/cassandra-server/src/main.rs` — Mod declarations + bootstrap
- `rust/crates/cassandra-cql/src/lib.rs` — Module declarations
- `rust/crates/cassandra-cql/src/prepared.rs` — PreparedCache (337 lines)
- `rust/crates/cassandra-cql/src/planner.rs` — AST→QueryPlan validation
- `rust/crates/cassandra-cql/src/ast.rs` — Statement types, Term::BindMarker
- `rust/crates/cassandra-native-protocol/src/message.rs` — Protocol messages (459 lines)
- `rust/crates/cassandra-native-protocol/src/response.rs` — Response encoding (455 lines)
- `rust/crates/cassandra-native-protocol/src/error_codes.rs` — Error mapping
- `rust/crates/cassandra-coordinator/src/read/paging.rs` — PagingState (232 lines)

## E2E Test Recipe
Each worker should verify their changes compile and pass tests:
```bash
cd rust && cargo build --workspace 2>&1 | tail -20
cd rust && cargo test --workspace 2>&1 | tail -40
```
If adding integration tests that need a running server, mark them `#[ignore]` with a comment explaining the prerequisite. Unit tests should be self-contained.

Skip browser/UI e2e — this is a Rust library/binary crate with no frontend. `cargo test --workspace` is the primary verification.

## Worker Instructions Template
```
After you finish implementing the change:
1. **Simplify** — Invoke the `Skill` tool with `skill: "simplify"` to review and clean up your changes.
2. **Run unit tests** — Run `cd rust && cargo test --workspace 2>&1 | tail -50`. If tests fail, fix them.
3. **Test end-to-end** — Run `cd rust && cargo build --workspace` to verify full compilation. No browser e2e needed.
4. **Commit and push** — Commit all changes with a clear message, push the branch, and create a PR with `gh pr create`. Use a descriptive title.
5. **Report** — End with a single line: `PR: <url>` so the coordinator can track it. If no PR was created, end with `PR: none — <reason>`.
```
