# Read Path Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the native-server-visible read-path parity slice for paging, tombstone warnings/failures, response metadata, and documented guard-rails around the still-unwired distributed coordinator path.

**Architecture:** Keep the existing distributed read coordinator in `cassandra-coordinator` intact, but harden the currently active `cassandra-server` local read path so it surfaces the same client-visible contracts for paging, warnings, and read errors. Limit changes to the active serving path, benchmark the hot filtering/paging helpers already added in the coordinator crate, and document the remaining integration boundary explicitly.

**Tech Stack:** Rust workspace crates (`cassandra-server`, `cassandra-coordinator`, `cassandra-cql`, `cassandra-native-protocol`), cargo tests, criterion benchmark, rewrite docs.

---

### Task 1: Stabilize the current branch state

**Files:**
- Modify: `rust/crates/cassandra-server/src/query_processor.rs`
- Modify: `rust/crates/cassandra-server/src/server.rs`
- Modify: `rust/crates/cassandra-server/src/error_mapping.rs`

**Step 1: Write/fix the failing tests**

- Collapse duplicate `#[cfg(test)] mod tests` blocks in `query_processor.rs`.
- Ensure every `QueryResult::Rows` construction/match includes paging and warnings.

**Step 2: Run the focused tests to verify failures**

Run: `cd rust && cargo test -p cassandra-server query_processor::tests::select_exposes_paging_state_across_pages -- --nocapture`

Expected: compile or test failures that confirm the branch is not yet stable.

**Step 3: Write the minimal implementation**

- Normalize `ExecutorError -> CassandraError` read-path mapping.
- Fix server response wrapping so warnings propagate on query responses without breaking lifecycle responses.

**Step 4: Run the focused tests to verify pass**

Run: `cd rust && cargo test -p cassandra-server query_processor::tests::select_exposes_paging_state_across_pages`

Expected: PASS.

**Step 5: Commit**

```bash
git add rust/crates/cassandra-server/src/query_processor.rs rust/crates/cassandra-server/src/server.rs rust/crates/cassandra-server/src/error_mapping.rs
git commit -m "fix: stabilize native read response contract"
```

### Task 2: Close observable single-partition read semantics

**Files:**
- Modify: `rust/crates/cassandra-server/src/executor.rs`
- Modify: `rust/crates/cassandra-cql/src/planner.rs`

**Step 1: Add/extend tests**

- Cover paging continuation, reversed ordering, per-partition limit, tombstone warnings, and tombstone failure propagation on single-partition reads.

**Step 2: Run focused tests**

Run: `cd rust && cargo test -p cassandra-server query_processor::tests:: -- --nocapture`

Expected: failures on any unimplemented behavior.

**Step 3: Implement**

- Thread `QueryParams.page_size` and `paging_state`.
- Enforce `PER PARTITION LIMIT`.
- Preserve stable cursor generation and warning propagation.
- Map tombstone overwhelming reads through read-path errors.

**Step 4: Re-run tests**

Run: `cd rust && cargo test -p cassandra-server query_processor::tests::`

Expected: PASS.

**Step 5: Commit**

```bash
git add rust/crates/cassandra-server/src/executor.rs rust/crates/cassandra-cql/src/planner.rs
git commit -m "feat: close native single-partition read paging semantics"
```

### Task 3: Validate benchmarks and documentation

**Files:**
- Modify: `docs/rewrite/read_path_parity.md`
- Add/Modify: `rust/crates/cassandra-coordinator/benches/read_path.rs`

**Step 1: Verify benchmark and docs coverage**

- Confirm the benchmark exercises partition filtering and paging-state serialization.
- Document closed scope and explicit remaining guard-rails.

**Step 2: Run validation**

Run: `cd rust && cargo bench -p cassandra-coordinator --bench read_path --no-run`

Expected: benchmark target builds successfully.

**Step 3: Finalize docs**

- Refresh parity notes, commands, and next logical cut.

**Step 4: Commit**

```bash
git add docs/rewrite/read_path_parity.md rust/crates/cassandra-coordinator/Cargo.toml rust/crates/cassandra-coordinator/benches/read_path.rs
git commit -m "docs: record read-path parity slice"
```

### Task 4: Final validation and handoff artifacts

**Files:**
- Modify: `.symphony/run-result.json`

**Step 1: Run final focused validation**

Run:
- `cd rust && cargo test -p cassandra-server query_processor::tests::select_exposes_paging_state_across_pages`
- `cd rust && cargo test -p cassandra-server query_processor::tests::select_surfaces_tombstone_warnings`
- `cd rust && cargo test -p cassandra-server`
- `cd rust && cargo bench -p cassandra-coordinator --bench read_path --no-run`

**Step 2: Write result artifact**

- Set status, short summary, and notes in `.symphony/run-result.json`.

**Step 3: Final commit**

```bash
git add .symphony/run-result.json
git commit -m "chore: record ISS-1 run result"
```
