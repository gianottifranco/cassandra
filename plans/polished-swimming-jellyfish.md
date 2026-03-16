# Plan: CQL Semantic Layer — Restrictions, Selection, Conditions, Built-in Functions

## Context

The gap analysis (`prompts/step 3/INPUT_full_gap_analysis.md`) identifies CQL semantic validation as **entirely missing** (0% complete). The Rust CQL crate has a complete parser and AST but the planner passes WHERE clauses, selectors, and IF conditions through without validation. There are zero built-in functions, no function resolver, no restriction validation, no selector evaluation, and no condition evaluation.

**Gaps being closed in this phase:**
- `cql3/restrictions/**` — 14 Java files, 0 Rust equivalent
- `cql3/selection/**` — 28 Java files, 0 Rust equivalent
- `cql3/conditions/**` — 6 Java files, 0 Rust equivalent
- `cql3/functions/**` — 47+ Java files, 0 Rust equivalent (UDF registry exists but no built-ins)
- Missing statements: AlterType, AlterView, Describe, ListUsers/ListSuperUsers
- Planner: 11 already-parsed statement types fall through to `_` catch-all

**Existing foundation:**
- `cassandra-cql/src/ast.rs` (619 LOC): Complete AST with `Relation`, `Selector`, `Term`, `RelationOp`
- `cassandra-cql/src/planner.rs` (613 LOC): Plans 16 of 27 stmt types, has TODO at line 360
- `cassandra-schema`: `TableMetadata` with `partition_key_columns()`, `clustering_columns()`, `column()`, `ColumnKind`, `ColumnMetadata`
- `cassandra-types`: `CqlValue` enum (30 variants), `CqlType` enum

## Work Units

### Unit 1: Restrictions Framework
**Files:** New `rust/crates/cassandra-cql/src/restrictions.rs`
**Change:** Implement `StatementRestrictions` validator that takes a `Vec<Relation>` + `TableMetadata` and validates: all referenced columns exist, partition key is fully restricted (or ALLOW FILTERING), clustering restrictions are contiguous prefix, IN restrictions only on last partition key or clustering column, range restrictions (</>/<=/>=/LIKE) only on clustering columns, CONTAINS/CONTAINS KEY only on collection columns. Produces structured `ValidatedRestrictions` with partition key values, clustering bounds, and filter predicates. Add `RelationOp::Like` and `RelationOp::IsNotNull` to `ast.rs` (2 lines).
**Tests:** ~15 tests: valid PK restriction, missing PK error, non-contiguous clustering error, IN on wrong column, range on regular column, ALLOW FILTERING bypass, multi-column scenarios.

### Unit 2: Selection Engine
**Files:** New `rust/crates/cassandra-cql/src/selection.rs`
**Change:** Implement `Selection` struct that resolves `SelectColumns` + `Vec<Selector>` against `TableMetadata`. `ResolvedSelector` enum: Column(name, type), Function(name, args), Alias(inner, name), CountAll, WritetimeOrTtl(column, which). `ResultSetBuilder` that accumulates rows and applies selectors to produce result columns. Column validation (all selected columns must exist). `AggregationState` for count/sum/avg/min/max tracking across rows. Wildcard expansion (SELECT * → all columns in schema order).
**Tests:** ~12 tests: wildcard expansion, named columns, alias, non-existent column error, writetime/ttl selector, count(*), basic aggregation state.

### Unit 3: LWT Conditions
**Files:** New `rust/crates/cassandra-cql/src/conditions.rs`
**Change:** Implement `ColumnCondition` that evaluates `IF col op value` against a current row (`HashMap<String, CqlValue>`). Support IF EXISTS (row must be present), IF NOT EXISTS (row must be absent), IF col = val / IF col != val / IF col < val etc. `ConditionSet` holds multiple conditions with AND semantics. Validation: condition columns must exist in table, operator must be valid for column type. Evaluation returns `ConditionResult { applied: bool, existing_row: Option<Row> }` matching Cassandra's CAS response format.
**Tests:** ~10 tests: IF EXISTS true/false, IF NOT EXISTS, IF col = val match/mismatch, IF col != val, IF col IN (...), multiple conditions AND, condition on non-existent column error.

### Unit 4: Function Infrastructure + Registry
**Files:** New `rust/crates/cassandra-cql/src/functions/mod.rs`, `rust/crates/cassandra-cql/src/functions/registry.rs`, `rust/crates/cassandra-cql/src/functions/resolver.rs`
**Change:** Define `NativeFunction` trait: `fn name() -> &str`, `fn arg_types() -> &[CqlType]`, `fn return_type() -> CqlType`, `fn execute(args: &[CqlValue]) -> Result<CqlValue>`, `fn is_pure() -> bool`, `fn is_aggregate() -> bool`. `FunctionRegistry` stores functions by name (supports overloads). `FunctionResolver` finds best match given name + argument types: exact match > implicit cast match > error. Implicit cast rules matching Java (e.g., int→bigint, float→double). Register all built-in functions via `register_all()`.
**Tests:** ~10 tests: register/lookup, overload resolution exact match, overload with implicit cast, no match error, multiple overloads.

### Unit 5: Aggregate Functions
**Files:** New `rust/crates/cassandra-cql/src/functions/aggregate_fcts.rs`
**Change:** Implement count(col), count(*), sum(T), avg(T), min(T), max(T) for all numeric types (tinyint, smallint, int, bigint, float, double, varint, decimal, counter). Each as a struct implementing the function trait from Unit 4 (or a local trait if building independently). `AggregateAccumulator` trait with `accumulate(&mut self, val: &CqlValue)` and `finalize(&self) -> CqlValue`. Sum/avg handle null correctly (skip nulls, avg divides by non-null count).
**Tests:** ~12 tests: count with nulls, sum of ints, avg of floats, min/max with mixed values, empty input, type mismatch error.

### Unit 6: Time, UUID, and Token Functions
**Files:** New `rust/crates/cassandra-cql/src/functions/time_fcts.rs`, `rust/crates/cassandra-cql/src/functions/uuid_fcts.rs`, `rust/crates/cassandra-cql/src/functions/token_fct.rs`
**Change:** Implement: `now()` → timeuuid, `currentTimestamp()` → timestamp, `currentDate()` → date, `currentTime()` → time, `toDate(timestamp)`, `toTimestamp(date)`, `toUnixTimestamp(timeuuid|timestamp|date)`, `dateOf(timeuuid)`, `unixTimestampOf(timeuuid)`, `minTimeuuid(timestamp)`, `maxTimeuuid(timestamp)`. `uuid()` → random UUID v4. `token(values...)` → bigint using Murmur3 hash (default partitioner). Add `uuid` crate to Cargo.toml dependencies.
**Tests:** ~15 tests: now() returns valid timeuuid, toDate round-trip, toTimestamp round-trip, uuid() uniqueness, token() deterministic, token() matches known Murmur3 output.

### Unit 7: Cast, Math, Bytes, Length, Operations, and Format Functions
**Files:** New `rust/crates/cassandra-cql/src/functions/scalar_fcts.rs`
**Change:** Implement:
- **Cast:** `cast_as_int`, `cast_as_bigint`, `cast_as_float`, `cast_as_double`, `cast_as_text`, `cast_as_decimal`, etc. Full cast compatibility matrix matching Java.
- **Math:** `abs(T)`, `exp(double)`, `log(double)`, `log10(double)`, `round(T)` for numeric types.
- **Bytes:** `blobAsInt`, `intAsBlob`, etc. for all native types (using CqlValue serialization).
- **Length:** `length(varchar)` → int.
- **Operations:** `+`, `-`, `*`, `/` for numeric types, `+` for string concatenation, `+`/`-` for date/timestamp arithmetic.
- **Format:** `toJson(any)` → text (JSON serialization of CqlValue), `fromJson(text)` → parsed CqlValue.
**Tests:** ~20 tests: cast int→bigint, cast overflow, math functions, blob round-trip, length, arithmetic, toJson/fromJson round-trip.

### Unit 8: Missing DDL/Auth Statements
**Files:** Modifies `ast.rs`, `parser.rs`, `planner.rs`, `lexer.rs`
**Change:** Add parsing and planning for:
- `ALTER TYPE ks.name ADD field type | RENAME field TO newfield` → AlterType AST + AlterTypePlan
- `ALTER MATERIALIZED VIEW ks.name WITH options` → AlterMaterializedView AST + AlterMaterializedViewPlan
- `DESCRIBE [KEYSPACE|TABLE|TYPE|...] [name]` → DescribeStatement AST + DescribePlan
- `LIST USERS` / `LIST SUPERUSERS` → filter variants of ListRoles
- New keywords in lexer: `DESCRIBE`, `USERS`, `SUPERUSERS`
- Add 3 new Statement enum variants, 3 new QueryPlan variants
**Tests:** ~10 tests: parse ALTER TYPE ADD/RENAME, parse ALTER MV, parse DESCRIBE variants, parse LIST USERS.

### Unit 9: Planner Completion for Existing Statements
**Files:** Modifies `planner.rs`
**Change:** Add plan structs and match arms for the 11 already-parsed statement types that currently fall through to `_`: CreateIndex, DropIndex, CreateMaterializedView, DropMaterializedView, CreateType, DropType, CreateFunction, DropFunction, CreateAggregate, DropAggregate, CreateTrigger, DropTrigger, ListPermissions. Each plan struct copies AST fields with resolved keyspaces. Replace `_` catch-all with explicit arms.
**Tests:** ~11 tests: one per new plan type, verifying keyspace resolution and field propagation.

### Unit 10: Planner Semantic Integration
**Files:** Modifies `planner.rs` (plan_select/update/delete sections)
**Change:** Wire restrictions, selection, and conditions into the planner:
- SELECT: validate WHERE clause via restrictions module, validate SELECT columns via selection module
- UPDATE/DELETE: validate WHERE clause, validate IF conditions via conditions module
- INSERT: validate column names exist, validate value count matches column count
- Add column-exists checks for all DML statements
- Import and use the new modules (add `mod restrictions; mod selection; mod conditions; mod functions;` to lib.rs)
**Tests:** ~10 tests: SELECT with invalid column rejected, UPDATE with bad WHERE rejected, INSERT column count mismatch, IF condition on non-existent column.

### Unit 11: Documentation and ADR
**Files:** New `docs/rewrite/adrs/adr-010-cql-semantic-layer.md`, update `docs/rewrite/reports/prompt-03-cql-semantics.md`
**Change:** ADR documenting the design decisions for the CQL semantic layer: why separate modules for restrictions/selection/conditions/functions, the function trait design, overload resolution algorithm, how conditions integrate with LWT. Report documenting gaps closed, gaps remaining, and risks.
**Tests:** None (documentation only).

## E2E Test Recipe

This is a Rust library crate — no UI, no server to start. E2E verification:

```bash
cd rust/
cargo build --all 2>&1     # Must compile cleanly
cargo test -p cassandra-cql 2>&1  # All tests must pass
cargo test --all 2>&1       # No regressions in other crates
```

Skip browser/CLI e2e — unit tests + `cargo build` are sufficient for a library crate with no runtime entry point.

## Worker Instructions Template

Each worker receives:
1. The overall goal (CQL semantic layer implementation)
2. Their specific unit task (title, files, change description)
3. Conventions: Apache 2.0 license header, `// Java Oracle: org.apache.cassandra.cql3.X` doc comments, `#[cfg(test)] mod tests` at bottom of file, use `CqlValue` from `cassandra_types`, use `TableMetadata`/`ColumnMetadata` from `cassandra_schema`, follow existing patterns in `planner.rs` and `ast.rs`
4. E2e recipe: `cargo build --all && cargo test -p cassandra-cql`
5. Standard worker completion steps (simplify, test, commit, PR)
