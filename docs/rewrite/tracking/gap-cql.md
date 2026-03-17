# Gap Backlog: Cql

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 5 missing

## cql-constraints: CQL Constraints

- **Criticality**: P2
- **Target crate**: cassandra-cql
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cql3.constraints`

### Description

CHECK constraints. Trunk-only feature.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- None identified

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## cql-functions: CQL Functions (builtins, UDF, UDA)

- **Criticality**: P1
- **Target crate**: cassandra-cql
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cql3.functions`

### Description

Builtin functions (token, now, uuid, cast, etc.), aggregate functions (count, sum, avg, min, max), UDFs (Java/JavaScript), UDAs. UDF needs sandbox/WASM strategy.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- None identified

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## cql-functions-masking: CQL Masking Functions

- **Criticality**: P2
- **Target crate**: cassandra-cql
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cql3.functions.masking`

### Description

Dynamic data masking functions (mask_default, mask_null, mask_inner, etc.).

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- None identified

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## cql-functions-types: CQL Function Type Helpers

- **Criticality**: P2
- **Target crate**: cassandra-cql
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cql3.functions.types`

### Description

UDF type resolution helpers, CodecRegistry for UDF arguments.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- None identified

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## cql-statements-schema: CQL Schema Statements

- **Criticality**: P1
- **Target crate**: cassandra-cql
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cql3.statements.schema`

### Description

CREATE/ALTER/DROP for types, functions, aggregates, indexes, triggers. Sub-package of statements.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- None identified

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

