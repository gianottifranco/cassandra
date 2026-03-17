# Gap Backlog: Diagnostics

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 1 missing

## fql: Full Query Logging

- **Criticality**: P2
- **Target crate**: cassandra-security
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.fql`

### Description

Chronicle-queue based. Rust needs equivalent file-based FQL.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: Security subsystem core

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

