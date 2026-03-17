# Gap Backlog: Observability

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 2 missing

## diag: Diagnostics Events

- **Criticality**: P3
- **Target crate**: cassandra-admin
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.diag`

### Description

DiagnosticEventService, event broadcasting.

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

## notifications: Notifications

- **Criticality**: P3
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.notifications`

### Description

INotification, SSTableListChangedNotification, etc.

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

