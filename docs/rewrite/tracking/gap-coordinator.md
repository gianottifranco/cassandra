# Gap Backlog: Coordinator

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 4 missing

## coordinator-pager: Paging

- **Criticality**: P1
- **Target crate**: cassandra-coordinator
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.service.pager`

### Description

QueryPager, paging state serialization.

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

## coordinator-thresholds: Coordinator Thresholds

- **Criticality**: P2
- **Target crate**: cassandra-coordinator
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.service.thresholds`

### Description

Coordinator-level local read/write time tracking.

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

## service-disk: Disk Management

- **Criticality**: P2
- **Target crate**: cassandra-admin
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.service.disk`

### Description

Disk boundary management.

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

## coordinator-read-repair: Read Repair

- **Criticality**: P1
- **Target crate**: cassandra-coordinator
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.service.reads.repair`

### Description

BlockingReadRepair, AsyncReadRepair for consistency convergence.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: Merkle trees, messaging

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

