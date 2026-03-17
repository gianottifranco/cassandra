# Gap Backlog: Repair

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 3 missing

## repair-messages: Repair Messages

- **Criticality**: P2
- **Target crate**: cassandra-repair
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.repair.messages`

### Description

RepairMessage types for inter-node repair coordination.

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

## repair-asymmetric: Asymmetric Repair

- **Criticality**: P2
- **Target crate**: cassandra-repair
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.repair.asymmetric`

### Description

DifferenceHolder, RangeMap for optimized repair.

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

## repair-consistent: Consistent Repair

- **Criticality**: P2
- **Target crate**: cassandra-repair
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.repair.consistent`

### Description

CoordinatorSession, LocalSession for incremental repair.

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

