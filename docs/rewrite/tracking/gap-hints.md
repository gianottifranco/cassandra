# Gap Backlog: Hints

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 1 missing

## hints-persistence: Hints File Persistence

- **Criticality**: P1
- **Target crate**: cassandra-coordinator
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.hints`

### Description

HintsWriter, HintsReader, HintsDescriptor for on-disk hint storage.

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

