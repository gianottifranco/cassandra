# Gap Backlog: Indexes

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 2 missing

## index-sai-disk-format: SAI Disk Format

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.index.sai.disk.format, org.apache.cassandra.index.sai.disk.io, org.apache.cassandra.index.sai.disk.v1, org.apache.cassandra.index.sai.disk.v2, org.apache.cassandra.index.sai.disk.v3, org.apache.cassandra.index.sai.disk.v5, org.apache.cassandra.index.sai.disk.vector`

### Description

SAI on-disk format versioning (v1-v5), vector index persistence, I/O layer.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: Secondary Index Framework

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## index-sai-analyzer: SAI Analyzers

- **Criticality**: P2
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.index.sai.analyzer, org.apache.cassandra.index.sai.analyzer.filter`

### Description

Text analyzers (Standard, NonTokenizing), filter chain for SAI text indexes.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: Secondary Index Framework

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

