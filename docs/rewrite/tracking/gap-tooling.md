# Gap Backlog: Tooling

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 2 missing

## tools-nodetool-formatter: Nodetool Output Formatters

- **Criticality**: P2
- **Target crate**: cassandra-tools
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.tools.nodetool.formatter`

### Description

TableFormatter and output formatting for nodetool output.

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

## tools-nodetool-stats: Nodetool Stats Commands

- **Criticality**: P2
- **Target crate**: cassandra-tools
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.tools.nodetool.stats`

### Description

TableStatsHolder, StatsTable, StatsPrinter for cfstats/tablestats.

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

