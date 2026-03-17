# Gap Backlog: Storage

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 13 missing

## compaction-unified: Unified Compaction Strategy

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.compaction.unified`

### Description

UCS tiered/leveled hybrid. Controller, ShardManager, CostsCalculator.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: SSTable write path, compaction framework

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## compaction-writers: Compaction Writers

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.compaction.writers`

### Description

DefaultCompactionWriter, SplittingSizeTieredCompactionWriter, MajorLeveledCompactionWriter.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: SSTable write path, compaction framework

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## db-guardrails: Guardrails

- **Criticality**: P2
- **Target crate**: cassandra-config
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.guardrails`

### Description

Table/column count limits, partition size warnings, query complexity limits. Config-driven safety rails.

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

## db-transform: Row Transformations

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.transform`

### Description

Filter, DuplicateRowChecker, Transformation pipeline.

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

## db-tries: Trie Index

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.tries`

### Description

InMemoryTrie, trie-based partition index used in latest SSTable format.

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

## db-monitoring: Query Monitoring

- **Criticality**: P2
- **Target crate**: cassandra-admin
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.monitoring`

### Description

Slow query log, operation timeouts, abort mechanism.

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

## db-aggregation: Query Aggregation

- **Criticality**: P1
- **Target crate**: cassandra-coordinator
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.aggregation`

### Description

GROUP BY aggregation, aggregate functions.

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

## cache: Caching (Key, Row, Counter)

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cache`

### Description

KeyCache, RowCache, CounterCache, SerializingCache.

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

## io-sstable-indexing: SSTable Indexing

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.io.sstable.indexsummary, org.apache.cassandra.io.sstable.keycache`

### Description

IndexSummary for sampling-based partition lookups. SSTable key cache integration.

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

## io-sstable-metadata: SSTable Metadata

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.io.sstable.metadata`

### Description

StatsMetadata, CompactionMetadata, ValidationMetadata. Read for migration compat.

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

## memtable-trie: Trie Memtable

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.memtable`

### Description

TrieMemtable for off-heap trie-based memtable. Sub-package of db.memtable.

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

## commitlog-compression: CommitLog Compression & Encryption

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.commitlog`

### Description

Compressed/encrypted commit log segments. CDC integration.

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

## db-columnindex: Column Index Builder

- **Criticality**: P1
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.db.columnindex`

### Description

ColumnIndexBuilder for partition-internal row index blocks.

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

