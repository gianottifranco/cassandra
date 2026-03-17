# Gap Backlog: Common

**Generated**: 2026-03-17 (Prompt 11)
**Features**: 8 missing

## utils-bloom-filter: Bloom Filter

- **Criticality**: P0
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.bloom`

### Description

BloomFilter, FilterFactory for SSTable bloom filters.

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

## utils-memory: Memory Management Utilities

- **Criticality**: P1
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.memory`

### Description

MemtableAllocator, NativeAllocator, SlabAllocator, MemtablePool.

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

## utils-obs: Observable Utilities

- **Criticality**: P2
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.obs`

### Description

OffHeapBitSet, OpenBitSet for bloom filter backends.

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

## utils-concurrent: Concurrent Utilities

- **Criticality**: P1
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.concurrent`

### Description

Ref, SharedCloseable, Transactional, WaitQueue, OpOrder.

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

## utils-streaming: Streaming Utilities

- **Criticality**: P2
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.streamhist`

### Description

StreamingTombstoneHistogramBuilder for estimated histogram in SSTables.

### Acceptance Criteria

- [ ] Rust implementation compiles and passes unit tests
- [ ] Gap guard test in `gap_guards.rs` un-ignored and passing
- [ ] Gap matrix status updated to `partial` or `done`
- [ ] Coverage audit passes with this feature classified

### Dependencies

- Depends on: Internode messaging

### Test Strategy

- Unit tests for core logic
- Integration tests against Java oracle where applicable

---

## utils-bytecomparable: ByteComparable & Tries

- **Criticality**: P1
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.utils.bytecomparable`

### Description

ByteComparable, ByteSource for trie-compatible key encoding.

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

## cdc: Change Data Capture

- **Criticality**: P2
- **Target crate**: cassandra-storage
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.cdc`

### Description

CDCCommitLogSegmentAllocator, CDC reader for streaming changes.

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

## concurrent: Concurrency Primitives

- **Criticality**: P1
- **Target crate**: cassandra-common
- **Closure phase**: prompt-12
- **Java packages**: `org.apache.cassandra.concurrent`

### Description

SEPExecutor, SharedExecutorPool, Stage. Rust uses tokio, but stage model missing.

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

