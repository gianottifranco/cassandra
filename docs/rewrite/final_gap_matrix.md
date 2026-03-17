# Final Gap Matrix — Cassandra Java -> Rust Rewrite

**Generated**: 2026-03-17 (Prompt 11 — Definitive Baseline Freeze)
**Baseline**: trunk `076c6f11` | Stable ref: `cassandra-5.0.3`
**Source**: [`final_gap_matrix.yaml`](final_gap_matrix.yaml)

## Summary Statistics

| Status | Count | Description |
|--------|-------|-------------|
| `done` | 0 | Fully at parity with evidence |
| `partial` | 44 | Core logic implemented, gaps documented |
| `stub` | 18 | API surface exists, minimal logic |
| `missing` | 45 | Not yet started |
| `trunk-only` | 2 | Present only in trunk, feature-gated |
| `experimental` | 5 | Flagged experimental upstream |
| `baseline-excluded` | 4 | Excluded with justification |

**Total features tracked**: 118

---

## Core Data Path

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| CQL Type System | `partial` | cassandra-types | P0 | Frozen deep-nesting, DynamicCompositeType |
| CQL Parser | `partial` | cassandra-cql | P0 | GRANT/REVOKE, roles, JSON, UDF/UDA |
| CQL Conditions (LWT IF) | `partial` | cassandra-cql | P1 | Collection element conditions |
| CQL Constraints | `missing` | cassandra-cql | P2 | CHECK constraints (trunk-only) |
| CQL Functions (builtins, UDF, UDA) | `missing` | cassandra-cql | P1 | All builtin + aggregate + UDF/UDA |
| CQL Masking Functions | `missing` | cassandra-cql | P2 | Dynamic data masking |
| CQL Function Type Helpers | `missing` | cassandra-cql | P2 | UDF type resolution |
| CQL Schema Statements | `missing` | cassandra-cql | P1 | CREATE/ALTER/DROP for types/functions/indexes |
| CQL Selection Function Calls | `partial` | cassandra-cql | P1 | writetime, ttl, cast in SELECT |
| CQL Advanced Terms | `partial` | cassandra-cql | P1 | UserTypes literal, collection modifiers |
| CQL Restrictions | `partial` | cassandra-cql | P0 | Multi-column, token(), CONTAINS, LIKE |
| CQL Selection & Projection | `partial` | cassandra-cql | P0 | Function calls, aliases, writetime/ttl |
| CQL Statements | `partial` | cassandra-cql | P0 | Permission/role/index/function stmts, DESCRIBE |
| CQL Terms & Literals | `partial` | cassandra-cql | P0 | Function terms, casts, collection constructors |
| CQL Accord Transactions | `experimental` | -- | P3 | Entire subsystem (experimental) |
| Prepared Statement Cache | `partial` | cassandra-cql | P0 | Not wired to protocol layer |

## Storage Engine

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Memtable | `partial` | cassandra-storage | P0 | TrieMemtable, off-heap, concurrent writes |
| Trie Memtable | `missing` | cassandra-storage | P1 | Off-heap trie-based memtable |
| CommitLog | `partial` | cassandra-storage | P0 | Compression, encryption, archive, CDC |
| CommitLog Compression & Encryption | `missing` | cassandra-storage | P1 | Compressed/encrypted segments, CDC |
| SSTable Read | `partial` | cassandra-storage | P0 | Java format compat, trie index, bloom interop |
| SSTable Write | `partial` | cassandra-storage | P0 | Java format compat |
| SSTable Format Implementation | `partial` | cassandra-storage | P0 | big-format reader, BTI format |
| SSTable Indexing | `missing` | cassandra-storage | P1 | IndexSummary, key cache integration |
| SSTable Metadata | `missing` | cassandra-storage | P1 | StatsMetadata, CompactionMetadata |
| Compaction | `stub` | cassandra-storage | P0 | Actual execution, task management |
| Unified Compaction Strategy | `missing` | cassandra-storage | P1 | UCS tiered/leveled hybrid |
| Compaction Writers | `missing` | cassandra-storage | P1 | Splitting, leveled writers |
| Compression | `partial` | cassandra-storage | P1 | Zstd, dictionaries |
| Compressed I/O | `partial` | cassandra-storage | P1 | CompressedSequentialWriter/Reader |
| I/O Utilities | `partial` | cassandra-io | P1 | FileHandle, RandomAccessReader |
| Counter Context | `partial` | cassandra-storage | P1 | Old shard cleanup |
| Column Index Builder | `missing` | cassandra-storage | P1 | Partition-internal row index |
| Type Marshalling (Extended) | `partial` | cassandra-types | P0 | DynamicCompositeType, frozen nesting |
| Row/Partition Filters | `stub` | cassandra-storage | P0 | ClusteringIndexFilter, ColumnFilter |
| Guardrails | `missing` | cassandra-config | P2 | All guardrail checks |
| SSTable Lifecycle | `stub` | cassandra-storage | P1 | Tracker, LogTransaction |
| Partition Iterators | `partial` | cassandra-storage | P0 | UnfilteredPartitionIterator, merge |
| Row Representation | `partial` | cassandra-storage | P0 | ComplexColumnData, range tombstones |
| DB-level Streaming | `stub` | cassandra-streaming | P2 | SSTable streaming integration |
| Row Transformations | `missing` | cassandra-storage | P1 | Filter, DuplicateRowChecker |
| Trie Index | `missing` | cassandra-storage | P1 | InMemoryTrie, trie partition index |
| Query Monitoring | `missing` | cassandra-admin | P2 | Slow query log, abort mechanism |
| DB Repair Metadata | `stub` | cassandra-repair | P2 | Anti-compaction, repair tracking |
| Query Aggregation | `missing` | cassandra-coordinator | P1 | GROUP BY, aggregate functions |
| Materialized Views | `baseline-excluded` | -- | deferred | Deprecated upstream |
| Virtual Tables Framework | `partial` | cassandra-admin | P2 | ~60 additional virtual table types |
| Caching (Key, Row, Counter) | `missing` | cassandra-storage | P1 | KeyCache, RowCache, CounterCache |
| I/O Layer | `partial` | cassandra-storage | P0 | Java format compat, compressed I/O |

## Native Protocol

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| CQL Native Protocol v4/v5 | `stub` | cassandra-native-protocol | P0 | No TCP listener, no query execution |

## Schema & Configuration

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Schema Catalog | `partial` | cassandra-schema | P0 | UDT/UDA/UDF metadata, agreement, migration |
| Configuration | `stub` | cassandra-config | P0 | Most config keys missing |

## Distributed Operations

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Gossip & Failure Detection | `partial` | cassandra-cluster-metadata | P1 | Wire format not binary-compatible |
| Snitches & Topology | `partial` | cassandra-cluster-metadata | P1 | Dynamic snitch, PropertyFileSnitch |
| Dynamic Snitch | `missing` | cassandra-cluster-metadata | P1 | Latency-aware routing |
| Partitioner & Token Ring | `partial` | cassandra-cluster-metadata | P1 | ByteOrdered, Random partitioners |
| Internode Messaging | `stub` | cassandra-messaging | P1 | No wire compat, no connection pool |
| Read Coordinator | `partial` | cassandra-coordinator | P1 | Speculative retry, read repair |
| Read Repair | `missing` | cassandra-coordinator | P1 | Blocking/Async read repair |
| Write Coordinator | `partial` | cassandra-coordinator | P1 | Counter writes, batch log, view updates |
| Paging | `missing` | cassandra-coordinator | P1 | QueryPager, paging state |
| Coordinator Thresholds | `missing` | cassandra-coordinator | P2 | Local read/write time tracking |
| Service Layer | `partial` | cassandra-coordinator | P0 | Bootstrap, token migration |
| Disk Management | `missing` | cassandra-admin | P2 | Disk boundary management |
| Snapshot Management | `stub` | cassandra-admin | P2 | Snapshot management service |
| Paxos / LWT | `partial` | cassandra-coordinator | P2 | system.paxos persistence, PaxosV2 |
| Repair & Anti-Entropy | `stub` | cassandra-repair | P2 | No actual data exchange |
| Repair Messages | `missing` | cassandra-repair | P2 | Inter-node repair coordination |
| Asymmetric Repair | `missing` | cassandra-repair | P2 | DifferenceHolder, RangeMap |
| Consistent Repair | `missing` | cassandra-repair | P2 | CoordinatorSession, LocalSession |
| Streaming | `stub` | cassandra-streaming | P2 | No actual data transfer |
| Streaming Management | `missing` | cassandra-streaming | P2 | Stream monitoring |
| Streaming Messages | `missing` | cassandra-streaming | P2 | StreamMessage types |
| Service Consensus Sub-packages | `experimental` | -- | P3 | Experimental |

## Indexes

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Secondary Index Framework | `partial` | cassandra-storage | P2 | Lifecycle management |
| Built-in Indexes | `partial` | cassandra-storage | P2 | Composites, keys, values sub-types |
| SAI | `partial` | cassandra-storage | P1 | On-disk persistence, HNSW/IVF ANN |
| SAI Disk Format | `missing` | cassandra-storage | P1 | v1-v5 formats, vector persistence |
| SAI Query Planning | `partial` | cassandra-storage | P1 | QueryController, Expression |
| SAI Analyzers | `missing` | cassandra-storage | P2 | Text analyzers, filter chain |
| SASI | `baseline-excluded` | -- | deferred | Deprecated in favor of SAI |
| Accord Index Integration | `experimental` | -- | P3 | Experimental |

## Security

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Authentication | `partial` | cassandra-security | P1 | LDAP, Kerberos, persistent stores |
| Auth Persistent Stores | `missing` | cassandra-security | P1 | system_auth keyspace persistence |
| Authorization | `partial` | cassandra-security | P1 | Persistent permission store |
| TLS/SSL | `partial` | cassandra-security | P1 | Not wired to listeners |
| Audit Logging | `partial` | cassandra-security | P2 | Syslog sink, BinAuditLogger |
| Full Query Logging | `missing` | cassandra-security | P2 | Needs file-based FQL |

## Hints & Batchlog

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Hinted Handoff | `stub` | cassandra-coordinator | P1 | File persistence, delivery |
| Hints File Persistence | `missing` | cassandra-coordinator | P1 | On-disk hint storage |
| Batchlog | `stub` | cassandra-coordinator | P1 | Actual batchlog write/replay |

## Observability & Tooling

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Metrics | `partial` | cassandra-admin | P1 | Full JMX metric coverage |
| Distributed Tracing | `stub` | cassandra-coordinator | P2 | system_traces storage |
| Diagnostics Events | `missing` | cassandra-admin | P3 | DiagnosticEventService |
| Notifications | `missing` | cassandra-common | P3 | INotification hierarchy |
| nodetool CLI | `stub` | cassandra-tools | P2 | ~140 commands missing |
| Nodetool Output Formatters | `missing` | cassandra-tools | P2 | TableFormatter |
| Nodetool Stats Commands | `missing` | cassandra-tools | P2 | TableStatsHolder, StatsPrinter |
| SSTable Offline Tools | `stub` | cassandra-tools | P2 | All tools are stubs |
| HTTP Admin API | `partial` | cassandra-admin | P2 | Rust-only feature |

## Common / Utilities

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Utilities | `stub` | cassandra-common | P0 | BloomFilter, ByteBufferUtil, FBUtilities |
| Bloom Filter | `missing` | cassandra-common | P0 | BloomFilter, FilterFactory |
| Memory Management | `missing` | cassandra-common | P1 | MemtableAllocator, SlabAllocator |
| OffHeap BitSet | `missing` | cassandra-common | P2 | Bloom filter backend |
| Concurrent Utilities | `missing` | cassandra-common | P1 | Ref, SharedCloseable, OpOrder |
| Streaming Histograms | `missing` | cassandra-common | P2 | StreamingTombstoneHistogramBuilder |
| ByteComparable & Tries | `missing` | cassandra-common | P1 | Trie-compatible key encoding |
| Type Serializers | `partial` | cassandra-types | P0 | DurationSerializer edge cases |
| Change Data Capture | `missing` | cassandra-storage | P2 | CDC commit log integration |
| Harry Testing Framework | `baseline-excluded` | -- | deferred | Java-only testing framework |
| Concurrency Primitives | `missing` | cassandra-common | P1 | Stage model, SEPExecutor |
| Exception Types | `partial` | cassandra-common | P0 | Full hierarchy mapping |

## Trunk-Only / Experimental

| Feature | Status | Crate | Criticality | Condition |
|---------|--------|-------|-------------|-----------|
| TCM | `trunk-only` | -- | P3 | `#[cfg(feature = "trunk_only")]` |
| Accord | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Consensus Service | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Service Consensus Sub-pkgs | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Journal | `trunk-only` | -- | P3 | `#[cfg(feature = "trunk_only")]` |
| Profiler | `baseline-excluded` | -- | deferred | Dev-time only |
| Triggers | `stub` | cassandra-storage | P3 | `#[cfg(feature = "triggers")]` |
