# Final Gap Matrix — Cassandra Java -> Rust Rewrite

**Generated**: 2026-03-17 (Prompt 11 — Definitive Baseline Freeze)
**Baseline**: trunk `076c6f11` | Stable ref: `cassandra-5.0.3`
**Source**: [`final_gap_matrix.yaml`](final_gap_matrix.yaml)

## Summary Statistics

| Status | Count | Description |
|--------|-------|-------------|
| `done` | 16 | Fully at parity with evidence |
| `partial` | 91 | Core logic implemented, gaps documented |
| `stub` | 0 | API surface exists, minimal logic |
| `missing` | 0 | Not yet started |
| `trunk-only` | 2 | Present only in trunk, feature-gated |
| `experimental` | 5 | Flagged experimental upstream |
| `baseline-excluded` | 4 | Excluded with justification |

**Total features tracked**: 118

---

## Core Data Path

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| CQL Type System | `done` | cassandra-types | P0 | Native, collection, frozen, UDT, vector, tuple, CompositeType, DynamicCompositeType coverage implemented |
| CQL Parser | `partial` | cassandra-cql | P0 | COMMENT ON KEYSPACE/TABLE/COLUMN/TYPE/FIELD grammar now parses to AST and plans/executes against schema metadata, BATCH grammar limits objectives to INSERT/UPDATE/DELETE like Java, DELETE USING accepts only TIMESTAMP like Java, and TRUNCATE accepts Java's optional COLUMNFAMILY keyword rather than TABLE; remaining CQL grammar edge cases |
| CQL Conditions (LWT IF) | `done` | cassandra-cql | P1 | IF condition parsing and typed evaluation implemented |
| CQL Constraints | `partial` | cassandra-cql | P2 | Full trunk grammar, validation semantics, Java reporting parity |
| CQL Functions (builtins, UDF, UDA) | `partial` | cassandra-cql | P1 | MathFcts coverage now includes fixed-width numeric abs/exp/log/log10/round, varint abs/exp/log/log10/round, and decimal abs/exp/log/log10/round; TimeFcts current/timeuuid/date/timestamp conversion names cover Java snake_case and legacy aliases, with floor(timestamp/timeuuid/date/time, duration) including UTC month calendar semantics; CastFcts fixed-width/varint/decimal numeric conversions, numeric and native boolean/inet/uuid/timeuuid/timestamp/date/time text/ascii casts, temporal conversions, and Java/legacy names; OperationFcts fixed-width/varint/decimal numeric arithmetic/negation, timestamp/date +/- duration, and text/ascii concatenation builtins; ClusterMetadataFcts transformation_kind covers Java Transformation.Kind id/name mapping; CollectionFcts map_keys/map_values cover native and complex map key/value pairs with return types derived from concrete map signatures, collection_count resolves for list/set/map, collection_min/max cover native and complex list/set elements using CQL type comparison, and collection_sum/avg cover fixed numeric, varint, and decimal list/set elements; AggregateFcts count(*)/COUNT(1), count_rows/countRows catalog aliases, dynamic count(column) catalog typing and null semantics execute, sum/avg/min/max dynamic catalog typing covers Java fixed numeric, counter, varint, decimal, and arbitrary min/max argument types, and row execution preserves Java argument result types across fixed numeric, counter, varint, and decimal execution; FormatFcts format_time/format_bytes cover Java unit conversion and formatting rules for supported scalar inputs; LengthFcts length/octet_length semantics execute in selectors; BytesConversionFcts Java/legacy blob aliases resolve for native non-blob types; JSON functions resolve Java to_json/from_json names and legacy tojson/fromjson aliases, with argument-typed to_json/tojson selector/write execution and receiver-typed from_json/fromjson execution for scalar and collection write terms; VectorFcts similarity_cosine/similarity_euclidean/similarity_dot_product include Java all-zero cosine rejection; full Java catalog, UDF sandbox/WASM parity, UDA runtime integration |
| CQL Masking Functions | `partial` | cassandra-cql | P2 | Native scalar and complex mask_default return typing/default serialization, mask_null/mask_replace return typing, Java mask_inner/mask_outer signatures including unclamped begin/end boundary behavior, mask_hash blob output with SHA-256 default plus JDK-standard MD2/MD5/SHA-1/SHA-2/SHA3 algorithms, local SELECT column masking through FunctionRegistry, CQL-typed SELECT JSON rendering of masked values, GROUP BY clear-value grouping with masked projection, clear-value ORDER BY/LIMIT before masked projection, paged local SELECT masking, CREATE/ADD/ALTER MASK validation including type-preserving return enforcement, UNMASK clear-value reads, and SELECT_MASKED authorization for masked-column restrictions implemented; provider-specific MessageDigest extensions, remaining protocol/permission edge cases, and complete read-path enforcement remain |
| CQL Function Type Helpers | `partial` | cassandra-cql | P2 | Function signatures, shared CQL type helpers, codec-backed UDF values, and UDFDataType compose empty-buffer null semantics for empty-meaningless native types implemented; full Java CodecRegistry surface and remaining conversion edge cases |
| CQL Schema Statements | `partial` | cassandra-cql | P1 | Schema mutation execution, agreement, validation semantics |
| CQL Selection Function Calls | `done` | cassandra-cql | P1 | Function, writetime/ttl/maxwritetime, alias, and cast selectors implemented |
| CQL Advanced Terms | `done` | cassandra-cql | P1 | UDT literals and collection modifiers implemented |
| CQL Restrictions | `done` | cassandra-cql | P0 | Multi-column, token(), CONTAINS/CONTAINS KEY, LIKE restrictions implemented |
| CQL Selection & Projection | `done` | cassandra-cql | P0 | Selector projection and prepared metadata inference implemented |
| CQL Statements | `partial` | cassandra-cql | P0 | COMMENT ON schema element statements now parse; KEYSPACE/TABLE/COLUMN/TYPE/FIELD comments update metadata, DESCRIBE emits comment DDL, generic/unqualified DESCRIBE resolves keyspace/table/type/function/aggregate objects, TRUNCATE follows Java's optional COLUMNFAMILY grammar and rejects TABLE, DELETE USING TIMESTAMP follows Java's delete-only USING grammar and rejects TTL, BATCH accepts only INSERT/UPDATE/DELETE objectives and USING TIMESTAMP propagates to child mutations with Java validation for global TTL/counter/conditional/mixed timestamps, and DCL covers CREATE/ALTER/DROP ROLE plus CREATE/ALTER/DROP USER aliases with Java string-literal/dollar-quoted role names, USER quoted-identifier rejection, HASHED PASSWORD, custom OPTIONS maps, ACCESS TO DATACENTERS / ACCESS FROM CIDRS, multiple permission lists, and ON ALL KEYSPACES; schema agreement/migration semantics and reporting edge cases |
| CQL Terms & Literals | `done` | cassandra-cql | P0 | Core literal, bind marker, constructor, cast/type-hint, and function-call term support implemented |
| CQL Accord Transactions | `experimental` | -- | P3 | Entire subsystem (experimental) |
| Prepared Statement Cache | `done` | cassandra-cql | P0 | Prepared/result metadata IDs and EXECUTE metadata reuse implemented |

## Storage Engine

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Memtable | `partial` | cassandra-storage | P0 | Off-heap allocation, full Java concurrent writes |
| Trie Memtable | `partial` | cassandra-storage | P1 | Off-heap trie allocation, Java concurrency/flush integration |
| CommitLog | `done` | cassandra-storage | P0 | Segment WAL, CRC, compression hooks, archive/restore, CDC mirroring |
| CommitLog Compression & Encryption | `partial` | cassandra-storage | P1 | Java segment binary compatibility, production encryption provider integration |
| SSTable Read | `partial` | cassandra-storage | P0 | Full Java SSTable row import, trie-index parity, bloom filter interop |
| SSTable Write | `partial` | cassandra-storage | P0 | Java-compatible Data.db writer and byte-for-byte Java unfiltered marker layout parity |
| SSTable Format Implementation | `partial` | cassandra-storage | P0 | big-format reader, BTI format |
| SSTable Indexing | `partial` | cassandra-storage | P1 | Java Summary.db compatibility, full downsampling/resizing behavior |
| SSTable Metadata | `partial` | cassandra-storage | P1 | ValidationMetadata, complete Statistics.db component coverage |
| Compaction | `partial` | cassandra-storage | P0 | ColumnFamilyStore/SSTable rewrite integration, Java operational parity |
| Unified Compaction Strategy | `partial` | cassandra-storage | P1 | Full controller/shard manager/cost calculator parity |
| Compaction Writers | `partial` | cassandra-storage | P1 | Dedicated writer types, SSTableRewriter lifecycle parity |
| Compression | `partial` | cassandra-storage | P1 | LZ4/Snappy/Zstd/Deflate plus chunked I/O and Zstd dictionaries implemented; Java compressed Data.db reader integration remains |
| Compressed I/O | `done` | cassandra-io | P1 | Chunked compressed writer/reader supports LZ4, Snappy, Zstd, Deflate, no-op |
| I/O Utilities | `done` | cassandra-io | P1 | FileHandle, RandomAccessReader, SequentialWriter, mmap, data I/O implemented |
| Counter Context | `done` | cassandra-storage | P1 | CRDT merge, binary format, and remote-shard cleanup implemented |
| Column Index Builder | `partial` | cassandra-storage | P1 | BigFormatPartitionWriter integration, Java row-index binary compatibility |
| Type Marshalling (Extended) | `done` | cassandra-types | P0 | Java-style TypeParser plus legacy composite/dynamic composite wire-format coverage implemented |
| Row/Partition Filters | `partial` | cassandra-storage | P0 | CQL lowering parity, Java filter edge cases |
| Guardrails | `partial` | cassandra-config | P2 | Upstream flat cassandra.yaml catalog for schema/query/table/SAI/password/role guardrails is parsed/defaulted; all CQL hook points remain |
| SSTable Lifecycle | `partial` | cassandra-storage | P1 | ColumnFamilyStore integration, notification parity |
| Partition Iterators | `partial` | cassandra-storage | P0 | Full streaming Java UnfilteredPartitionIterators parity, range tombstone merge edge cases |
| Row Representation | `partial` | cassandra-storage | P0 | Java row binary/layout parity, byte-for-byte Java range tombstone marker serialization parity |
| DB-level Streaming | `partial` | cassandra-streaming | P2 | Java CassandraStreamReader/Writer compatibility, lifecycle pinning parity |
| Row Transformations | `partial` | cassandra-storage | P1 | Full Java transformation chain coverage |
| Trie Index | `partial` | cassandra-storage | P1 | BTI/SSTable disk-format parity, byte-comparable cursor semantics |
| Query Monitoring | `partial` | cassandra-admin | P2 | Production slow-query recorder wiring, full monitorable task lifecycle |
| DB Repair Metadata | `partial` | cassandra-repair | P2 | Persisted system_distributed metadata, compaction rewrite integration |
| Query Aggregation | `partial` | cassandra-coordinator | P1 | Full Java aggregation selector semantics, distributed merge parity |
| Materialized Views | `baseline-excluded` | -- | deferred | Deprecated upstream |
| Virtual Tables Framework | `partial` | cassandra-admin | P2 | ~60 additional virtual table types |
| Caching (Key, Row, Counter) | `partial` | cassandra-storage | P1 | SerializingCache, full operational tuning |
| I/O Layer | `partial` | cassandra-storage | P0 | Java format compat, compressed I/O |

## Native Protocol

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| CQL Native Protocol v4/v5 | `partial` | cassandra-native-protocol | P0 | v5 PREPARE, QUERY, EXECUTE, and BATCH options now decode Java-compatible 32-bit flags/keyspace metadata including KEYSPACE/NOW_IN_SECONDS, custom payload is retained for QUERY/PREPARE/EXECUTE/BATCH requests, BATCH rejects non-0/1 query kinds like Java, request keyspace overrides are propagated through query/batch processing, and driver-style v5 coverage uses the correct option width plus request-keyspaced PREPARE and BATCH over TCP; remaining protocol edge cases and driver compatibility coverage |

## Schema & Configuration

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Schema Catalog | `partial` | cassandra-schema | P0 | UDT/UDF/UDA/index metadata, schema agreement, system keyspace metadata, local coordinated migrations, remote base-versioned migration envelopes, SchemaPush migration payloads, per-peer fan-out/ACK reporting with retry/backoff, divergent-version decisions, conservative snapshot merge, append-only migration journal, local+journal+fan-out coordinator, and FIFO migration scheduler implemented; production runtime/background scheduler integration, full semantic conflict resolution, and full Java schema-manager parity remain |
| Configuration | `partial` | cassandra-config | P0 | Runtime keys for bootstrap, bind/broadcast/RPC networking, modern topology providers/proximity/address config, request/repair/slow-query/internode timeouts, snitching, throughput, FD, native transport executors/protocol flags, legacy native flush batching, internode queue/socket limits, streaming state tracking, memtable/file/network-cache sizing, repair session space, partition denylist, CDC/index-summary intervals, trickle fsync, snapshots/backups, SSTable format/preemptive-open/UUID IDs, column-index sizing, read-size threshold toggles/limits, JMX/startup-check/Accord/auto-repair sections, full-SSTable streaming throughput, TLS/crypto/TDE options, hints/batchlog/heapdump opt-ins, auth cache refresh/update/consistency/warming controls, cache save/load tuning, CDC block/repair toggles, disk/commitlog/flush compression modes, internode compression/tracing TTLs, corrupted tombstone strategy, GC thresholds, max value size, keyspace RF defaults, automatic SSTable upgrade, repaired-data tracking, dynamic masking, client-error exclusions, audit/FQL archive options, feature flags, storage compatibility, compression dictionary settings, and compaction validators/builders parsed/validated/exposed; repository `conf/cassandra.yaml` now deserializes and validates; remaining optional upstream catalog and cross-field Java behavior remain |

## Distributed Operations

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Gossip & Failure Detection | `partial` | cassandra-cluster-metadata | P1 | Wire format not binary-compatible |
| Snitches & Topology | `partial` | cassandra-cluster-metadata | P1 | Production config integration, cloud metadata refresh edge cases |
| Dynamic Snitch | `partial` | cassandra-cluster-metadata | P1 | Gossip severity feed, latency probe wiring, subsnitch lifecycle parity |
| Partitioner & Token Ring | `done` | cassandra-cluster-metadata | P1 | Murmur3, Random, ByteOrdered, Local partitioners implemented |
| Internode Messaging | `partial` | cassandra-messaging | P1 | Complete Java wire compatibility, persistent connection reuse under load |
| Read Coordinator | `partial` | cassandra-coordinator | P1 | Delayed speculative scheduling, short-read retry integration, AbstractReadExecutor edge cases |
| Read Repair | `partial` | cassandra-coordinator | P1 | AbstractReadExecutor speculative retry, short-read, and failure edge cases |
| Write Coordinator | `partial` | cassandra-coordinator | P1 | Java write-stage scheduling, failure classification, MV worker lifecycle |
| Paging | `partial` | cassandra-coordinator | P1 | Full Java distributed execution parity |
| Coordinator Thresholds | `partial` | cassandra-coordinator | P2 | Full warnings catalog, wiring through every execution path |
| Service Layer | `partial` | cassandra-coordinator | P0 | Bootstrap, token migration |
| Disk Management | `partial` | cassandra-admin | P2 | Filesystem capacity provider integration, disk failure policies, JMX/admin wiring |
| Snapshot Management | `partial` | cassandra-admin | P2 | Full SnapshotManager lifecycle and nodetool parity |
| Paxos / LWT | `partial` | cassandra-coordinator | P2 | system.paxos persistence, PaxosV2 |
| Repair & Anti-Entropy | `partial` | cassandra-repair | P2 | Live internode orchestration, durable repair metadata, Java protocol parity |
| Repair Messages | `partial` | cassandra-repair | P2 | Internode verb wiring, Java protocol compatibility |
| Asymmetric Repair | `partial` | cassandra-repair | P2 | Full Java DifferenceHolder/RangeMap semantics, streaming execution integration |
| Consistent Repair | `partial` | cassandra-repair | P2 | Durable persistence, cluster transport integration |
| Streaming | `partial` | cassandra-streaming | P2 | Java stream protocol compatibility, restartable transfers, failure semantics |
| Streaming Management | `partial` | cassandra-streaming | P2 | JMX StreamStateCompositeData |
| Streaming Messages | `partial` | cassandra-streaming | P2 | Java binary wire compatibility |
| Service Consensus Sub-packages | `experimental` | -- | P3 | Experimental |

## Indexes

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Secondary Index Framework | `partial` | cassandra-storage | P2 | Lifecycle management |
| Built-in Indexes | `partial` | cassandra-storage | P2 | Composites, keys, values sub-types |
| SAI | `partial` | cassandra-storage | P1 | On-disk persistence, HNSW/IVF ANN |
| SAI Disk Format | `partial` | cassandra-storage | P1 | v1-v5 component families, vector persistence, checksum footer parity |
| SAI Query Planning | `partial` | cassandra-storage | P1 | QueryController, Expression |
| SAI Analyzers | `partial` | cassandra-storage | P2 | Full Lucene analyzer parity, complete filter catalog |
| SASI | `baseline-excluded` | -- | deferred | Deprecated in favor of SAI |
| Accord Index Integration | `experimental` | -- | P3 | Experimental |

## Security

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Authentication | `partial` | cassandra-security | P1 | Production external-provider integration |
| Auth Persistent Stores | `done` | cassandra-security | P1 | Persistent role/permission/identity/network stores implemented |
| Authorization | `partial` | cassandra-security | P1 | Full Cassandra system_auth table compatibility |
| TLS/SSL | `partial` | cassandra-security | P1 | Not wired to listeners |
| Audit Logging | `partial` | cassandra-security | P2 | Syslog sink, BinAuditLogger |
| Full Query Logging | `done` | cassandra-security | P2 | File-based FQL implemented and server-wired |

## Hints & Batchlog

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Hinted Handoff | `partial` | cassandra-coordinator | P1 | Java segment binary compatibility, production topology event wiring |
| Hints File Persistence | `partial` | cassandra-coordinator | P1 | Java segment binary compatibility, tooling |
| Batchlog | `partial` | cassandra-coordinator | P1 | Java binary compatibility and production cluster scheduling |

## Observability & Tooling

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Metrics | `partial` | cassandra-admin | P1 | Full JMX metric coverage |
| Distributed Tracing | `partial` | cassandra-coordinator | P2 | Persisted system_traces storage, full cross-node propagation |
| Diagnostics Events | `partial` | cassandra-admin | P3 | Full event catalog, cluster-wide broadcast |
| Notifications | `partial` | cassandra-storage | P3 | Common INotification hierarchy, full type catalog |
| nodetool CLI | `partial` | cassandra-tools | P2 | Complete upstream command catalog and Java output parity |
| Nodetool Output Formatters | `partial` | cassandra-tools | P2 | Full formatter catalog, adoption by all commands |
| Nodetool Stats Commands | `partial` | cassandra-tools | P2 | Java StatsTable/StatsPrinter formatting and cfstats/tablestats output parity |
| SSTable Offline Tools | `partial` | cassandra-tools | P2 | Java output compatibility, remaining niche tools |
| HTTP Admin API | `partial` | cassandra-admin | P2 | Rust-only feature |

## Common / Utilities

| Feature | Status | Crate | Criticality | Gaps |
|---------|--------|-------|-------------|------|
| Utilities | `partial` | cassandra-common | P0 | ByteBufferUtil, UUIDGen timeuuid, and FBUtilities-style helpers implemented; full utility catalog remains |
| Bloom Filter | `done` | cassandra-common | P0 | Java FilterFactory sizing and native OffHeapBitSet backend implemented |
| Memory Management | `partial` | cassandra-common | P1 | Strict off-heap arenas, cleaner lifecycle, Java allocator hierarchy |
| OffHeap BitSet | `done` | cassandra-common | P2 | Native OffHeapBitSet backend and size accounting implemented |
| Concurrent Utilities | `partial` | cassandra-common | P1 | Transactional hierarchy, Java edge cases |
| Streaming Histograms | `partial` | cassandra-common | P2 | Java SSTable statistics serialization integration |
| ByteComparable & Tries | `partial` | cassandra-types | P1 | ByteSource cursors and separators implemented; Java trie encoding compatibility remains |
| Type Serializers | `done` | cassandra-types | P0 | Duration and core serializers covered |
| Change Data Capture | `partial` | cassandra-storage | P2 | CDCCommitLogSegmentAllocator parity, streaming consumer APIs |
| Harry Testing Framework | `baseline-excluded` | -- | deferred | Java-only testing framework |
| Concurrency Primitives | `partial` | cassandra-common | P1 | SEPExecutor/SharedExecutorPool parity |
| Exception Types | `done` | cassandra-common | P0 | Java exception class/category mapping implemented |

## Trunk-Only / Experimental

| Feature | Status | Crate | Criticality | Condition |
|---------|--------|-------|-------------|-----------|
| TCM | `trunk-only` | -- | P3 | `#[cfg(feature = "trunk_only")]` |
| Accord | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Consensus Service | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Service Consensus Sub-pkgs | `experimental` | -- | P3 | `#[cfg(feature = "experimental")]` |
| Journal | `trunk-only` | -- | P3 | `#[cfg(feature = "trunk_only")]` |
| Profiler | `baseline-excluded` | -- | deferred | Dev-time only |
| Triggers | `partial` | cassandra-storage | P3 | WASM/scripting or Java-class plugin loader |
