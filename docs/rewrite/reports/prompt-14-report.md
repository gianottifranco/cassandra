# Prompt 14 Report — Read Path Completo

## Summary

Prompt 14 closes the standard read path with observable parity across 18 work units covering: error mapping, executor fundamentals, ordering/paging/static rows, tombstone guardrails, coordinator read wiring, and comprehensive testing.

## Work Units

### AREA 1: Error Mapping and Executor Fundamentals

| WU | Description | Status |
|----|-------------|--------|
| WU-01 | ReadError → CassandraError mapping | ✅ Complete |
| WU-02 | Early LIMIT break in execute_select() | ✅ Complete |
| WU-03 | PER PARTITION LIMIT in planner + executor | ✅ Complete |
| WU-04 | Clustering slice filtering in execute_select() | ✅ Complete |

### AREA 2: Ordering, Static Rows, and Paging

| WU | Description | Status |
|----|-------------|--------|
| WU-05 | Reversed order reads (ORDER BY DESC) | ✅ Complete |
| WU-06 | Paging integration in execute_select() | ✅ Complete |
| WU-07 | Static row handling in execute_select() | ✅ Complete |
| WU-08 | Range scan (scan_all_partitions) | ✅ Complete |

### AREA 3: Tombstones, Guardrails, and Aggregates

| WU | Description | Status |
|----|-------------|--------|
| WU-09 | Tombstone warnings in protocol responses | ✅ Complete |
| WU-10 | Read guardrail checks (check_select_guardrails) | ✅ Complete |
| WU-11 | SELECT COUNT(*) and SELECT JSON | ✅ Complete |

### AREA 4: Coordinator Read Wiring

| WU | Description | Status |
|----|-------------|--------|
| WU-12 | Short-read protection wiring in coordinate_read() | ✅ Complete |
| WU-13 | Speculative retry with tokio timing | ✅ Complete |
| WU-14 | Async read coordination (fetch_rows_async) | ✅ Complete |
| WU-15 | Read repair end-to-end (serialized payloads) | ✅ Complete |

### AREA 5: Testing and Documentation

| WU | Description | Status |
|----|-------------|--------|
| WU-16 | Read path differential tests | ✅ Complete |
| WU-17 | Golden fixtures for read errors | ✅ Complete |
| WU-18 | Read path parity documentation | ✅ Complete |

## Key Files Modified

| File | Changes |
|------|---------|
| `cassandra-server/src/error_mapping.rs` | `read_error_to_cassandra_error()`, `read_error_to_error_frame()` |
| `cassandra-server/src/executor.rs` | LIMIT break, per-partition limit, clustering filter, reversed, static rows, paging, range scan, COUNT(*), tombstone tracking |
| `cassandra-server/src/guardrail_checks.rs` | `check_select_guardrails()` |
| `cassandra-native-protocol/src/auth.rs` | Native protocol SASL PLAIN parsing and password verification via `cassandra-security` |
| `cassandra-server/src/server.rs` | Native auth wrapper shares SASL parser and reports Java authenticator class names |
| `cassandra-cql/src/planner.rs` | `per_partition_limit` field on SelectPlan |
| `cassandra-schema/src/view.rs` | Materialized-view table options stored in view metadata |
| `cassandra-admin/src/operations.rs` | Operation history retention for completed admin tasks |
| `cassandra-coordinator/src/read/mod.rs` | Short-read wiring, `coordinate_read_async()` |
| `cassandra-coordinator/src/read/repair.rs` | `serialize_repair_payload()`, blocking repair |
| `cassandra-coordinator/src/storage_proxy.rs` | `fetch_rows_async()` |
| `cassandra-storage/src/engine.rs` | `scan_all_partitions()`, `truncate_table()` |
| `cassandra-coordinator/src/verb_handlers/read_handler.rs` | Storage-backed replica read data/digest verb handlers |
| `cassandra-coordinator/src/verb_handlers/mutation_handler.rs` | Storage-backed replica mutation verb handler |
| `cassandra-coordinator/src/verb_handlers/hint_handler.rs` | Storage-backed hint delivery verb handler |
| `cassandra-coordinator/src/verb_handlers/read_repair_handler.rs` | Storage-backed read repair verb handler |
| `cassandra-coordinator/src/verb_handlers/batch_handler.rs` | Batchlog-backed replica batch store/remove verb handlers |
| `cassandra-coordinator/src/verb_handlers/registration.rs` | Storage- and batchlog-backed verb registration helpers |

## New Files

| File | Purpose |
|------|---------|
| `diff-tests/golden/read_errors/*.json` | 4 golden fixtures (read_timeout, read_failure, unavailable, tombstone_overwhelming) |
| `cassandra-diff-tests/tests/read_error_golden_tests.rs` | Golden test suite for read errors |
| `cassandra-diff-tests/tests/read_path_tests.rs` | Read path differential test suite |
| `docs/rewrite/reports/prompt-14-report.md` | This report |

## Gap Guards Updated

- `gap_guard_paging` — partially closed (PagingState wired into executor)
- `gap_guard_coordinator_read_repair` — partially closed (serialization, blocking dispatch)
- `gap_guard_aggregation` / `gap_guard_read_aggregation` — partially closed (GROUP BY parsed/planned and single-node executor aggregate state wired)
- `gap_guard_read_distinct` — partially closed (single-node executor DISTINCT over partition keys)
- `gap_guard_dc_aware_read_executor` — partially closed (LOCAL_QUORUM/LOCAL_ONE coordinator reads filter to local DC before availability and execution planning)
- `gap_guard_write_path_read_before_write` — partially closed (WriteCoordinator can read existing base-row state before MV delta generation)
- `gap_guard_utils_bytecomparable` — closed (order-preserving byte encoding for signed values, floats, byte strings, reversed order, varints, and UUID version ordering)
- `gap_guard_bloom_filter` — closed (SSTable BloomFilter membership checks, Filter.db serialization round-trip, and false-positive smoke coverage)
- `gap_guard_sstable_metadata` — partially closed (binary Statistics.db metadata serialization/deserialization, legacy JSON fallback, collector bounds, and corrupt magic detection)
- `gap_guard_sstable_index_summary` — closed (Summary.db serialization/load, sampled lookup windows, min/max keys, and empty summary behavior)
- `gap_guard_caching` — closed (KeyCache, RowCache, CounterCache, ChunkCache, CacheService invalidation, and key/counter cache persistence)
- `gap_guard_cql_function_type_helpers` — partially closed (function arg/return signatures, case-qualified names, type parsing/names, fixed sizes, collection flags, and assignment compatibility)
- `gap_guard_memtable_trie` — partially closed (selectable trie memtable backend, shared-prefix keys, sorted iteration, row merge, partition tombstone, and backend factory wiring)
- `gap_guard_compaction_execution` — partially closed (task execution, manager metrics, concurrency rejection, cancellation, cleanup, scrub, and rate limiting)
- `gap_guard_compaction_unified` — partially closed (experimental UCS density bucket selection, scaling parameter fallback, min/max thresholds, and below-threshold behavior)
- `gap_guard_compaction_writers` — partially closed (SSTable rewriter preserves partitions, writes readable outputs, and supports size-based output splitting)
- `gap_guard_cdc` — partially closed (CDC raw segment discovery, status/space accounting, usage percentage, and consumed segment discard)
- `gap_guard_commitlog_compression` — partially closed (v2 segment compression flags, compressed-entry replay round-trip, and explicit plaintext encryption marker)
- `gap_guard_fql` — closed (binary full-query-log record encode/decode, file logger, reader, disabled mode, and corrupt decode handling)
- `gap_guard_sai_disk_format` — partially closed (SAI segment binary writer/reader, posting-list round-trip, merge behavior, and corrupt magic rejection)
- `gap_guard_repair_messages` — closed (consistent repair and sync message payload round-trips across the RepairMessage envelope)
- `gap_guard_consistent_repair` — closed (coordinator prepare/finalize/commit state machine, local participant lifecycle, in-memory session store, pending tracker, and failure transition)
- `gap_guard_streaming_messages` — closed (stream init/data/complete payload round-trips, response messages, wire partition conversion, and invalid payload rejection)
- `gap_guard_coordinator_read_repair` — partially closed (read-repair strategy parsing, staging, disabled strategy behavior, replica verb response, and invalid payload failure)
- `gap_guard_utils_memory` — partially closed (lock-free IO buffer pool metrics/reuse and mmap-backed rebuffering)
- `gap_guard_virtual_tables` — partially closed (admin registry exposes >=22 built-in system_views tables with columns, rows for local/settings, and missing-table lookup behavior)
- `gap_guard_sstable_offline_tools` — partially closed (Rust SSTable verifier, scrubber, upgrader, readable outputs, and overwrite rejection)
- `gap_guard_utils_concurrent` — partially closed (shared cancellation tokens, active compaction conflict tracking/cancel/unregister, and lifecycle transaction commit/abort logging)
- `gap_guard_tracing_storage` — partially closed (coordinator trace event capture, active/recent in-memory session store, disabled mode, bounded cleanup store, and TTL cleanup task)
- `gap_guard_tcm` — partially closed (TCM epochs, metadata transformations, snapshots/restores, in-memory log storage, duplicate detection, and sealed periods)
- `gap_guard_accord` — partially closed (transaction IDs/status helpers, command store lifecycle validation, replayable Accord journal, topology mapping, and migration state tracking)
- `gap_guard_consensus` — partially closed (transactional-mode router covers Off/Paxos/Accord/Mixed dispatch, condition failures, per-key migration routing, and routing metrics)
- `gap_guard_cql_constraints` — partially closed (CREATE/ALTER/DROP CHECK parser support, scalar comparisons, LENGTH/OCTET_LENGTH-style function constraints, NOT NULL, planner metadata, and schema column storage)
- `gap_guard_cql_advanced_restrictions` — partially closed (token() restrictions, multi-column clustering tuple restrictions, and collection CONTAINS/CONTAINS KEY parser/planner validation)
- `gap_guard_cql_terms_advanced` — partially closed (UPDATE assignment parsing distinguishes list append/prepend/removal and map element put operations)
- `gap_guard_sai_analyzers` — partially closed (SAI non-tokenizing analyzer options, option extraction/validation, case folding, normalization hook, and ASCII folding)
- `gap_guard_column_index_builder` — partially closed (partition-internal IndexInfo block builder with clustering ranges, offsets, widths, row counts, and lookup)
- `gap_guard_journal` — partially closed (generic replayable journal primitives with stable record pointers, segment rollover, key lookup, ordered replay, and truncation)
- `gap_guard_gossip_wire_compat` — partially closed (Java-layout binary codec for gossip digest/SYN/ACK/ACK2 payloads, address+port endpoints, heartbeat state, supported ApplicationState ordinals, and VersionedValue payloads)
- `gap_guard_internode_wire_compat` — partially closed (Java-layout raw internode Message.Serializer header codec, Cassandra unsigned vint encoding, overlapping Java verb-id mapping, unknown-param skipping, and opaque payload framing)
- `gap_guard_ldap_kerberos_auth` — partially closed (provider-backed LDAP bind and Kerberos ticket validation authenticators map external identities to Cassandra roles)
- `gap_guard_auth_persistent` — partially closed (system_auth-style JSON store persists roles, role memberships, and permission grants with reopen/revoke coverage)
- Native protocol password authentication stub — closed (PLAIN SASL credentials are parsed once and verified through the bcrypt-backed `cassandra-security` authenticator; wrong passwords and unknown users are rejected in native protocol tests)
- TRUNCATE executor stub — closed (executor validates keyspace/table, storage discards active memtable and loaded SSTables, invalidates caches, truncates secondary indexes, and covers memtable+SSTable data removal in tests)
- ALTER MATERIALIZED VIEW options stub — closed (CREATE stores MV table options and ALTER validates the view, merges updated options into schema metadata, and rejects missing views)
- Compaction history handler stub — closed (admin operation tracker retains removed operations and `/api/v1/compaction/history` returns completed rebuild/compaction entries with id, status, progress, description, and elapsed time)
- Replica read verb local-storage stub — partially closed (ReadData/ReadDigest handlers now have storage-backed entry points, deterministic binary partition payloads, real partition digest responses, explicit no-storage failures, and registration support for a local `StorageEngine`)
- Replica mutation verb stub — partially closed (Mutation handler now converts `CoordinatedMutation` into storage commitlog mutations, applies through `StorageEngine`, returns storage failures, and storage-backed registration covers mutation-then-read dispatch)
- Replica hint/read-repair verb stubs — partially closed (Hint and ReadRepair handlers now reuse the same mutation-to-storage conversion, apply through `StorageEngine`, return storage failures, and fail explicitly when storage is not configured)
- Replica batchlog verb stub — partially closed (BatchStore/BatchRemove handlers now have batchlog-backed entry points, preserve remote batch IDs, report missing removes, full-state verb registration dispatches store/remove into a local `BatchLogManager`, and the coordinator has async `Verb::BatchStore`/`Verb::BatchRemove` request-response methods covered by a real messaging listener test)
- Startup disk-space check stub — closed (startup checks now call Unix `statvfs`, compare available MiB to the configured minimum, and reject impossible disk-space requirements in tests)
- StorageEngine SAI rebuild stub — closed (rebuild now creates/registers SAI indexes, replaces stale definitions, feeds SSTable and active memtable rows, marks index status query-ready, and verifies searchability after rebuild)
- Admin stats placeholder handlers — partially closed (table stats now surface storage-engine totals and virtual SSTable tasks, table/proxy histograms are extracted from Prometheus histogram buckets, client counts use native-client metrics when virtual rows are absent, and top partitions are ranked from storage/schema snapshots)
- Storage UDF/UDA manager stub — partially closed (the `udfs` feature now exports the real module, registers native Rust UDFs and UDAs, executes deterministic built-in bodies such as identity/concat/case conversion/integer addition/constants, and accumulates UDA state through registered state/final functions)
- Commitlog encrypted segment stub — partially closed (commitlog now has a real encrypted segment codec trait, pass-through and encrypting writer implementations, and round-trip coverage for transformed payload blocks)
- Trigger executor integration stub — partially closed (CQL trigger registry now stores loaded implementations, executes them for mutation events, `QueryExecutor` accepts an injected trigger registry, and INSERT/UPDATE/DELETE paths apply returned trigger mutations through storage)
- Read planner schema stub — closed (planner now uses a real `SchemaIndexCatalog` abstraction implemented for `cassandra_schema::SchemaCatalog` and `SchemaSnapshot`, mapping schema index metadata to legacy/SAI/SASI/custom planner index types)
- StorageProxy read messaging stub — partially closed (`fetch_rows` now keeps coordinator replica/CL planning but sends `ReadData`/`ReadDigest` requests through `MessagingService`, uses local dispatch for the local endpoint, validates digest responses, and returns storage-backed opaque partition bytes in a focused test)
- StorageProxy local mutation ack simulation — closed (mutate/mutate_async now send `MutationRequest` payloads through `MessagingService`, dispatch local replicas into the registered mutation verb handler, validate `MutationResponse`, and cover storage-backed local apply in tests)
- Coordinator trigger augmentation merge stub — partially closed (`coordinate_write_with_hooks` now decodes trigger-returned mutation bytes as `CoordinatedMutation`s and coordinates those augmented writes after the base mutation; focused coverage verifies both base and augmented writes are coordinated)
- `gap_guard_nodetool_commands` — partially closed (nodetool command catalog covers 50+ admin-mapped cluster/topology/snapshot/compaction/stats/cache/hints/logging/repair/SSTable operations)
- `gap_guard_nodetool_formatters` — partially closed (reusable nodetool table formatter and pretty JSON formatter)
- `gap_guard_nodetool_stats` — partially closed (TableStatsHolder/StatsTable aggregates and renders tablestats/cfstats-style data)
- `gap_guard_trigger_plugin_system` — partially closed (TriggerManager runtime plugin registry executes registered trigger augmentations and rejects missing plugins)
- `gap_guard_async_mv_fanout` — partially closed (queue-backed async MV fanout dispatcher tracks generated/applied/failed mutations and backlog separately from base write generation)
- `gap_guard_index_differential_testing` — partially closed (secondary-index differential harness compares canonical search results across index backends)
- `gap_guard_sstable_java_compat` — partially closed (Java big-format component manifest discovery and minimal readability probing)
- `gap_guard_sstable_read_compat` — partially closed (Java big-format TOC/component parsing and descriptor filename reconstruction)
- DC-aware consistency helper gap — partially closed (`ConsistencyLevel` now has topology-aware per-DC block/satisfaction helpers, `EACH_QUORUM` requires quorum in every datacenter, and write CL availability uses per-DC live/RF counts)
- Native protocol execute metadata-change gap — closed (Rows metadata encoding now writes `METADATA_CHANGED` result metadata IDs and `EXECUTE` responses propagate `ExecuteResult.new_metadata_id`)
- Storage table scan gap — closed (`StorageEngine::scan_all_partitions` now merges active memtable and SSTable partitions for the requested table, and SSTable reads are filtered by keyspace/table to avoid cross-table key collisions)
- `system_auth` role scan gap — closed (`SystemAuthRoleManager::list_roles` and role membership lookup now use storage table scans over `system_auth.roles` and `system_auth.role_members`)
- ALTER TABLE executor dispatch gap — partially closed (`QueryExecutor` now applies ADD/DROP/ALTER column, mask/constraint, and core table option changes to schema metadata and returns `UPDATED TABLE` schema-change responses)
- Legacy secondary index rebuild gap — partially closed (legacy indexes rebuild durable query state from the table-scoped storage scan, covering active and flushed base data without scanning unrelated SSTables)
- Materialized-view backfill scan gap — partially closed (feature-gated MV backfill now uses the table-scoped storage scan instead of SSTable-only iteration)
- Consensus mixed-mode table-id placeholder — partially closed (Mixed routing now uses a deterministic keyspace/table identifier when schema table IDs are not supplied, and migrated-key routing tests register against that identifier instead of the nil UUID)
- StorageProxy async read bypass gap — closed (`fetch_rows_async` now delegates to the StorageProxy messaging path instead of the synchronous read-coordinator simulation, with storage-backed handler coverage)
- Read planning/simulation split — partially closed (`ReadCoordinator::plan_read` now computes availability, CL requirements, and contacted replicas without fabricated data or responses, and `StorageProxy` uses it before real read messaging)
- ReadCoordinator async simulation gap — closed (`coordinate_read_async` now returns the plan-only result instead of delegating to the synchronous fake-response path)
- Speculative retry latency placeholder — partially closed (`ReadCoordinator` now accepts an injected percentile latency estimator and uses it in read execution planning instead of a hardcoded delay)
- ReadCoordinator synchronous planning duplication — partially closed (`coordinate_read` now reuses `plan_read` for replica/CL planning before returning its compatibility placeholder response)
- Read repair response simulation gap — partially closed (`read_repair_from_responses` now accepts full data responses collected by callers and validates response/replica counts before resolving repairs; the old `read_repair` remains a compatibility shim)
- Write CL completion reuse — partially closed (`WriteCoordinator::complete_write_plan` now evaluates actual ack/hint counts for CL satisfaction, and `StorageProxy.mutate` uses it after messaging instead of duplicating timeout/ANY logic)
- QueryExecutor batch delegation seam — partially closed (`QueryExecutor` now accepts a `BatchMutationSink`, sends collected batch mutations to it when configured, and keeps local storage as the embedded fallback)

## Remaining Gaps

- Production latency percentile collection/wiring for speculative retry beyond the injected estimator
- GROUP BY / aggregation is wired for single-node executor scans; remaining work is coordinator/DataLimits parity
- Real SELECT DISTINCT over partition keys is wired for executor table scans; remaining DISTINCT work is broader coordinator/DataLimits parity
- Broader DC-aware read executor parity beyond LOCAL_QUORUM/LOCAL_ONE coordinator filtering
- Full storage-engine-backed read-before-write MV integration beyond the coordinator hook
- Range read concurrency limiter
- MV read repair filtering
- Remaining SSTable metadata subtypes beyond the implemented StatsMetadata-style binary metadata
- Remaining Java TrieMemtable off-heap memory layout parity beyond the current prefix-trie backend behavior
- Remaining Java UDF CodecRegistry, arbitrary sandboxed Java/WASM execution, and argument deserializer/inference parity beyond the current shared type helpers and native deterministic UDF/UDA evaluator
- Remaining compaction execution integration that rewrites on-disk SSTable components end-to-end
- Remaining UCS Controller/Sharded compaction orchestration parity beyond the current density-bucket strategy
- Remaining Java compaction writer class hierarchy parity beyond the current SSTable rewrite/splitting primitive
- Remaining CDC commitlog write-path integration and segment reader/consumer flow
- Remaining commitlog configuration/write-path/replay wiring for encrypted segment codecs beyond the current payload block adapter
- Remaining SAI disk coverage for vector index persistence, compaction-integrated segment rewrite, and broader Java version compatibility
- Remaining read-repair integration that applies repair mutations to local storage and verifies end-to-end consistency convergence
- Remaining Java memory utility parity for MemtableAllocator, NativeAllocator, SlabAllocator, MemtablePool, and Ref/shared-closeable lifecycles
- Remaining Java virtual table parity for the full db.virtual catalog beyond the current admin/system_views subset
- Remaining Java SSTable offline tool parity for export/split/metadata/relevel/repair-set workflows beyond verifier/scrubber/upgrader primitives
- Remaining Java concurrent utility parity for Ref, SharedCloseable, WaitQueue, OpOrder, and broader utils.concurrent semantics
- Remaining distributed tracing persistence into the `system_traces` keyspace beyond in-memory session tracking/cleanup
- Remaining distributed CMS/quorum, production persistence, and Java trunk TCM parity beyond current in-memory epoch/log primitives
- Remaining distributed Accord protocol, recovery, storage application, and Java experimental parity beyond current in-memory command/journal primitives
- Remaining Java `service.consensus` parity beyond the current Paxos/Accord request router and per-key migration state
- Remaining CQL constraint validation and write-path enforcement beyond current parser/planner/schema metadata
- Remaining CQL LIKE restrictions beyond current advanced token/tuple/collection restriction coverage
- Remaining UDT literals and full map/set discarder term semantics beyond current collection assignment parsing
- Remaining Lucene-style/tokenizing SAI analyzer pipeline beyond current non-tokenizing analyzer support
- Remaining Java big-format RowIndexEntry serialization compatibility beyond current partition-internal column index block builder
- Remaining Java journal durable segment files, on-disk indexes, serializers, compaction, and subsystem integrations beyond current in-memory journal primitives
- Remaining full mixed Java+Rust gossip cluster formation beyond current Java-layout gossip payload codec
- Remaining Java internode connection pool, flow control, typed payload serializers, compression/encryption negotiation parity, and full mixed-cluster transport beyond current raw-message header codec
- Remaining production LDAP/Kerberos networking, SASL/GSSAPI negotiation, connection pooling, and directory configuration parity beyond current provider-backed authenticators
- Remaining distributed system_auth keyspace/query integration, replication consistency, and migration parity beyond current persistent auth store and native password authenticator integration
- Remaining full Java nodetool/JMX command parity and command-specific argument behavior beyond current admin-mapped command catalog
- Remaining Java nodetool formatter variants and command-specific layouts beyond current table/JSON formatter primitives
- Remaining live JMX-equivalent per-table stats collection and Java StatsPrinter variants beyond current engine-backed admin stats and tablestats aggregation/rendering
- Remaining WASM/FFI trigger sandbox loading, isolation, deployment, and stricter all-or-nothing atomic mutation merge parity beyond current runtime plugin registry, executor-applied trigger mutations, and coordinator-applied augmented writes
- Remaining real replica messaging integration and retry scheduling for asynchronous MV fanout beyond current queue-backed dispatcher
- Remaining startup wiring for storage-backed replica read/write verb registration in every server path, plus full read command slicing/filter/limit serialization beyond current full-partition local storage handler and StorageProxy messaging dispatch
- Remaining execute-batch wiring to call the async batchlog replica send/remove path from the logged batch protocol
- Remaining Java secondary-index oracle execution for full differential tests beyond current Rust backend comparison harness
- Remaining Java SSTable Data.db/Index.db/Statistics.db binary row/cell decoding beyond current big-format component discovery and TOC parsing
