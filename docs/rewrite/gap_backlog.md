# Gap Backlog — Master Classification

All gaps classified by blocking tier with closure criteria and owning prompt.

## Summary

| Classification | Count | Description |
|----------------|-------|-------------|
| Startup Blocker (P0) | 7 | Must work before any client can connect |
| Beta Blocker (P1) | 11 | Must work before beta deployment |
| GA Blocker (P2) | 12 | Must work before production release |
| Tech Debt (non-blocking) | 6+ | Known gaps that don't block milestones |
| Accepted Divergence | 5+ | Deferred, excluded, or experimental items |
| TODOs | 42 | In-code TODO markers across 11 crates |
| Stubs | 55+ | API-only implementations across 7 crates |
| Gap Guards | 27 | Ignored tests marking known feature gaps |

---

## Startup Blockers (P0) — 7 gaps

These block any client connectivity. Phase A (Prompts 00–10).

### SB-1: No TCP listener for native protocol
- **Gap ID**: native-protocol (partial: frame codec exists)
- **Prompt**: 01
- **Closure criteria**: Rust server accepts CQL native protocol v4/v5 connections on TCP port
- **Evidence required**: Integration test connecting a CQL driver

### SB-2: No QueryProcessor
- **Gap ID**: cql-statements, cql-parser, cql-selection, cql-restrictions, cql-terms, prepared-statements
- **Prompt**: 02, 03
- **Closure criteria**: CQL statements parsed, validated, and executed through a central QueryProcessor
- **Evidence required**: Golden tests for SELECT, INSERT, UPDATE, DELETE

### SB-3: No StorageProxy
- **Gap ID**: service-core
- **Prompt**: 06
- **Closure criteria**: Coordinated read/write path with consistency level enforcement
- **Evidence required**: Integration tests with CL.ONE, CL.QUORUM

### SB-4: No gossip loop
- **Gap ID**: cluster-metadata
- **Prompt**: 07
- **Closure criteria**: Periodic gossip rounds with failure detection
- **Evidence required**: Multi-node integration test showing node discovery

### SB-5: Persistent internode connections
- **Gap ID**: messaging
- **Prompt**: 08
- **Closure criteria**: Connection pooling with 3-channel architecture, CRC frames, LZ4 compression
- **Evidence required**: Internode message exchange without new TCP per send

### SB-6: No SSTable compression
- **Gap ID**: compression, io
- **Prompt**: 10
- **Closure criteria**: LZ4, Snappy, Zstd compression for SSTables
- **Evidence required**: Compressed SSTable read/write round-trip tests

### SB-7: No CompactionManager execution
- **Gap ID**: compaction
- **Prompt**: 09
- **Closure criteria**: Compaction strategies execute via CompactionManager
- **Evidence required**: STCS/LCS compaction completing on test data

---

## Beta Blockers (P1) — 11 gaps

Must work before beta deployment. Phase B (Prompts 11–20).

### BB-1: CQL built-in functions
- **Gap ID**: cql-functions
- **Prompt**: 03 (builtins), 18 (UDF/UDA)
- **Closure criteria**: token(), now(), uuid(), cast(), count(), sum(), avg(), min(), max()
- **Evidence required**: Golden tests for function evaluation

### BB-2: WHERE clause validation
- **Gap ID**: cql-restrictions
- **Prompt**: 03
- **Closure criteria**: Multi-column, token(), CONTAINS, LIKE restrictions
- **Evidence required**: Unit tests + golden tests for restriction validation

### BB-3: SELECT evaluation
- **Gap ID**: cql-selection
- **Prompt**: 03
- **Closure criteria**: Function calls in SELECT, aliases, writetime/ttl
- **Evidence required**: Golden tests

### BB-4: marshal/ type system
- **Gap ID**: type-system
- **Prompt**: 04
- **Closure criteria**: All CQL types serialize/deserialize, including frozen nesting
- **Evidence required**: Unit tests + diff tests against Java serialization

### BB-5: IO util layer
- **Gap ID**: io
- **Prompt**: 10
- **Closure criteria**: RandomAccessReader, CompressedSequentialWriter, mmap support
- **Evidence required**: Integration tests with large files

### BB-6: Rows/Partitions iterator framework
- **Gap ID**: db-rows, db-partitions
- **Prompt**: 05
- **Closure criteria**: UnfilteredPartitionIterator, merge iterators, ComplexColumnData
- **Evidence required**: Unit tests with multi-partition scans

### BB-7: Filter subsystem
- **Gap ID**: db-filter
- **Prompt**: 05
- **Closure criteria**: ClusteringIndexFilter, ColumnFilter, RowFilter
- **Evidence required**: Unit tests for slice/names/limit queries

### BB-8: Cache subsystem
- **Gap ID**: cache
- **Prompt**: 20
- **Closure criteria**: KeyCache, RowCache with LRU eviction
- **Evidence required**: Integration tests showing cache hits reduce disk I/O

### BB-9: Auth persistence to system_auth
- **Gap ID**: auth, authz
- **Prompt**: 14
- **Closure criteria**: Credentials and permissions persisted to system_auth tables
- **Evidence required**: Integration test: create user → restart → authenticate

### BB-10: CRC/LZ4 in internode frames
- **Gap ID**: messaging
- **Prompt**: 08
- **Closure criteria**: CRC32 integrity + LZ4 compression on internode frames
- **Evidence required**: Internode test with corruption detection

### BB-11: 3-node cluster validated
- **Gap ID**: (cross-cutting)
- **Prompt**: 26
- **Closure criteria**: 3-node cluster runs real CQL workload
- **Evidence required**: E2E test with reads/writes at CL.QUORUM

---

## GA Blockers (P2) — 12 gaps

Must work before production release. Phase C (Prompts 21–26).

| # | Gap | Gap ID | Prompt | Closure Criteria |
|---|-----|--------|--------|------------------|
| 1 | Consistent repair protocol | repair | 13 | Incremental repair with Merkle tree exchange |
| 2 | Streaming network transport | streaming, db-streaming | 12 | SSTable data transfer between nodes |
| 3 | Paxos/LWT persistence | paxos | 22 | system.paxos table survives restart |
| 4 | TCM commit protocol | tcm | 21 | Linearizable metadata commits |
| 5 | SSTable lifecycle transactions | db-lifecycle | 09 | Crash-safe SSTable transitions |
| 6 | Token allocator | partitioner | 23 | Balanced token distribution on join |
| 7 | Index execution engine (SAI min) | index-sai, index-framework | 16, 17 | SAI query execution on indexed columns |
| 8 | Schema change propagation | schema | 07, 18 | Schema changes propagate via gossip/TCM |
| 9 | Guardrails framework | db-guardrails | 15 | Config-driven safety limits enforced |
| 10 | Virtual tables (system_views) | db-virtual | 19 | Core system_views tables queryable |
| 11 | SSTable format compat / migration | sstable-read | 11, 26 | Read Java SSTables or migrate them |
| 12 | Transform pipeline | db-transform | 05 | DuplicateRowChecker, filter chain |

---

## Tech Debt (non-blocking)

| Item | Gap ID | Prompt | Notes |
|------|--------|--------|-------|
| Full exception hierarchy | exceptions | 25 | ~30 specialized exceptions missing |
| Diagnostics events | diag | 19 | DiagnosticEventService |
| Notifications | notifications | 25 | SSTable/memtable event broadcasting |
| Query monitoring | db-monitoring | 19 | Slow query log |
| Coordinator thresholds | coordinator-thresholds | 25 | Local read/write time tracking |
| Disk management | service-disk | 25 | Disk boundary management |

---

## Accepted Divergence (temporary)

| Item | Gap ID | Rationale |
|------|--------|-----------|
| Materialized Views | db-view | Deprecated upstream |
| SASI Indexes | index-sasi | Deprecated in favor of SAI |
| Profiler | profiler | Dev-time tool, not runtime feature |
| Accord transactions | accord, cql-transactions | Experimental, API still fluid |
| Consensus service | consensus | Experimental |
| Journal | journal | Trunk-only, still evolving |
| Triggers | triggers | Needs WASM/scripting strategy |

---

## TODO Cross-Reference (42 items)

Source: `06_todos_stubs_gapguards_a_prompts.md`

| Crate | Count | Owning Prompts |
|-------|-------|----------------|
| cassandra-server | 5 | 01, 02, 14, 24 |
| cassandra-tools | 15 | 24, 26 |
| cassandra-storage | 11 | 05, 09, 10, 11, 18 |
| cassandra-security | 3 | 14, 15, 19 |
| cassandra-cql | 2 | 02, 03, 04 |
| cassandra-cluster-metadata | 1 | 07, 21, 23 |
| cassandra-coordinator | 4 | 06, 13, 22 |
| cassandra-native-protocol | 2 | 01, 02, 14 |
| cassandra-migration | 1 | 11, 26 |
| cassandra-common | 2 | 25 |
| cassandra-diff-tests | 1 | 26 |

---

## Stub Cross-Reference (55+ items)

| Crate | Notable Stubs | Owning Prompts |
|-------|---------------|----------------|
| cassandra-server | Auth stubs, executor stubs, result mapping | 01, 02, 14 |
| cassandra-tools | ~15 nodetool command stubs | 24 |
| cassandra-storage | UDF module (entire), Triggers module (entire) | 18, 19 |
| cassandra-coordinator | Trigger augmentation, Accord routing, consensus router | 06, 13, 22 |
| cassandra-cluster-metadata | Ec2Snitch, GceSnitch, TransientReplication | 23 |
| cassandra-native-protocol | PasswordAuthenticator (accepts any) | 14 |
| cassandra-accord | execute_transaction is stub | 22 |

---

## Gap Guard Cross-Reference (27 ignored tests)

| Category | Gaps | Owning Prompts |
|----------|------|----------------|
| CQL | Functions, permissions, constraints, restrictions, index diff | 02, 03, 04 |
| Storage | Java SSTable compat, compaction exec, trie index, filters, cache, transforms, guardrails | 05, 09, 10, 11, 20 |
| Distributed | Paging, aggregation, gossip wire compat, internode wire compat | 06, 07, 08, 12, 13, 21, 22, 23 |
| Security | FQL, LDAP/Kerberos | 14, 15, 19 |
| Tooling | ~140 nodetool cmds, SSTable offline tools, ~60 virtual tables | 19, 24 |
| Trunk-only | TCM, Accord, consensus, journal | 09, 21, 22 |
| Infrastructure | Concurrency stages, tracing storage | 25, 26 |
