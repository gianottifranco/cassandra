# Gap Ownership Matrix

Full traceability from every identified gap to its owning prompt, crate, test type, and status.

Sources: `final_gap_matrix.yaml`, `INPUT_full_gap_analysis.md`, `06_todos_stubs_gapguards_a_prompts.md`

## Feature Gaps (63 items from gap matrix)

| gap_id | Name | Java Package(s) | Rust Crate | Prompt | Test Type | Crit | Status |
|--------|------|-----------------|------------|--------|-----------|------|--------|
| type-system | CQL Type System | db.marshal, serializers | cassandra-types | 02 | unit | P0 | partial |
| cql-parser | CQL Parser | cql3 | cassandra-cql | 03 | unit, golden | P0 | partial |
| cql-conditions | CQL Conditions (LWT IF) | cql3.conditions | cassandra-cql | 07 | unit | P1 | partial |
| cql-constraints | CQL Constraints | cql3.constraints | cassandra-cql | 12 | — | P2 | missing |
| cql-functions | CQL Functions (builtins, UDF, UDA) | cql3.functions | cassandra-cql | 12 | — | P1 | missing |
| cql-restrictions | CQL Restrictions | cql3.restrictions | cassandra-cql | 03 | unit | P0 | partial |
| cql-selection | CQL Selection & Projection | cql3.selection | cassandra-cql | 03 | unit | P0 | partial |
| cql-statements | CQL Statements | cql3.statements | cassandra-cql | 03 | unit | P0 | partial |
| cql-terms | CQL Terms & Literals | cql3.terms | cassandra-cql | 03 | unit | P0 | partial |
| cql-transactions | CQL Accord Transactions | cql3.transactions | — | deferred | — | P3 | experimental |
| prepared-statements | Prepared Statement Cache | cql3 | cassandra-cql | 03 | unit | P0 | partial |
| memtable | Memtable | db.memtable, db | cassandra-storage | 04 | unit, integration | P0 | partial |
| commitlog | CommitLog | db.commitlog | cassandra-storage | 04 | unit | P0 | partial |
| sstable-read | SSTable Read Path | io, db | cassandra-storage | 04 | unit, integration | P0 | partial |
| sstable-write | SSTable Write Path | io | cassandra-storage | 04 | unit | P0 | partial |
| compaction | Compaction | db.compaction | cassandra-storage | 07 | unit | P0 | stub |
| compression | Compression | db.compression | cassandra-storage | 04 | unit | P1 | partial |
| db-context | Counter Context | db.context | cassandra-storage | 07 | unit | P1 | partial |
| db-filter | Row/Partition Filters | db.filter | cassandra-storage | 12 | — | P0 | stub |
| db-guardrails | Guardrails | db.guardrails | cassandra-config | 12 | — | P2 | missing |
| db-lifecycle | SSTable Lifecycle | db.lifecycle | cassandra-storage | 12 | — | P1 | stub |
| db-partitions | Partition Iterators | db.partitions | cassandra-storage | 04 | unit | P0 | partial |
| db-rows | Row Representation | db.rows | cassandra-storage | 04 | unit | P0 | partial |
| db-streaming | DB-level Streaming | db.streaming | cassandra-streaming | 06 | — | P2 | stub |
| db-transform | Row Transformations | db.transform | cassandra-storage | 12 | — | P1 | missing |
| db-tries | Trie Index | db.tries | cassandra-storage | 12 | — | P1 | missing |
| db-monitoring | Query Monitoring | db.monitoring | cassandra-admin | 12 | — | P2 | missing |
| db-repair | DB Repair Metadata | db.repair | cassandra-repair | 06 | — | P2 | stub |
| db-aggregation | Query Aggregation | db.aggregation | cassandra-coordinator | 12 | — | P1 | missing |
| db-view | Materialized Views | db.view | — | deferred | — | deferred | baseline-excluded |
| db-virtual | Virtual Tables Framework | db.virtual | cassandra-admin | 08 | unit | P2 | partial |
| cache | Caching (Key, Row, Counter) | cache | cassandra-storage | 12 | — | P1 | missing |
| io | I/O Layer (SSTable I/O) | io | cassandra-storage | 04 | unit | P0 | partial |
| native-protocol | CQL Native Protocol v4/v5 | transport | cassandra-native-protocol | 02 | unit | P0 | stub |
| schema | Schema Catalog | schema | cassandra-schema | 03 | unit | P0 | partial |
| config | Configuration (cassandra.yaml) | config | cassandra-config | 02 | unit | P0 | stub |
| cluster-metadata | Gossip & Failure Detection | gms | cassandra-cluster-metadata | 05 | unit | P1 | partial |
| snitches | Snitches & Topology | locator | cassandra-cluster-metadata | 05 | unit | P1 | partial |
| partitioner | Partitioner & Token Ring | dht | cassandra-cluster-metadata | 05 | unit | P1 | partial |
| messaging | Internode Messaging | net | cassandra-messaging | 05 | unit | P1 | stub |
| coordinator-read | Read Coordinator | service.reads | cassandra-coordinator | 05 | unit | P1 | partial |
| coordinator-write | Write Coordinator | service.writes | cassandra-coordinator | 05 | unit | P1 | partial |
| coordinator-pager | Paging | service.pager | cassandra-coordinator | 12 | — | P1 | missing |
| coordinator-thresholds | Coordinator Thresholds | service.thresholds | cassandra-coordinator | 12 | — | P2 | missing |
| service-core | Service Layer (StorageProxy) | service | cassandra-coordinator | 05 | unit | P0 | partial |
| service-disk | Disk Management | service.disk | cassandra-admin | 12 | — | P2 | missing |
| service-snapshot | Snapshot Management | service.snapshot | cassandra-admin | 12 | — | P2 | stub |
| paxos | Paxos / LWT | service.paxos | cassandra-coordinator | 07 | unit | P2 | partial |
| repair | Repair & Anti-Entropy | repair | cassandra-repair | 06 | unit | P2 | stub |
| streaming | Streaming | streaming | cassandra-streaming | 06 | unit | P2 | stub |
| index-framework | Secondary Index Framework | index | cassandra-storage | 07 | unit | P2 | partial |
| index-internal | Built-in (Internal) Indexes | index.internal | cassandra-storage | 07 | unit | P2 | partial |
| index-sai | Storage Attached Indexes (SAI) | index.sai | cassandra-storage | 07 | unit | P1 | partial |
| index-sasi | SASI Indexes | index.sasi | — | deferred | — | deferred | baseline-excluded |
| index-accord | Accord Index Integration | index.accord | — | deferred | — | P3 | experimental |
| hints | Hinted Handoff | hints | cassandra-coordinator | 06 | unit | P1 | stub |
| batchlog | Batchlog | batchlog | cassandra-coordinator | 12 | unit | P1 | stub |
| auth | Authentication | auth | cassandra-security | 08 | unit | P1 | partial |
| authz | Authorization | auth | cassandra-security | 08 | unit | P1 | partial |
| security-tls | TLS/SSL | security | cassandra-security | 08 | unit | P1 | partial |
| audit | Audit Logging | audit | cassandra-security | 08 | unit | P2 | partial |
| fql | Full Query Logging | fql | cassandra-security | 12 | — | P2 | missing |
| metrics | Metrics | metrics | cassandra-admin | 08 | unit | P1 | partial |
| tracing | Distributed Tracing | tracing | cassandra-coordinator | 12 | — | P2 | stub |
| diag | Diagnostics Events | diag | cassandra-admin | 12 | — | P3 | missing |
| notifications | Notifications | notifications | cassandra-common | 12 | — | P3 | missing |
| nodetool | nodetool CLI | tools, tools.nodetool | cassandra-tools | 08 | unit | P2 | stub |
| sstable-tools | SSTable Offline Tools | tools | cassandra-tools | 12 | — | P2 | stub |
| admin-http | HTTP Admin API | — | cassandra-admin | 08 | unit | P2 | partial |
| utils | Utilities | utils | cassandra-common | 02 | unit | P0 | stub |
| concurrent | Concurrency Primitives | concurrent | cassandra-common | 12 | — | P1 | missing |
| exceptions | Exception Types | exceptions | cassandra-common | 02 | unit | P0 | partial |
| tcm | Transactional Cluster Metadata | tcm | — | deferred | — | P3 | trunk-only |
| accord | Accord Distributed Transactions | service.accord | — | deferred | — | P3 | experimental |
| consensus | Consensus Service | service.consensus | — | deferred | — | P3 | experimental |
| journal | Journal | journal | — | deferred | — | P3 | trunk-only |
| profiler | Profiler | profiler | — | deferred | — | deferred | baseline-excluded |
| triggers | Triggers | triggers | cassandra-storage | deferred | — | P3 | stub |

## TODOs by Crate (42 total)

| Crate | Count | Key Items | Owning Prompts |
|-------|-------|-----------|----------------|
| cassandra-server | 5 | AlterTable no-op, TLS config, schema passing, Row results mapping, role_permissions | 01, 02, 14, 24 |
| cassandra-tools | 15 | Admin API calls (snapshot, compact, sstableloader, audit, FQL, load gen, bootstrap) | 24, 26 |
| cassandra-storage | 11 | SASI memtable ops, SAI gaps, legacy index persistence, MV backfill, MV WHERE/PK | 05, 09, 10, 11, 18 |
| cassandra-security | 3 | Persist permissions, persist roles, SHA-256 masking | 14, 15, 19 |
| cassandra-cql | 2 | Tuple type parsing, column validation | 02, 03, 04 |
| cassandra-cluster-metadata | 1 | Transient replication | 07, 21, 23 |
| cassandra-coordinator | 4 | Read repair blocking, latency percentiles, READ_DATA replicas, repair tracking | 06, 13, 22 |
| cassandra-native-protocol | 2 | Real credential verification | 01, 02, 14 |
| cassandra-migration | 1 | Full binary SSTable conversion | 11, 26 |
| cassandra-common | 2 | Module expansion, murmur3 diff-test | 25 |
| cassandra-diff-tests | 1 | Wire to QueryExecutor | 26 |

## Stubs (55+)

| Crate | Notable Stubs | Owning Prompts |
|-------|---------------|----------------|
| cassandra-server | Auth stubs, executor stubs, result mapping | 01, 02, 14 |
| cassandra-tools | ~15 nodetool command stubs | 24 |
| cassandra-storage | UDF module (entire), Triggers module (entire) | 18, 19 |
| cassandra-coordinator | Trigger augmentation, Accord routing, consensus router | 06, 13, 22 |
| cassandra-cluster-metadata | Ec2Snitch, GceSnitch, TransientReplication | 23 |
| cassandra-native-protocol | PasswordAuthenticator (accepts any credentials) | 14 |
| cassandra-accord | `execute_transaction` is stub | 22 |

## Gap Guards (27 ignored tests)

| Category | Gaps Tracked | Owning Prompts |
|----------|-------------|----------------|
| CQL | Functions, permission statements, constraints, advanced restrictions, index diff testing | 02, 03, 04 |
| Storage | Java SSTable compat, compaction execution, trie index, DB filters, caching, row transforms, guardrails | 05, 09, 10, 11, 20 |
| Distributed | Paging, aggregation, gossip wire compat, internode wire compat | 06, 07, 08, 12, 13, 21, 22, 23 |
| Security | FQL, LDAP/Kerberos auth | 14, 15, 19 |
| Tooling | ~140 nodetool commands, SSTable offline tools, ~60 virtual tables | 19, 24 |
| Trunk-only | TCM, Accord, consensus, journal | 09, 21, 22 |
| Infrastructure | Concurrency stages, tracing storage | 25, 26 |
