# Análisis Completo de Gaps: Java Cassandra vs Rust Rewrite

**Fecha**: 2026-03-16
**Baseline Java**: `src/java/org/apache/cassandra/` (3,168 archivos)
**Rust Rewrite**: `rust/crates/` (209 archivos, ~75,000 LOC)
**Método**: Comparación directa archivo por archivo contra el source de Java

---

## Tabla de Contenidos

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Storage / DB](#2-storage--db)
3. [CQL](#3-cql)
4. [Transport (Protocolo Nativo)](#4-transport-protocolo-nativo)
5. [Net (Inter-nodo)](#5-net-inter-nodo)
6. [Cluster / Gossip / DHT / Locator](#6-cluster--gossip--dht--locator)
7. [Service Layer](#7-service-layer)
8. [TCM (Transactional Cluster Metadata)](#8-tcm-transactional-cluster-metadata)
9. [IO / SSTable](#9-io--sstable)
10. [Streaming](#10-streaming)
11. [Repair](#11-repair)
12. [Auth](#12-auth)
13. [Security](#13-security)
14. [Config](#14-config)
15. [Index (Secondary Indexes)](#15-index-secondary-indexes)
16. [Schema](#16-schema)
17. [Tools](#17-tools)
18. [Audit / FQL](#18-audit--fql)
19. [Hints](#19-hints)
20. [Batchlog](#20-batchlog)
21. [Cache](#21-cache)
22. [Metrics](#22-metrics)
23. [Tracing](#23-tracing)
24. [Triggers / Journal / Notifications / Diag](#24-triggers--journal--notifications--diag)
25. [Exceptions](#25-exceptions)
26. [Utils](#26-utils)
27. [Serializers](#27-serializers)
28. [Concurrent](#28-concurrent)
29. [TODOs y Stubs en el Código Rust](#29-todos-y-stubs-en-el-código-rust)
30. [Resumen de Cobertura por Subsistema](#30-resumen-de-cobertura-por-subsistema)
31. [Top 30 Gaps Críticos (Ordenados por Prioridad)](#31-top-30-gaps-críticos-ordenados-por-prioridad)

---

## 1. Resumen Ejecutivo

| Métrica | Valor |
|---------|-------|
| Archivos Java | 3,168 |
| Archivos Rust | 209 |
| LOC Rust estimadas | ~75,000 |
| Features en gap matrix | 63 (0 done, 30 partial, 11 stub, 14 missing) |
| TODOs en código Rust | 42 |
| Stubs en código Rust | 55+ |
| Gap guards (tests ignorados) | 27 |
| Evaluación oficial | **NOT YET RC — Conditional GO for beta** |

**Conclusión**: La arquitectura y los cimientos están bien puestos. Los subsistemas de storage, protocolo, clustering y coordinación tienen implementaciones parciales funcionales. Sin embargo, faltan capas críticas de ejecución (TCP listener, QueryProcessor, StorageProxy, compaction executor, compression, caching) que impiden el uso en producción.

---

## 2. Storage / DB

**Java**: `db/` — ~400+ archivos, ~23 subdirectorios
**Rust**: `cassandra-storage/src/` — 34 archivos

### 2.1 CommitLog

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| Core WAL append + CRC32c | `CommitLog`, `CommitLogSegment` | `commitlog/mod.rs`, `segment.rs` | ✅ Implementado |
| Segment rotation + recycling | 3 clases | Inline | ✅ Implementado |
| CDC dual-write | `CommitLogSegmentManagerCDC` | Inline en `append()` | ✅ Implementado |
| Archiving hooks | `CommitLogArchiver` | Inline | ✅ Implementado |
| LZ4 compression per entry | `CompressedSegment` | Inline | ✅ Implementado |
| Batch/periodic sync | `BatchCommitLogService`, `PeriodicCommitLogService` | Config flag | ✅ Implementado |
| `CommitLogReader` / `CommitLogReplayer` | 2 clases separadas | Replay inlined | 🔶 Simplificado |
| `CommitLogDescriptor` | Metadata persistence | — | ❌ Falta |
| `DirectIOSegment` / `MemoryMappedSegment` | Segment type variants | Solo plain file I/O | ❌ Falta |
| `EncryptedSegment` | Encryption support | — | ❌ Falta |
| `GroupCommitLogService` | Group commit batching | — | ❌ Falta |
| Async segment management | Dedicated allocator threads | — | ❌ Falta |
| `CommitLogPosition` first-class type | Dedicated class | Tuple `(u64, u64)` | 🔶 Simplificado |

### 2.2 Compaction

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| STCS strategy | `SizeTieredCompactionStrategy` | `mod.rs` | ✅ Implementado |
| LCS strategy | `LeveledCompactionStrategy` | `lcs.rs` | ✅ Implementado |
| TWCS strategy | `TimeWindowCompactionStrategy` | `twcs.rs` | ✅ Implementado |
| UCS strategy | `UnifiedCompactionStrategy` | `ucs.rs` | ✅ Implementado |
| Merge logic (LWW, tombstone GC, TTL) | — | Inline | ✅ Implementado |
| Anti-compaction | `AntiCompaction*` | `anticompaction.rs` | ✅ Implementado |
| **`CompactionManager`** (execution engine) | Scheduling, throttling, concurrent | — | ❌ **CRÍTICO** — Falta |
| `CompactionTask` / hierarchy | 5+ task classes | — | ❌ Falta |
| `CompactionIterator` | Row-by-row merge with purging | — | ❌ Falta |
| `CompactionController` | Strategy control | — | ❌ Falta |
| `CompactionStrategyManager` / `Holder` | Manager hierarchy | — | ❌ Falta |
| `CompactionLogger` | Event logging | — | ❌ Falta |
| `ActiveCompactions` tracker | Concurrent tracking | — | ❌ Falta |
| `PendingRepairManager` | Repair-aware compaction | — | ❌ Falta |
| `ShardManager*` family | Disk-aware distribution | — | ❌ Falta |
| All `writers/` | 5 writer classes | — | ❌ Falta |
| `LeveledGenerations` / `LeveledManifest` | Level persistence | — | ❌ Falta |

### 2.3 Marshal — ENTERAMENTE FALTA

**Java**: 58 archivos — El sistema de tipos CQL completo (`AbstractType`, `UTF8Type`, `Int32Type`, `ListType`, `MapType`, `SetType`, `TupleType`, `UserType`, `CompositeType`, `ReversedType`, `FrozenType`, `TypeParser`, `ValueAccessor`, etc.)

**Rust**: No existe. Los valores se manejan como `Vec<u8>` raw. No hay framework de serialización/comparación/parsing de tipos.

> ⚠️ **Gap crítico**: marshal types son fundamentales para compatibilidad CQL, operaciones de índice, range scans y wire protocol.

### 2.4 Memtable

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `SkipListMemtable` (BTreeMap-based) | `SkipListMemtable` | `mod.rs` | ✅ Implementado |
| `TrieMemtable` | Prefix trie-based | `trie.rs` | ✅ Implementado |
| `MemtableManager` lifecycle | Flush orchestration | `mod.rs` | ✅ Implementado |
| Memory usage estimation | — | Inline | ✅ Implementado |
| `ShardedSkipListMemtable` | Multi-core sharded variant | — | ❌ Falta |
| `AbstractAllocatorMemtable` | Off-heap allocation | — | ❌ Falta |
| `ShardBoundaries` | Shard distribution | — | ❌ Falta |
| ConcurrentSkipListMap equivalente | Lock-free | `RwLock<BTreeMap>` (coarser) | 🔶 Simplificado |

### 2.5 Rows — ENTERAMENTE FALTA (como subsistema)

**Java**: 41 archivos — `Row`, `BTreeRow`, `Cell`, `AbstractCell`, `BufferCell`, `NativeCell`, `Unfiltered`, `UnfilteredRowIterator`, `RangeTombstoneMarker`, `ComplexColumnData`, `Cells`, `EncodingStats`, 14+ iterators

**Rust**: Solo `Cell` y `Row` structs simplificados en `partition.rs`. Falta toda la infraestructura de iteración (`UnfilteredRowIterator`, range tombstone markers, complex column data, encoding stats, todos los iteradores).

### 2.6 Partitions — ENTERAMENTE FALTA (como subsistema)

**Java**: 19 archivos — `AbstractBTreePartition`, `AtomicBTreePartition`, `ImmutableBTreePartition`, `PartitionUpdate`, `FilteredPartition`, `PurgeFunction`, `PartitionIterator`, etc.

**Rust**: Solo `PartitionData` con `BTreeMap<Vec<u8>, Row>` simplificado.

### 2.7 Filter — ENTERAMENTE FALTA

**Java**: 11 archivos — `ClusteringIndexFilter`, `ClusteringIndexSliceFilter`, `ClusteringIndexNamesFilter`, `ColumnFilter`, `DataLimits`, `RowFilter`

**Rust**: Sin infraestructura de filtrado en read path. Se retornan particiones completas.

### 2.8 Lifecycle — ENTERAMENTE FALTA

**Java**: 18 archivos — `LifecycleTransaction`, `Tracker`, `View`, `LogFile`, `LogRecord` — Sistema de transacciones crash-safe para mutaciones atómicas del set de SSTables.

**Rust**: SSTables se manejan via `Vec` + `RwLock` simple.

### 2.9 Guardrails — ENTERAMENTE FALTA

**Java**: 30 archivos — `Guardrails`, `GuardrailsConfig`, `Guardrail`, `Threshold`, `MaxThreshold`, `EnableFlag`, `CassandraPasswordValidator`, etc.

**Rust**: Sin framework de guardrails configurables.

### 2.10 Virtual Tables — ENTERAMENTE FALTA

**Java**: 70 archivos — `VirtualTable`, `VirtualKeyspace`, `SystemViewsKeyspace`, `SettingsTable`, `ClientsTable`, `SSTableTasksTable`, 40+ virtual table implementations.

**Rust**: Sin infraestructura de virtual tables.

### 2.11 Materialized Views

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| View definition + registration | `View`, `ViewManager` | `materialized_views.rs` | ✅ Implementado |
| Write-path fanout | `ViewUpdateGenerator` | `generate_view_updates()` | ✅ Implementado |
| `ViewBuilder` (backfill) | Full-table rebuild | — | ❌ Falta |
| WHERE clause evaluation | — | TODO (always passes) | ❌ Falta |
| View PK recomputation | — | TODO | ❌ Falta |
| View read-repair | — | TODO | ❌ Falta |

### 2.12 Transform — ENTERAMENTE FALTA

**Java**: 18 archivos — `Transformation`, `BaseIterator`, `Filter`, `FilteredRows`, `FilteredPartitions`, `DuplicateRowChecker`, `RTBoundCloser`, `RTBoundValidator`, etc. Pipeline composable para read path.

### 2.13 Tries — ENTERAMENTE FALTA

**Java**: 13 archivos — `InMemoryTrie`, `MergeTrie`, `SlicedTrie`, etc. Usados para TrieMemtable y BTI partition index. La Rust `TrieMemtable` usa `BTreeMap` simple, no un trie real.

### 2.14 Top-level Classes

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `ColumnFamilyStore` | Core table store | `StorageEngine` (parcial) | 🔶 Parcial |
| `Keyspace` | Keyspace runtime | Parcial en engine | 🔶 Parcial |
| `Mutation` | Write mutation | Struct simplificado | 🔶 Parcial |
| **`ReadCommand`** hierarchy | Query execution | — | ❌ **CRÍTICO** — Falta |
| **`DecoratedKey`** / `BufferDecoratedKey` | Token-decorated key | — | ❌ Falta |
| **`Clustering*`** hierarchy | Clustering comparison | — | ❌ Falta |
| `DeletionInfo` / `RangeTombstone` | Deletion tracking | Flags simplificados | 🔶 Simplificado |
| `SerializationHeader` | SSTable schema | — | ❌ Falta |
| `Directories` / `DiskBoundaries` | Disk management | — | ❌ Falta |
| `SystemKeyspace` | System tables | — | ❌ Falta |
| `SSTableImporter` | Bulk loading | — | ❌ Falta |
| `ConsistencyLevel` (en db) | — | En coordinator crate | ✅ |

### 2.15 Otros subsistemas de db/

| Subsistema | Java | Rust | Estado |
|------------|------|------|--------|
| `compression/` (18 archivos) | Dictionary compression framework | — | ❌ Falta |
| `context/` (CounterContext) | CRDT counter | `counter.rs` | ✅ Implementado |
| `aggregation/` (3 archivos) | Query aggregation | — | ❌ Falta |
| `monitoring/` (4 archivos) | DB monitoring | — | ❌ Falta |
| `streaming/` (17 archivos en db) | DB-level streaming | — | ❌ Falta |

---

## 3. CQL

**Java**: `cql3/` — ~286 archivos
**Rust**: `cassandra-cql/src/` — 9 archivos

### 3.1 Statements (Parser/AST)

**Presente en Rust**: CreateKeyspace, AlterKeyspace, DropKeyspace, CreateTable, AlterTable, DropTable, CreateIndex, DropIndex, CreateMaterializedView, DropMaterializedView, CreateType, DropType, CreateFunction, DropFunction, CreateAggregate, DropAggregate, CreateTrigger, DropTrigger, CreateRole, AlterRole, DropRole, Grant, Revoke, ListRoles, ListPermissions, Select, Insert, Update, Delete, Use, Truncate, Batch

**Falta en Rust**:

| Statement | Estado |
|-----------|--------|
| `AlterTypeStatement` | ❌ Falta |
| `AlterViewStatement` | ❌ Falta |
| `DescribeStatement` | ❌ Falta |
| `TransactionStatement` (Accord) | ❌ Falta |
| `ConditionStatement` (Accord) | ❌ Falta |
| `CopyTableStatement` | ❌ Falta |
| `AddIdentityStatement` / `DropIdentityStatement` | ❌ Falta (mTLS identity) |
| `ListSuperUsersStatement` / `ListUsersStatement` | ❌ Falta |
| `CommentOn*Statements` (5 clases) | ❌ Falta |
| `SecurityLabelOn*Statements` (5 clases) | ❌ Falta |

### 3.2 Built-in Functions — ENTERAMENTE FALTA

**Java**: ~15 clases de funciones + `NativeFunctions` registry con ~50+ builtins

| Función | Estado |
|---------|--------|
| `count`, `sum`, `avg`, `min`, `max` (AggregateFcts) | ❌ Falta |
| `now()`, `toTimestamp()`, `toDate()` (TimeFcts) | ❌ Falta |
| `uuid()`, `timeuuidToTimestamp()` (UuidFcts) | ❌ Falta |
| `token()` (TokenFct) | ❌ Falta |
| `CAST(x AS type)` (CastFcts) | ❌ Falta |
| `blobAsX()`, `XAsBlob()` (BytesConversionFcts) | ❌ Falta |
| `abs`, `exp`, `log`, `round` (MathFcts) | ❌ Falta |
| `length()` (LengthFcts) | ❌ Falta |
| `+`, `-`, `*`, `/` (OperationFcts) | ❌ Falta |
| `toJson`, `fromJson` (FormatFcts) | ❌ Falta |
| `similarity_cosine` etc. (VectorFcts) | ❌ Falta |
| `mask_*` (MaskingFcts, 8 clases) | ❌ Falta |
| `FunctionResolver` (overload resolution) | ❌ Falta |

### 3.3 Restrictions (WHERE clause) — ENTERAMENTE FALTA

**Java**: 14 archivos — `StatementRestrictions`, `PartitionKeyRestrictions`, `ClusteringColumnRestrictions`, `RestrictionSet`, `SingleRestriction`, `IndexRestrictions`, `LikePattern`, etc.

**Rust**: `Relation` existe en el AST pero NO hay validación. El planner tiene un TODO: `// TODO(phase-3+): Validate columns against table metadata`.

### 3.4 Selection (SELECT processing) — ENTERAMENTE FALTA

**Java**: 28 archivos — `Selection`, `Selector`, `SelectorFactories`, `AggregateFunctionSelector`, `WritetimeOrTTLSelector`, `ResultSetBuilder`, `FieldSelector`, `ElementsSelector`, etc.

**Rust**: Enum `Selector` básico en AST sin lógica de evaluación.

### 3.5 Conditions (LWT IF) — ENTERAMENTE FALTA

**Java**: 6 archivos — `ColumnCondition`, `CQL3CasRequest`, condition evaluation

**Rust**: Solo `if_conditions` y `if_exists` en AST nodes. Sin evaluación.

### 3.6 Terms (Value processing)

**Presente**: `Term` enum básico (Literal, BindMarker, FunctionCall, TypeHint, CollectionLiteral, MapLiteral, TupleLiteral)

**Falta**: Type validation, binding to schema types, serialization to wire format, `Constants`, `Lists/Maps/Sets/Tuples/Vectors` term classes, `MultiElements`, `UserTypes`, `InMarker`

### 3.7 Query Processing

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `PreparedCache` | Cache with MD5 IDs | `prepared.rs` | ✅ Implementado |
| **`QueryProcessor`** | Central execution orchestrator | — | ❌ **CRÍTICO** — Falta |
| `QueryHandler` | Pluggable interface | — | ❌ Falta |
| `QueryOptions` | Paging, timestamps, etc. | — | ❌ Falta |
| `ResultSet` | Result construction + pagination | — | ❌ Falta |
| `UntypedResultSet` | Internal system queries | — | ❌ Falta |
| `UpdateParameters` | Timestamp/TTL context | — | ❌ Falta |

### 3.8 Constraints — ENTERAMENTE FALTA

**Java**: 16 archivos — Column constraints (NOT NULL, LENGTH, REGEXP, JSON schema validation)

---

## 4. Transport (Protocolo Nativo)

**Java**: `transport/` — ~47 archivos
**Rust**: `cassandra-native-protocol/src/` — 11 archivos

### Presente en Rust (buena cobertura del codec):
- ✅ Frame codec completo (Tokio Decoder/Encoder)
- ✅ Todos los opcodes (0x00-0x10)
- ✅ Todos los message types (request + response)
- ✅ Protocol type read/write (int, long, string, bytes, uuid, inet, consistency, etc.)
- ✅ Connection lifecycle state machine (New → Authenticating → Ready)
- ✅ Event dispatcher (TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE)
- ✅ Compression (LZ4, Snappy)
- ✅ Error codes completos

### Falta:

| Componente | Java | Severidad |
|------------|------|-----------|
| **`Server.java`** (TCP listener + Netty pipeline) | — | ❌ **CRÍTICO** — No hay server TCP |
| **`Dispatcher.java`** (message → CQL execution) | — | ❌ **CRÍTICO** — No hay dispatch |
| `PipelineConfigurator` (SSL + frame + handler chain) | — | ❌ Alta |
| `CQLMessageHandler` | — | ❌ Alta |
| `Flusher` (write coalescing) | — | 🔶 Media |
| `ClientResourceLimits` (per-client memory) | — | ❌ Alta |
| `ConnectionLimitHandler` (max connections) | — | ❌ Alta |
| `QueueBackpressure` | — | 🔶 Media |
| `ConnectedClient` / `ClientStat` | — | 🔶 Media |
| `InitialConnectionHandler` (version detection) | — | 🔶 Media |

> ⚠️ **El gap más crítico de todo el proyecto**: Sin TCP listener, no hay Cassandra. El codec está completo pero no hay server que acepte conexiones.

---

## 5. Net (Inter-nodo)

**Java**: `net/` — ~70 archivos
**Rust**: `cassandra-messaging/src/` — 5 archivos

### Presente en Rust:
- ✅ 42 verbs definidos
- ✅ Frame codec (length-prefixed, 28-byte header)
- ✅ MessagingService con handler registration, send/send_and_wait
- ✅ Per-verb timeouts, métricas
- ✅ Version negotiation handshake

### Falta:

| Componente | Java | Severidad |
|------------|------|-----------|
| **Persistent outbound connections** | `OutboundConnection` (reconnect, coalescing) | ❌ **CRÍTICO** — Rust crea nueva TCP por cada `send()` |
| **3-channel per peer** | `OutboundConnections` (urgent/large/small) | ❌ Alta |
| `OutboundMessageQueue` | Send queue with pruning | ❌ Alta |
| **CRC/LZ4 frame integrity** | `FrameDecoder*` (5 clases), `FrameEncoder*` (3 clases) | ❌ Alta |
| Full handshake protocol | `HandshakeProtocol` (3-step) | 🔶 Media |
| `ForwardingInfo` | Request forwarding | 🔶 Media |
| Rich message headers | `Message.java` headers | 🔶 Media |
| `ResourceLimits` | Endpoint + global limits | ❌ Alta |
| `AsyncStreamingInputPlus/OutputPlus` | Large data streaming | ❌ Alta |

> ⚠️ Nueva TCP por cada `send()` causaría problemas severos de performance en producción.

---

## 6. Cluster / Gossip / DHT / Locator

**Java**: `gms/` (27) + `dht/` (30+) + `locator/` (70+) — ~130 archivos
**Rust**: `cassandra-cluster-metadata/src/` — 12 archivos

### 6.1 Gossip

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `EndpointState`, `ApplicationState`, `VersionedValue`, `HeartBeatState` | — | `gossip::*` | ✅ Implementado |
| `FailureDetector` (phi-accrual) | — | `failure_detector.rs` | ✅ Implementado |
| `GossipDigest*` messages | — | `gossip::messages` | ✅ Implementado |
| `SeedProvider` | — | `gossip::SeedProvider` | ✅ Implementado |
| **Gossip loop** (periodic SYN/ACK/ACK2) | `Gossiper` (2,402 LOC) | — | ❌ **CRÍTICO** — Falta |
| Verb handlers (SYN/ACK/ACK2) | 3 handler clases | — | ❌ Falta |
| `IEndpointStateChangeSubscriber` | Observer pattern | — | ❌ Falta |
| Shadow round (initial discovery) | — | — | ❌ Falta |
| Quarantine logic | — | — | ❌ Falta |
| `GossipShutdown` protocol | — | — | ❌ Falta |
| `NewGossiper` (TCM-aware) | — | — | ❌ Falta |

### 6.2 DHT

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| Token (Murmur3) | `Token`, `Murmur3Partitioner` | `Token` (hardcoded Murmur3) | ✅ Implementado |
| `Range` / `TokenRange` | — | `TokenRange` | ✅ Implementado |
| **`IPartitioner` trait** | Interface + 5 implementations | Hardcoded Murmur3 only | ❌ Falta |
| `RandomPartitioner` | MD5-based | — | ❌ Falta |
| `ByteOrderedPartitioner` | Byte ordering | — | ❌ Falta |
| `AbstractBounds` / variants | Inclusive/exclusive boundaries | — | ❌ Falta |
| `BootStrapper` (token selection) | — | Skeleton | 🔶 Parcial |
| `RangeStreamer` (range computation) | — | — | ❌ Falta |
| `tokenallocator/` (10 archivos) | Vnode-aware allocation | — | ❌ Falta |

### 6.3 Locator / Replication

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `SimpleStrategy` | — | ✅ | ✅ Implementado |
| `NetworkTopologyStrategy` | — | ✅ | ✅ Implementado |
| `LocalStrategy` | — | ✅ | ✅ Implementado |
| `EverywhereStrategy` | — | ✅ | ✅ Implementado |
| `MetaStrategy`, `RemoteStrategy`, `SystemStrategy` | — | — | ❌ Falta |
| `SimpleSnitch` | — | ✅ | ✅ |
| `PropertyFileSnitch` | — | ✅ | ✅ |
| `GossipingPropertyFileSnitch` | — | ✅ | ✅ |
| `DynamicEndpointSnitch` | — | ✅ | ✅ |
| `RackInferringSnitch` | — | ✅ | ✅ |
| `Ec2Snitch` / `Ec2MultiRegionSnitch` | Cloud metadata API | Stub (hardcoded values) | 🔶 Stub |
| `GoogleCloudSnitch` | Cloud metadata API | Stub (hardcoded values) | 🔶 Stub |
| `AzureSnitch`, `AlibabaCloudSnitch`, `CloudstackSnitch` | — | — | ❌ Falta |
| Cloud metadata HTTP client | `AbstractCloudMetadataServiceConnector` | — | ❌ Falta |
| `EndpointsForToken/Range` typed collections | — | — | ❌ Falta |
| `ReplicaPlan` / `ReplicaPlans` | — | — | ❌ Falta |
| `ReplicationFactor` type (full vs transient RF) | — | Bare `usize` | 🔶 Simplificado |

---

## 7. Service Layer

**Java**: `service/` — 50+ archivos + 7 subpackages (~20K+ LOC)
**Rust**: Parcialmente en `cassandra-coordinator/` y `cassandra-cluster-metadata/src/topology.rs`

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| **`StorageService`** (5,856 LOC) | Brain of Cassandra | `TopologyCoordinator` (~700 LOC, skeleton) | 🔶 Muy parcial |
| **`StorageProxy`** (3,863 LOC) | Read/write path | — | ❌ **CRÍTICO** — Falta |
| `CassandraDaemon` | Main daemon | — | ❌ Falta |
| `ClientState` / `QueryState` | Per-client state | — | ❌ Falta |
| `NativeTransportService` | CQL service | — | ❌ Falta |
| `CacheService` | Row/key cache management | — | ❌ Falta |
| `ActiveRepairService` | Repair coordination | — | ❌ Falta |
| `WriteResponseHandler` hierarchy | 4 response handler classes | — | ❌ Falta |
| `StartupChecks` | Pre-flight checks | — | ❌ Falta |

### service/paxos/ — ENTERAMENTE FALTA
**Java**: ~20 archivos — Ballot, Prepare, Propose, Commit, PaxosState, PaxosRepair, ContentionStrategy

### service/accord/ — ENTERAMENTE FALTA
**Java**: ~70 archivos — CommandStore, DataStore, TopologyService, Journal, FastPath

### service/reads/ — ENTERAMENTE FALTA
**Java**: ~20 archivos — ReadExecutor, DataResolver, DigestResolver, SpeculativeRetryPolicy, ShortReadProtection

### service/consensus/ — ENTERAMENTE FALTA
**Java**: 3+ archivos — Consensus migration (Paxos to Accord)

---

## 8. TCM (Transactional Cluster Metadata)

**Java**: `tcm/` — 60+ archivos, 10 subpackages
**Rust**: `cassandra-cluster-metadata/src/tcm.rs` (parcial)

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| `Epoch` | — | `tcm::Epoch` | ✅ |
| `ClusterMetadata` (1,185 LOC) | — | `tcm::TcmMetadata` | 🔶 Parcial |
| `MetadataLog` | — | `tcm::MetadataLog` | ✅ |
| `Transformation` (25+ tipos) | — | 8 variantes | 🔶 Muy parcial |
| `MetadataSnapshots` | — | `tcm::TcmSnapshot` | ✅ |
| **Commit/Processor** (Paxos-backed) | Core of TCM | — | ❌ **CRÍTICO** — Falta |
| `Discovery` / `Startup` | — | — | ❌ Falta |
| `tcm/log/` (persistent storage) | 5 archivos | — | ❌ Falta |
| `tcm/membership/` | Directory, Location, NodeVersion | Parcial | 🔶 |
| `tcm/ownership/` | DataPlacements, MovementMap, PlacementTransitionPlan | Muy parcial | 🔶 |
| `tcm/sequences/` (18 archivos) | Bootstrap, Leave, Move sequences | Step tracking básico | 🔶 |
| `tcm/listeners/` (9 archivos) | — | — | ❌ Falta |
| `tcm/serialization/` (8 archivos) | Versioned wire serialization | Serde derives | 🔶 |
| `tcm/migration/` (CMS election) | 5 archivos | — | ❌ Falta |

---

## 9. IO / SSTable

**Java**: `io/` — SSTable (~100+ archivos), compress (13), util (70+)
**Rust**: `cassandra-storage/src/sstable/` — 6 archivos

### 9.1 SSTable Reader/Writer

| Componente | Java | Rust | Estado |
|------------|------|------|--------|
| Big format writer (Data.db + Index.db + Filter.db + Summary.db + Stats + TOC) | — | `writer.rs` | ✅ |
| Big format reader (bloom + binary search + partition read) | — | `reader.rs` | ✅ |
| BTI writer (trie partition index) | — | `bti.rs` | ✅ |
| BTI reader | — | `bti.rs` | ✅ |
| Bloom filter | `BloomFilter*` | `bloom.rs` | ✅ |
| **Compression** (LZ4/Snappy/Zstd/Deflate) | 13 archivos | — | ❌ **CRÍTICO** — Falta |
| **Index Summary** (Big format) | Full impl, off-heap | Written but NEVER READ | ❌ Falta |
| **Key Cache** | `KeyCache` + metrics | — | ❌ Falta |
| Range tombstones in data format | Full support | — | ❌ Falta |
| Row-level index (BTI) | Intra-partition seeking | Solo partition offsets | ❌ Falta |
| Reverse iteration | `SSTableReversedIterator` | — | ❌ Falta |
| Token-range scanning | `SSTableScanner` with filtering | Returns all | ❌ Falta |
| `SSTableRewriter` | Atomic replacement during compaction | — | ❌ Falta |
| `SSTableLoader` | Bulk loading | — | ❌ Falta |
| `Scrubber` | Corrupt recovery | — | ❌ Falta |
| `Verifier` | Integrity checks | — | ❌ Falta |
| Zero-copy writer | Streaming | — | ❌ Falta |
| Metadata serialization | Binary `MetadataSerializer` | JSON-based (incompatible) | ❌ Incompatible |
| Version handling | Feature flags per version | Single `DATA_VERSION = 1` | ❌ Falta |

### 9.2 io/util/ — ENTERAMENTE FALTA

**Java**: 70+ archivos — `Rebufferer`, `RandomAccessReader`, `SequentialWriter`, `FileHandle`, `Memory`/`SafeMemory`, `MmappedRegions`, `ChecksumWriter`, `DiskOptimizationStrategy`, `DataInputPlus`/`DataOutputPlus`, `SimpleCachedBufferPool`

**Rust**: Usa `std::fs::File` con `BufReader`/`BufWriter` directamente. Sin mmap, sin off-heap, sin buffer pooling, sin rebufferer pluggable.

### 9.3 Compatibilidad de formato

> ⚠️ **Los SSTables de Rust NO son compatibles con Java**. Usan magic bytes diferentes (`SSDT`/`SSIX`/`SSFL`), encoding simplificado, JSON para statistics, sin partition header, sin complex types. **Rust no puede leer SSTables de Java y viceversa.**

---

## 10. Streaming

**Java**: `streaming/` — 50+ archivos, 8,253 LOC
**Rust**: `cassandra-streaming/src/` — 7 archivos, 1,918 LOC

### Presente:
- ✅ `StreamSession` state machine (Initialized → Preparing → Streaming → Complete/Failed)
- ✅ `StreamPlan` builder
- ✅ `StreamManager` registry
- ✅ `StreamTransfer` chunk-based con checksums
- ✅ Rate limiter, métricas, snapshot management

### Falta:

| Componente | Estado |
|------------|--------|
| **Network transport** (Netty channels, TCP) | ❌ **CRÍTICO** — No hay I/O real |
| **Message protocol** (11 message types) | ❌ Falta |
| `StreamCoordinator` (multi-session coordination) | ❌ Falta |
| `StreamResultFuture` | ❌ Falta |
| `StreamDeserializingTask` | ❌ Falta |
| `StreamReceiver` (write to disk) | ❌ Falta |
| Stream compression | ❌ Falta |
| Zero-copy streaming | ❌ Falta |

> ⚠️ Todo el streaming es data-structure only. `StreamTransfer.chunkify()` divide datos en chunks pero nada los envía por la red.

---

## 11. Repair

**Java**: `repair/` — 90+ archivos, 13,236 LOC
**Rust**: `cassandra-repair/src/` — 6 archivos, 2,489 LOC

### Presente:
- ✅ `RepairCoordinator` con tree exchange
- ✅ `RepairSession` state machine
- ✅ `MerkleTree` con diff operation
- ✅ Métricas, history tracking
- ✅ `RepairType` (Full, Incremental, Preview)

### Falta:

| Componente | Estado |
|------------|--------|
| `RepairJob` (multi-replica pairwise comparison) | ❌ Falta |
| **Consistent repair** (9 archivos — `ConsistentSession`, `CoordinatorSession`, `LocalSession`) | ❌ **CRÍTICO** para incremental repair |
| Repair messages (16 archivos — solo 2 implementados) | ❌ Falta |
| `Validator` (build tree from SSTable data) | ❌ Falta |
| `SyncTask` / `RemoteSyncTask` | ❌ Falta |
| `StreamingRepairTask` | ❌ Falta |
| Asymmetric repair (8 archivos) | ❌ Falta |
| Auto-repair (10 archivos) | ❌ Falta |
| Repair state virtual tables (8 archivos) | ❌ Falta |
| `RepairParallelism` (Sequential/Parallel/DC-parallel) | ❌ Falta |
| `MerkleTrees` (plural, per-CF container) | ❌ Falta |

---

## 12. Auth

**Java**: `auth/` — 68 archivos
**Rust**: `cassandra-security/src/{auth.rs, authz.rs, roles.rs, cidr.rs}`

### Presente:
- ✅ `Authenticator` trait + `AllowAllAuthenticator` + `PasswordAuthenticator` (bcrypt)
- ✅ `Authorizer` trait + `AllowAllAuthorizer` + `CassandraAuthorizer`
- ✅ `Permission` enum (10 variants)
- ✅ `Resource` enum (Keyspace/Table/Function/Role/Jmx)
- ✅ `RoleManager` trait + `InMemoryRoleManager`
- ✅ `CidrAuthorizer`

### Falta:

| Componente | Estado |
|------------|--------|
| **Persistence to system_auth** | ❌ **CRÍTICO** — Todo in-memory |
| `AuthCache` / `AuthCacheService` | ❌ Falta |
| mTLS authenticators (6 clases) | ❌ Falta |
| `INetworkAuthorizer` (network auth) | ❌ Falta |
| `IInternodeAuthenticator` (internode auth) | ❌ Falta |
| `PermissionsCache`, `RolesCache` | ❌ Falta |
| CIDR groups management (6 clases) | ❌ Falta |

---

## 13. Security

**Java**: `security/` — 18 archivos
**Rust**: `cassandra-security/src/tls.rs`

### Presente:
- ✅ TLS client/server configs (rustls)
- ✅ PEM cert/key loading
- ✅ Hot reload (`ReloadableTlsAcceptor`)

### Falta:

| Componente | Estado |
|------------|--------|
| `ISslContextFactory` pluggable trait | ❌ Falta |
| Encryption-at-rest (`EncryptionContext`, `CipherFactory`, `EncryptionUtils`) | ❌ Falta |
| Crypto provider abstraction | ❌ Falta |
| JKS support | N/A (by design, PEM only) |

---

## 14. Config

**Java**: `config/` — 35 archivos
**Rust**: `cassandra-config/src/` — 4 archivos

### Presente:
- ✅ `CassandraConfig` struct (YAML deserialization)
- ✅ `load_config` + validation
- ✅ `EncryptionOptions`, `GuardrailsConfig`
- ✅ `DataSize`, `Duration` units
- ✅ `PartitionDenylist`

### Falta:

| Componente | Estado |
|------------|--------|
| `DatabaseDescriptor` runtime state | ❌ Falta |
| `DataRateSpec` | ❌ Falta |
| `CassandraRelevantProperties` (system properties) | ❌ Falta |
| `RepairConfig` | ❌ Falta |
| `StorageAttachedIndexOptions` | ❌ Falta |
| `TransparentDataEncryptionOptions` | ❌ Falta |
| Environment variable overrides | ❌ Falta |
| Hot-reload | ❌ Falta |

---

## 15. Index (Secondary Indexes)

**Java**: `index/` — 70+ archivos (top-level + SAI + SASI + internal)
**Rust**: Solo `IndexMetadata` en schema crate

| Componente | Estado |
|------------|--------|
| `Index` interface | ❌ Falta |
| `IndexRegistry` | ❌ Falta |
| `SecondaryIndexManager` | ❌ Falta |
| **SAI engine** (~40+ clases) | ❌ **Enteramente falta** |
| **SASI engine** (~20+ clases) | ❌ **Enteramente falta** |
| Legacy 2i (`CassandraIndex`) | ❌ Falta |
| Index query planning | ❌ Falta |
| Index building/lifecycle | ❌ Falta |

> ⚠️ Solo existe metadata de índices. No hay motor de ejecución de ningún tipo de índice.

---

## 16. Schema

**Java**: `schema/` — 48 archivos
**Rust**: `cassandra-schema/src/` — 8 archivos

### Presente:
- ✅ `SchemaCatalog`, `KeyspaceMetadata`, `TableMetadata`, `ColumnMetadata`, `IndexMetadata`
- ✅ `SchemaConstants`, system keyspace definitions
- ✅ Schema agreement protocol

### Falta:

| Componente | Estado |
|------------|--------|
| `ViewMetadata` / `Views` | ❌ Falta |
| `TriggerMetadata` / `Triggers` | ❌ Falta |
| UDT/UDF/UDA management en catalog | ❌ Falta |
| `CachingParams`, `CompactionParams`, `CompressionParams`, `MemtableParams` | ❌ Falta |
| `SchemaTransformation`, `SchemaChangeListener`, `SchemaChangeNotifier` | ❌ Falta |
| `SchemaPull/Push/VersionVerbHandler` (gossip schema exchange) | ❌ Falta |
| `DistributedSchema` | ❌ Falta |
| `DroppedColumn`, `Diff`, `Difference` | ❌ Falta |

---

## 17. Tools

**Java**: `tools/` — 30+ archivos + `nodetool/` subdir
**Rust**: `cassandra-tools/src/` — 2 archivos

### Presente:
- ✅ CLI framework (clap, 28 subcommands)
- ✅ SSTable dump/metadata viewer
- ✅ fqltool (dump + stats)
- ✅ HTTP admin API client approach

### Falta:

| Componente | Estado |
|------------|--------|
| ~140 nodetool commands (17 implementados, resto stubs) | ❌ Mayormente stubs |
| `StandaloneScrubber` | ❌ Falta |
| `StandaloneSplitter` | ❌ Falta |
| `StandaloneUpgrader` | ❌ Falta |
| `StandaloneVerifier` | ❌ Falta |
| `GenerateTokens` | ❌ Falta |
| `SSTableLevelResetter`, `SSTableRepairedAtSetter`, etc. | ❌ Falta |
| `BootstrapMonitor` | ❌ Falta |

---

## 18. Audit / FQL

### Audit

**Java**: 15 archivos → **Rust**: `cassandra-security/src/audit.rs`

| Componente | Estado |
|------------|--------|
| `AuditLogger` trait | ✅ |
| `FileAuditLogger` (JSON + rotation) | ✅ |
| `AsyncAuditLogger` (channel-based) | ✅ |
| `NoOpAuditLogger` | ✅ |
| 14 event types | ✅ |
| Filter logic implementation | ❌ Falta |
| `BinAuditLogger` (Chronicle Queue) | ❌ Falta |
| `AuditLogContext` (per-request) | ❌ Falta |

### FQL

**Java**: 3 archivos → **Rust**: `fql.rs` + `fqltool.rs`

| Componente | Estado |
|------------|--------|
| `FqlLogger` with binary format | ✅ |
| `FqlOptions` | ✅ |
| fqltool CLI (dump + stats) | ✅ |

> ✅ FQL está funcionalmente completo (formato binario diferente al de Java — no cross-compatible).

---

## 19. Hints

**Java**: `hints/` — 30 archivos
**Rust**: `cassandra-coordinator/src/{hints.rs, hint_segment.rs}`

### Presente:
- ✅ `HintedHandoffManager` + `HintStore`
- ✅ Capacity limits, expiry, pause/resume
- ✅ Per-endpoint tracking, drain on recovery
- ✅ Segment read/write

### Falta:

| Componente | Estado |
|------------|--------|
| On-disk hint catalog (`HintsCatalog`, `HintsDescriptor`) | ❌ Falta |
| `HintsBuffer` / `HintsBufferPool` | ❌ Falta |
| Compression/encryption for hints | ❌ Falta |
| Background dispatch executor | ❌ Falta |
| `HintVerbHandler` | ❌ Falta |

---

## 20. Batchlog

**Java**: `batchlog/` — 5 archivos
**Rust**: `cassandra-coordinator/src/batch.rs`

| Componente | Estado |
|------------|--------|
| `BatchEntry`, `BatchLogManager` | ✅ |
| `BatchCoordinator` (logged/unlogged/counter) | ✅ |
| Guardrails (size/partition limits) | ✅ |
| Replay of expired entries | ✅ |
| `BatchStoreVerbHandler`, `BatchRemoveVerbHandler` | ❌ Falta |

> ✅ Bien implementado. Solo faltan verb handlers para replicación inter-nodo.

---

## 21. Cache — ENTERAMENTE FALTA

**Java**: `cache/` — 20 archivos

| Componente | Estado |
|------------|--------|
| `ICache`, `CacheProvider` | ❌ Falta |
| `CaffeineCache`, `OHCProvider` (off-heap) | ❌ Falta |
| `AutoSavingCache` (persist to disk) | ❌ Falta |
| `KeyCacheKey`, `RowCacheKey`, `CounterCacheKey` | ❌ Falta |
| `ChunkCache` | ❌ Falta |

> ⚠️ Sin cache de ningún tipo. Impacto severo en performance de lecturas.

---

## 22. Metrics

**Java**: `metrics/` — 90+ archivos (Dropwizard)
**Rust**: `cassandra-admin/src/{metrics.rs, prometheus_metrics.rs}` (Prometheus)

### Presente (~17 métricas registradas):
- ✅ Client request latency (histogram)
- ✅ Pending compactions (gauge)
- ✅ Repair gauges (7)
- ✅ Hint/batch metrics

### Falta (~75 de ~90 clases Java):

| Categoría | Estado |
|-----------|--------|
| Per-table metrics (`TableMetrics`) | ❌ |
| Per-keyspace metrics | ❌ |
| CommitLog metrics | ❌ |
| Cache metrics | ❌ |
| Messaging/dropped messages | ❌ |
| Thread pool metrics | ❌ |
| CQL metrics | ❌ |
| Client metrics | ❌ |
| Paxos/Accord metrics | ❌ |
| Latency sampling/top partitions | ❌ |
| TCM/Storage metrics | ❌ |

---

## 23. Tracing

**Java**: `tracing/` — 6 archivos
**Rust**: `cassandra-coordinator/src/tracing.rs`

| Componente | Estado |
|------------|--------|
| `TraceSession` with events | ✅ |
| Global tracing manager (`Tracing` singleton) | ❌ Falta |
| Persistence to `system_traces` | ❌ Falta |
| TTL/expiry for traces | ❌ Falta |

---

## 24. Triggers / Journal / Notifications / Diag

### Triggers — ENTERAMENTE FALTA
**Java**: 4 archivos — `ITrigger`, `TriggerExecutor`, `CustomClassLoader`

### Journal — ENTERAMENTE FALTA
**Java**: 25 archivos — `Journal`, `Segment`, `Compactor`, `Index`, etc. (foundation para commitlog moderno)

### Notifications — ENTERAMENTE FALTA
**Java**: 14 archivos — SSTable events, memtable events, truncation events

### Diagnostics — ENTERAMENTE FALTA
**Java**: 6 archivos — `DiagnosticEventService`, `DiagnosticEventPersistence`

---

## 25. Exceptions

**Java**: `exceptions/` — 45 archivos
**Rust**: `cassandra-common/src/error.rs`

### Presente (15 protocol error codes):
✅ ServerError, ProtocolError, AuthenticationError, Unauthorized, Unavailable, ReadTimeout, WriteTimeout, InvalidQuery, SyntaxError, AlreadyExists, ConfigError, Overloaded, IsBootstrapping, TruncateError, Unprepared

### Falta (~30 excepciones especializadas):
❌ ReadFailureException, WriteFailureException, CasWriteTimeoutException, CasWriteUnknownResultException, CDCWriteException, FunctionExecutionException, IncompatibleSchemaException, RepairException, OperationExecutionException, y ~20 más

---

## 26. Utils

**Java**: `utils/` — 120+ archivos + subdirectorios
**Rust**: `cassandra-common/src/` — 6 archivos

### Presente:
- ✅ `MurmurHash` (murmur3_x64_128)
- ✅ `CassandraVersion`
- ✅ Timestamp, TTL, Token basics

### Falta:

| Componente | Estado |
|------------|--------|
| `BloomFilter` (en utils, separado de SSTable bloom) | ❌ Falta |
| `MerkleTree` / `MerkleTrees` (en utils) | ❌ Falta |
| `EstimatedHistogram` | ❌ Falta |
| `TimeUUID` / `UUIDGen` | 🔶 Parcial (uuid crate) |
| btree package | ❌ Falta |
| bytecomparable package | ❌ Falta |
| memory package (off-heap) | ❌ Falta |
| binlog package | ❌ Falta |
| progress package | ❌ Falta |
| vint package | ❌ Falta |
| `RTree`, `RangeTree`, `IntervalTree` | ❌ Falta |

---

## 27. Serializers

**Java**: `serializers/` — 31 archivos
**Rust**: `cassandra-types/src/`

### Presente:
- ✅ Core native types (Int, Bigint, Varchar, UUID, Timestamp, Boolean, Float, Double, etc.)
- ✅ Collections (List, Set, Map)
- ✅ Tuple, UDT, Vector types
- ✅ Codec encode/decode

### Falta:

| Componente | Estado |
|------------|--------|
| `DurationSerializer` | ❌ Falta |
| `CounterSerializer` | ❌ Falta |
| `DecimalSerializer` (BigDecimal) | ❌ Falta |
| `IntegerSerializer` (BigInteger) | ❌ Falta |
| `SimpleDateSerializer` / `TimeSerializer` | ❌ Falta |
| Abstract serialization framework | ❌ Falta |
| `MarshalException` | ❌ Falta |

---

## 28. Concurrent

**Java**: `concurrent/` — 38 archivos — `SEPExecutor`, `SharedExecutorPool`, `Stage`, thread pool infrastructure

**Rust**: Usa Tokio async runtime. **By design** — Las utilidades de concurrencia de Java existen para workarounds del JVM que no aplican a Rust.

**Falta potencialmente útil**: El concepto de `Stage` (named stages → executors) para tracing/metrics.

---

## 29. TODOs y Stubs en el Código Rust

### TODOs por crate (42 total):

| Crate | Count | Items más importantes |
|-------|-------|----------------------|
| `cassandra-server` | 5 | AlterTable no-op, TLS config loading, schema passing, Row results mapping, role_permissions check |
| `cassandra-tools` | 15 | Admin API calls para snapshot, compact, sstableloader, audit, FQL, load gen, bootstrap ops |
| `cassandra-storage` | 11 | SASI memtable ops, SAI gaps, legacy index persistence, MV backfill, MV WHERE clause, MV PK computation |
| `cassandra-security` | 3 | Persist permissions, persist roles, SHA-256 masking |
| `cassandra-cql` | 2 | Tuple type parsing, column validation |
| `cassandra-cluster-metadata` | 1 | Transient replication |
| `cassandra-coordinator` | 4 | Read repair blocking, latency percentiles, READ_DATA to replicas, repair type tracking |
| `cassandra-native-protocol` | 2 | Real credential verification |
| `cassandra-migration` | 1 | Full binary SSTable conversion |
| `cassandra-common` | 2 | Module expansion, murmur3 diff-test |
| `cassandra-diff-tests` | 1 | Wire to QueryExecutor |

### Stubs (55+):

| Crate | Stubs notables |
|-------|----------------|
| `cassandra-server` | Auth stubs, executor stubs, result mapping |
| `cassandra-tools` | ~15 nodetool command stubs |
| `cassandra-storage` | UDF module (entero), Triggers module (entero) |
| `cassandra-coordinator` | Trigger augmentation, Accord routing, consensus router |
| `cassandra-cluster-metadata` | Ec2Snitch, GceSnitch, TransientReplication |
| `cassandra-native-protocol` | PasswordAuthenticator (accepts any credentials) |
| `cassandra-accord` | `execute_transaction` es stub |

### Gap Guards (27 tests ignorados en `gap_guards.rs`):

| Categoría | Gaps |
|-----------|------|
| CQL | Functions, permission statements, constraints, advanced restrictions, index diff testing |
| Storage | Java SSTable compat, compaction execution, trie index, DB filters, caching, row transformations, guardrails |
| Distributed | Paging, aggregation, gossip wire compat, internode wire compat |
| Security | FQL, LDAP/Kerberos auth |
| Tooling | ~140 nodetool commands, SSTable offline tools, ~60 virtual tables |
| Trunk-only | TCM, Accord, consensus, journal |
| Infrastructure | Concurrency stages, tracing storage |

---

## 30. Resumen de Cobertura por Subsistema

| Subsistema | Java Archivos | Rust Cobertura | Rating |
|------------|--------------|----------------|--------|
| CommitLog | 24 | Core WAL funcional, sin encryption/mmap/group commit | 60% |
| Compaction | 55 | Strategies OK, sin execution engine | 25% |
| Marshal/Types | 58 | Falta enteramente (critical) | 0% |
| Memtable | 12 | Core funcional, sin sharding | 50% |
| Rows | 41 | Structs básicos, sin iterators | 10% |
| Partitions | 19 | PartitionData simplificado | 10% |
| Filter | 11 | Falta enteramente | 0% |
| Lifecycle | 18 | Falta enteramente | 0% |
| Guardrails | 30 | Falta enteramente | 0% |
| Virtual Tables | 70 | Falta enteramente | 0% |
| MV | 7 | Parcial (fanout OK, sin backfill) | 35% |
| Transform | 18 | Falta enteramente | 0% |
| Tries | 13 | Falta enteramente | 0% |
| CQL Parser | ~50 | Parsing OK, sin ejecución | 30% |
| CQL Functions | ~15 | Falta enteramente | 0% |
| CQL Restrictions | 14 | Falta enteramente | 0% |
| CQL Selection | 28 | Falta enteramente | 0% |
| CQL Processing | ~20 | PreparedCache OK, sin QueryProcessor | 10% |
| Transport | 47 | Codec completo, sin TCP server | 40% |
| Net (inter-node) | 70 | Verbs+codec OK, sin persistent connections | 30% |
| Gossip | 27 | Data structures OK, sin protocol loop | 40% |
| DHT | 30 | Token ring OK, sin partitioner abstraction | 25% |
| Locator | 70 | Core strategies OK, sin cloud snitches | 35% |
| Service | 50+ (+7 subpkgs) | Topology skeleton only | 5% |
| TCM | 60 | Epoch/log OK, sin commit protocol | 20% |
| SSTable I/O | 100+ | Basic read/write, sin compression/mmap | 20% |
| IO Utils | 70 | Falta enteramente | 0% |
| Streaming | 50 | State machine OK, sin I/O real | 25% |
| Repair | 90 | Merkle+session OK, sin consistent repair | 20% |
| Auth | 68 | Core traits OK, sin persistence | 35% |
| Security | 18 | TLS OK, sin encryption-at-rest | 40% |
| Config | 35 | YAML loading OK | 50% |
| Index | 70 | Solo metadata, sin engine | 5% |
| Schema | 48 | Core model OK, sin change events | 35% |
| Tools | 30 | CLI framework, mayormente stubs | 30% |
| Audit | 15 | Buen coverage | 65% |
| FQL | 3 | Completo | 90% |
| Hints | 30 | Core OK, sin disk persistence | 45% |
| Batchlog | 5 | Bien implementado | 75% |
| Cache | 20 | Falta enteramente | 0% |
| Metrics | 90 | Prometheus core, 85% missing | 15% |
| Tracing | 6 | Session OK, sin persistence | 30% |
| Triggers | 4 | Falta enteramente | 0% |
| Journal | 25 | Falta enteramente | 0% |
| Notifications | 14 | Falta enteramente | 0% |
| Diag | 6 | Falta enteramente | 0% |
| Exceptions | 45 | 15 protocol codes, ~30 missing | 35% |
| Utils | 120 | Solo Murmur3 + basics | 10% |
| Serializers | 31 | Core types OK, framework missing | 40% |
| Concurrent | 38 | Tokio (by design) | N/A |

---

## 31. Top 30 Gaps Críticos (Ordenados por Prioridad)

### P0 — Bloquean cualquier uso

| # | Gap | Impacto |
|---|-----|---------|
| 1 | **No hay TCP listener** para protocolo nativo | No se pueden conectar clientes CQL |
| 2 | **No hay `QueryProcessor`** | No se pueden ejecutar queries |
| 3 | **No hay `StorageProxy`** | No hay read/write path coordinado |
| 4 | **No hay gossip loop** | Los nodos no se descubren entre sí |
| 5 | **Persistent internode connections** | Nueva TCP por send() — unusable |
| 6 | **No hay compression** para SSTables | SSTables 3-10x más grandes |
| 7 | **No hay `CompactionManager`** execution | Las strategies existen pero nada las ejecuta |

### P1 — Bloquean beta

| # | Gap | Impacto |
|---|-----|---------|
| 8 | CQL built-in functions (count, now, token, etc.) | Queries básicas no funcionan |
| 9 | WHERE clause validation (restrictions) | No se validan queries |
| 10 | SELECT evaluation (selection) | No se procesan resultados |
| 11 | `marshal/` type system | Sin serialización type-safe |
| 12 | IO util layer (mmap, RandomAccessReader) | Performance inaceptable |
| 13 | Rows/Partitions iterator framework | Read path incompleto |
| 14 | Filter subsystem (slice, names, limits) | No hay queries eficientes |
| 15 | Cache subsystem (key cache, row cache) | Read performance severa |
| 16 | Auth persistence a system_auth | Multi-node imposible |
| 17 | CRC/LZ4 en frames inter-nodo | Sin integridad en mensajes |
| 18 | Cluster de 3 nodos validado | Distributed path no probado |

### P2 — Bloquean GA

| # | Gap | Impacto |
|---|-----|---------|
| 19 | Consistent repair protocol | Incremental repair no funciona |
| 20 | Streaming network transport | No se puede mover data entre nodos |
| 21 | Paxos/LWT persistence | Se pierde en restart |
| 22 | TCM commit protocol | No hay metadata linearizable |
| 23 | SSTable lifecycle transactions | No crash-safe |
| 24 | Token allocator | Distribución desbalanceada |
| 25 | Index execution engine (SAI mínimo) | Queries sin índice |
| 26 | Schema change propagation (verb handlers) | Cambios de schema no se propagan |
| 27 | Guardrails framework | Sin safety limits |
| 28 | Virtual tables (system_views) | Sin observabilidad |
| 29 | SSTable format compatibility (o migration tool completa) | No hay path de migración |
| 30 | Transform pipeline (read path correctness) | Resultados incorrectos posibles |

---

*Generado el 2026-03-16 comparando `src/java/org/apache/cassandra/` (3,168 archivos Java) contra `rust/crates/` (209 archivos Rust).*
