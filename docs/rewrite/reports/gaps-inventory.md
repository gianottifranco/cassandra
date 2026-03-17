# Inventario Completo de Gaps — Cassandra Java → Rust

**Fecha**: 2026-03-17
**Baseline Java**: commit `076c6f11364645bbb43360f013bee6f50a099185` (trunk, 2026-03-15)
**Estado del workspace**: 20 crates Rust, ~89k LOC, 3,300+ tests

---

## Resumen Ejecutivo

| Categoría | Total | Cerrados | Abiertos |
|-----------|-------|----------|----------|
| Gap Guards (tests centinela) | 21 | 4 | 17 |
| Stubs (implementaciones parciales) | 61 | — | 61 |
| TODOs | 7 | — | 7 |
| "Not implemented" | 8 | — | 8 |
| **Total marcadores** | **97** | **4** | **93** |

**Veredicto**: El sistema funciona como **servidor single-node**. Para ser un reemplazo completo de Apache Cassandra faltan los componentes distribuidos (gossip, StorageProxy) y ~93 items de funcionalidad.

---

## 1. Gap Guards — Tests Centinela (21 total)

Cada gap guard es un test `#[ignore]` en `cassandra-diff-tests/src/gap_guards.rs` que fallará hasta que la funcionalidad se implemente. Son el tracking formal de lo que falta.

### 1.1 CERRADOS (4) — Ya implementados

| # | Gap Guard | Módulo | Cerrado en |
|---|-----------|--------|------------|
| 1 | `gap_guard_trie_index` | `cassandra_storage::tries` | Phase 5 |
| 2 | `gap_guard_db_filters` | `cassandra_storage::filter` | Phase 5 |
| 3 | `gap_guard_row_transformations` | `cassandra_storage::transform` | Phase 5 |
| 4 | `gap_guard_concurrency_stages` | `cassandra_common::Stage` | Phase 25 |

### 1.2 CQL — Lenguaje de Consulta (4 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 5 | `gap_guard_cql_functions` | Funciones builtin (`token()`, `now()`, `uuid()`, `cast()`), funciones de agregación (`count()`, `sum()`, `avg()`, `min()`, `max()`), UDF (requiere sandbox WASM), UDA | `cql3.functions` | **ALTO** — Muchas queries de producción usan estas funciones |
| 6 | `gap_guard_cql_permission_statements` | `GRANT`, `REVOKE`, `CREATE ROLE`, `ALTER ROLE`, `DROP ROLE`, `LIST ROLES`, `LIST PERMISSIONS` | `cql3.statements` | MEDIO — Necesario para multi-tenancy |
| 7 | `gap_guard_cql_constraints` | `CHECK` constraints | `cql3.constraints` | BAJO — Feature trunk-only |
| 8 | `gap_guard_cql_advanced_restrictions` | Restricciones multi-columna, restricción `token()`, operadores `CONTAINS`, `LIKE` | `cql3.restrictions` | **ALTO** — Queries con WHERE complejos fallarán |

### 1.3 Storage & Formatos (2 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 9 | `gap_guard_sstable_java_compat` | Leer SSTables formato Java (big-format). No hay compatibilidad binaria. | `io.sstable` | **CRÍTICO** — No se puede migrar data existente de Java sin conversión |
| 10 | `gap_guard_compaction_execution` | Task runner de compaction. Los strategy stubs existen (STCS, LCS, TWCS, UCS) pero no se ejecutan automáticamente. | `db.compaction` | **ALTO** — Sin auto-compaction, los SSTables crecen sin límite |

### 1.4 Cache & Guardrails (2 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 11 | `gap_guard_caching` | KeyCache, RowCache, CounterCache, SerializingCache, BloomFilter | `cache` | **ALTO** — Performance degradada sin caches |
| 12 | `gap_guard_guardrails` | Límites de tablas/columnas, warnings de tamaño de partición, límites de complejidad de queries | `db.guardrails` | MEDIO — Sin protecciones operacionales |

### 1.5 Queries Distribuidas (2 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 13 | `gap_guard_paging` | QueryPager, serialización de paging state, result sets multi-página | `service.pager` | **CRÍTICO** — Queries grandes no pueden paginar |
| 14 | `gap_guard_aggregation` | `GROUP BY`, manejo de estado de funciones agregadas | `db.aggregation` | MEDIO — Queries analíticas no funcionan |

### 1.6 Interoperabilidad de Cluster (2 abiertos) — **BLOQUEANTE PARA RC**

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 15 | `gap_guard_gossip_wire_compat` | Formato wire de gossip NO es compatible con Java. No hay gossip loop (GossipDigestSyn/Ack/Ack2), ni failure detection (Phi accrual), ni membership management. | `gms` | **CRÍTICO** — Los nodos no se descubren entre sí |
| 16 | `gap_guard_internode_wire_compat` | Protocolo de mensajería inter-nodo NO es wire-compatible. Sin connection pool ni flow control. | `net` | **CRÍTICO** — No hay comunicación entre nodos |

### 1.7 Seguridad & Logging (2 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 17 | `gap_guard_fql` | Full Query Logging (chronicle-queue based en Java, necesita equivalente file-based) | `fql` | BAJO — Feature de auditoría |
| 18 | `gap_guard_ldap_kerberos_auth` | LdapAuthenticator, integración Kerberos | `auth` | MEDIO — Necesario para enterprise |

### 1.8 Tooling (3 abiertos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 19 | `gap_guard_nodetool_commands` | ~140 comandos nodetool faltantes (17 implementados de ~157) | `tools.nodetool` | MEDIO — Operaciones administrativas limitadas |
| 20 | `gap_guard_sstable_offline_tools` | SSTableExport, Scrubber, Splitter, Upgrader, Verifier, MetadataViewer, LevelResetter, RepairSetSetter | `tools` | MEDIO — Sin herramientas offline para SSTables |
| 21 | `gap_guard_virtual_tables` | ~60 virtual tables (5 implementadas de ~65) | `db.virtual` | MEDIO — Diagnóstico limitado via CQL |

### 1.9 Experimental / Trunk-Only (4 abiertos — diferidos)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 22 | `gap_guard_tcm` | Transactional Cluster Metadata — bajo iteración activa en trunk | `tcm` | Diferido — API todavía fluida |
| 23 | `gap_guard_accord` | Accord — motor de transacciones distribuidas next-gen | `service.accord` | Diferido — Experimental |
| 24 | `gap_guard_consensus` | Servicio de consenso | `service.consensus` | Diferido — Experimental |
| 25 | `gap_guard_journal` | Subsistema de journal — nuevo, en evolución | `journal` | Diferido — Trunk-only |

### 1.10 Utilidades (1 abierto)

| # | Gap Guard | Qué falta | Ref Java | Impacto |
|---|-----------|-----------|----------|---------|
| 26 | `gap_guard_tracing_storage` | Almacenamiento de tracing distribuido (keyspace `system_traces`) | `tracing` | BAJO — Contexto de trace se propaga pero no se persiste |

---

## 2. Stubs — Implementaciones Parciales (61 marcadores)

Código que existe pero retorna datos falsos, no-ops, o implementaciones simplificadas.

### 2.1 cassandra-admin (6 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `handlers_stats.rs` | 32 | Table stats retorna datos stub cuando no hay virtual tables |
| `handlers_stats.rs` | 104 | Proxy histogram con percentiles stub |
| `handlers_stats.rs` | 134 | Latency histogram retorna zero-count stub |
| `handlers_stats.rs` | 151 | Top-partition stats son stubs (no hay live tracking) |
| `handlers_stats.rs` | 197 | Test para stats stub |
| `handlers_compaction.rs` | 261 | Compaction history es stub |

### 2.2 cassandra-coordinator (10 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `consensus/router.rs` | 313 | Accord mode disabled, stub que da error |
| `write.rs` | 979 | Trigger augmentation es stub — no se disparan triggers |
| `write.rs` | 1588 | Test de write con trigger stub |
| `write.rs` | 1596 | TriggerManager es stub |
| `read/repair.rs` | 147 | Read repair usa internode wire stub |
| `read/repair.rs` | 150 | Payload de repair simplificado para test |
| `read/mod.rs` | 375 | Latency percentiles hardcodeados a 50 |
| `read/mod.rs` | 567 | Read de replicas via MessagingService no wired |
| `read/planners.rs` | 33 | Schema catalog para planner es stub |
| `verb_handlers/read_handler.rs` | 81,126 | Lectura de storage engine es stub |

### 2.3 cassandra-cluster-metadata (3 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `snitch.rs` | 420 | Ec2Snitch / Ec2MultiRegionSnitch son stubs |
| `snitch.rs` | 769 | Defaults hardcodeados en vez de metadata API |
| `replication.rs` | 469-478 | TransientReplicationStrategy es stub (experimental) |

### 2.4 cassandra-cql (2 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `planner.rs` | 15 | QueryPlan es para ejecución stub |
| `udf.rs` | 27 | WASM sandbox para UDF es stub feature-gated |

### 2.5 cassandra-native-protocol (3 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `auth.rs` | 104-106 | PasswordAuthenticator acepta cualquier credencial no-vacía (stub) |

### 2.6 cassandra-security (4 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `masking.rs` | 109 | Hash mask usa placeholder en vez de SHA-256 |
| `roles.rs` | 118 | Persistencia de roles a storage no implementada |
| `encryption_at_rest.rs` | 67 | Chunked decrypt no implementado |
| `authz.rs` | 275 | Persistencia de permisos a `system_auth.role_permissions` no implementada |

### 2.7 cassandra-server (5 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `auth.rs` | 268 | `scan_partitions` para iteración de tabla no existe |
| `executor.rs` | 135 | Dispatch de funciones CQL es no-op |
| `executor.rs` | 142 | TRUNCATE es stub (solo log) |
| `executor.rs` | 638 | ALTER MATERIALIZED VIEW es stub |
| `startup_checks.rs` | 115-122 | Disk space check retorna MAX (siempre pasa) |

### 2.8 cassandra-storage (14 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `triggers.rs` | 59,75,91 | TriggerManager es no-op completo (requiere WASM/FFI) |
| `udf.rs` | 102,119,124,133,148 | UdfManager es no-op (requiere WASM sandbox) |
| `materialized_views.rs` | 26-27 | MV builder (backfill) y read-repair integration son TODO |
| `commitlog/encrypted.rs` | 3,14 | Encrypted commitlog es stub (escribe plaintext) |
| `index/legacy.rs` | 37 | Persistencia de índices a SSTables es TODO |
| `index/sai/vector_index.rs` | 36 | Vector index tiene TODOs |
| `index/sai/mod.rs` | 48 | SAI tiene TODOs |
| `engine.rs` | 778 | SAI rebuild via StorageEngine no implementado |
| `sstable/filtered_scanner.rs` | 191,195 | Operadores MapEquality, Custom no implementados |

### 2.9 cassandra-tools (4 stubs)

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `main.rs` | 759 | Comando stub — "implement via admin API" |
| `main.rs` | 768 | Comando stub |
| `main.rs` | 772 | Comando stub |
| `main.rs` | 776 | Comando stub |

### 2.10 Otros (10 stubs)

| Crate | Archivo | Descripción |
|-------|---------|-------------|
| `cassandra-accord` | `service.rs:137` | Accept phase sin conflict resolution |
| `cassandra-common` | `lib.rs:57-58` | Bloom filter y thread pool abstractions referenciados como GAP |
| `cassandra-migration` | `sstable_import.rs:362` | Conversión binaria Java→Rust SSTable referenciada como GAP |
| `cassandra-repair` | `coordinator.rs:228` | Repair type hardcodeado a Full |
| `cassandra-diff-tests` | `shadow_traffic.rs:124,300` | Shadow traffic replay no conectado a QueryExecutor |
| `cassandra-diff-tests` | `golden.rs:163-167` | Test de fixtures stub |
| `cassandra-server` | `query_processor.rs:75` | Schema version hardcodeado a 0 |

---

## 3. Componentes Mayores Faltantes (No cubiertos por gap guards individuales)

Estos son subsistemas completos que no existen o están vacíos:

| Componente | Descripción | LOC Estimado | Prioridad |
|------------|-------------|--------------|-----------|
| **StorageProxy** | Coordinator de reads/writes distribuidos. Token → replica selection → consistency enforcement. | ~10,000 | P0 |
| **Gossip Loop** | Ronda periódica de gossip, failure detection, cluster membership. | ~5,000 | P0 |
| **Hints** | Almacenamiento y replay de hints para nodos caídos. Schema existe, storage no conectado. | ~3,000 | P1 |
| **Streaming** | Transferencia bulk de datos entre nodos (bootstrap, decommission, repair). | ~5,000 | P1 |
| **Repair Protocol** | Intercambio de Merkle trees, anti-entropy repair. Scaffold existe. | ~4,000 | P1 |
| **Read Repair** | Reparación durante lectura (digest mismatch → full data repair). Stub existe. | ~2,000 | P1 |
| **Batch Log** | Batchlog para atomic batches distribuidos. | ~2,000 | P2 |
| **Materialized Views** | Builder, backfill, y maintenance de vistas materializadas. | ~4,000 | P2 |
| **CDC Reader** | Consumer de CDC logs. Writer existe, reader no. | ~2,000 | P2 |

---

## 4. Mapa de Calor por Crate

| Crate | Funcionalidad | Completitud | Gaps Abiertos |
|-------|---------------|-------------|---------------|
| `cassandra-common` | Utilidades, tipos, errores | 95% | Bloom filter, thread pools |
| `cassandra-config` | Configuración YAML | 98% | — |
| `cassandra-types` | Sistema de tipos CQL | 95% | — |
| `cassandra-schema` | DDL, schema management | 90% | — |
| `cassandra-native-protocol` | Codec protocolo CQL v4/v5 | 95% | Auth stub |
| `cassandra-cql` | Parser, planner, execution | 70% | Funciones, constraints, restrictions |
| `cassandra-storage` | Commitlog, memtable, SSTable, compaction | 80% | Auto-compaction, triggers, UDF, MV, encrypted CL, SAI |
| `cassandra-cluster-metadata` | Token ring, snitch, replication | 75% | Ec2Snitch, transient replication |
| `cassandra-messaging` | Mensajería inter-nodo | 40% | Wire compat, connection pool |
| `cassandra-coordinator` | Read/write coordination | 30% | **StorageProxy**, paging, read repair |
| `cassandra-repair` | Anti-entropy repair | 25% | Merkle trees, repair protocol |
| `cassandra-streaming` | Bulk data transfer | 20% | Streaming protocol |
| `cassandra-security` | Auth, TLS, encryption | 60% | LDAP, Kerberos, role persistence |
| `cassandra-admin` | HTTP admin API | 70% | Stats reales, compaction history |
| `cassandra-server` | Servidor CQL, startup | 75% | Disk check, TRUNCATE, schema version |
| `cassandra-tools` | nodetool CLI | 40% | ~140 comandos |
| `cassandra-io` | I/O, mmap | 90% | — |
| `cassandra-migration` | Herramientas de migración | 85% | SSTable import binario |
| `cassandra-diff-tests` | Tests diferenciales | 90% | — |
| `cassandra-accord` | Transacciones distribuidas | 10% | Experimental, diferido |

---

## 5. Priorización para Roadmap

### P0 — Bloqueantes para cualquier uso multi-nodo

1. **Gossip loop** — Sin esto, cada nodo es una isla
2. **StorageProxy** — Sin esto, no hay reads/writes distribuidos
3. **Query paging** — Sin esto, queries grandes fallan
4. **Auto-compaction execution** — Sin esto, SSTables crecen sin límite
5. **Caching** (al menos KeyCache) — Sin esto, performance inaceptable

### P1 — Necesarios para producción

6. CQL functions (builtins: `now()`, `token()`, `uuid()`, aggregaciones)
7. CQL advanced restrictions (`CONTAINS`, `LIKE`, multi-column)
8. Hints storage y replay
9. Streaming protocol (bootstrap, decommission)
10. Repair protocol (Merkle tree exchange)
11. Auth real (verificación contra `system_auth.roles`)
12. Role/permission persistence
13. Nodetool commands principales (~30 críticos)

### P2 — Necesarios para feature parity

14. SSTable Java compat (lectura de formato big)
15. Full Query Logging
16. LDAP/Kerberos auth
17. Virtual tables (~60)
18. SSTable offline tools
19. Materialized views
20. CDC reader
21. Guardrails
22. Distributed tracing storage
23. Encrypted commitlog
24. SAI/Vector index completion

### P3 — Experimental / Diferido

25. TCM (Transactional Cluster Metadata)
26. Accord (distributed transactions)
27. Consensus service
28. Journal subsystem
29. CQL CHECK constraints
30. Triggers (requiere WASM/FFI plugin system)
31. UDF/UDA (requiere WASM sandbox)

---

## 6. Métricas de Cobertura

```
Total líneas de código Rust:          ~89,000
Tests:                                 3,300+
Gap guards cerrados:                   4 / 21  (19%)
Stubs en código:                       61
TODOs pendientes:                      7
Crates con >80% completitud:          10 / 20  (50%)
Crates con <50% completitud:           4 / 20  (20%)
Componentes mayores faltantes:         9
LOC estimado para feature parity:     ~37,000
```
