# Matriz de cobertura total (continuación desde Prompt 10)

Esta matriz muestra qué zonas de Cassandra cubren los prompts 11–25 de este paquete.

| Área | Subsuperficies / ejemplos | Prompt(s) |
|---|---|---|
| Frontend cliente/servidor | Native protocol, auth challenge, prepared statements, paging, tracing, warnings, eventos, cqlsh/drivers | 12, 23 |
| CQL y semántica larga | DDL/DML/DCL/TCL, JSON, functions, collection functions, TTL/WRITETIME sobre colecciones y UDTs, UDF/UDA, triggers | 12, 21, 22 |
| Write path estándar | Mutations, batches, batchlog, hints, TTL/timestamps/tombstones, MV fanout | 13 |
| Read path | Digest reads, range reads, short-read protection, speculative retry, tombstone warnings/failures, paging | 14 |
| Storage engine on-disk | Commit log, replay, CDC, memtables clásica+trie, SSTables `big`+`bti`, backups, snapshots | 15, 24 |
| Compaction | STCS, LCS, TWCS, UCS, rewrite/recompress/upgrade flows | 15, 23, 24, 25 |
| System keyspaces y virtual tables | system/local/peers/schema/auth/traces/distributed/views/system_views y tablas auxiliares | 16 |
| Control plane clásico | Partitioners, token metadata, snitches, failure detector, gossip, internode messaging | 17 |
| Control plane nuevo | TCM/CMS, metadata log, placements, membership directory, sequences, consensus bridge | 17, 20, 24 |
| Topology changes y streaming | Bootstrap, decommission, move, removenode, replace, rebuild, streaming sessions | 18 |
| Repair y anti-entropía | Full/incremental/preview/subrange repair, Merkle trees, anti-compaction, read repair | 19 |
| Transient replication | Replica transitoria, cheap quorum/additional_write_policy, discard tras repair | 19 |
| Consistencia fuerte | LWT, Paxos, serial reads/writes, ballot cleanup | 20 |
| Counters | Write/read path específico y recovery | 20 |
| Índices tradicionales | 2i, rebuild y planner | 21 |
| SASI experimental | Feature-flag, config, postura operativa igual a baseline | 21 |
| SAI | Index build/rebuild, memtable/SSTable integration, monitoring, query planner | 21 |
| Vector search | Tipo vector, similarity functions, ANN/top-k, integración con SAI | 12, 21 |
| Materialized views | Convergencia, rebuild/backfill, interacción con write/read/topology | 13, 21 |
| Seguridad | Roles, permisos, authn/authz, TLS/mTLS, network/CIDR authorizer, DDM | 22 |
| Logging sensible | Audit logging, Audit logging 2 si aplica, Full Query Logging, replay/compare | 22, 23, 24 |
| Crypto provider | Pluggable crypto provider y config asociada | 15, 22 |
| Herramientas operativas | nodetool, cqlsh, SSTable tools, cassandra-stress, sstableloader, auditlogviewer, fqltool | 23 |
| Migración y upgrades | Mixed-version, mixed-format, mixed-cluster o dual-cluster, rollback, restore | 24 |
| Validación final | Soak, chaos, performance, fuzzing, sanitizers, release candidate | 25 |
## Criterio de lectura

- Si una fila aparece aquí, **no debe quedar fuera del inventario** de Prompt 11.
- Si una feature existe en el release/commit congelado, debe terminar en uno de estos estados:
  - cerrada,
  - explícitamente experimental,
  - trunk-only bajo gate,
  - o documentada con fallback seguro y plan de cierre.
- Ninguna feature puede desaparecer de la matriz sin justificación documental y sin ajuste del baseline congelado.

## Features especialmente fáciles de olvidar y que esta tanda fuerza a cubrir

- prepared statement invalidation / metadata ids / paging state fino
- batchlog + hinted handoff + MV fanout
- short-read protection / read repair / tombstone errors
- commitlog archiving / CDC / incremental backups / restore
- system keyspaces auxiliares y virtual tables operativas
- snitches cloud-specific / CIDR authorizer / pluggable crypto
- SASI experimental
- SAI + vector search
- audit logging vs FQL y sus herramientas
- nodetool compatibility strategy
- mixed-version / rollback / dual-cluster shadow traffic
- TCM / CMS / Accord / consensus migration si el árbol objetivo lo incluye