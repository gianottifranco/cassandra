# Paquete de prompts Codex para cerrar la reescritura completa de Cassandra (continuación desde el Prompt 10)

Este paquete **no reemplaza** el zip anterior; lo continúa.  
Asume que ya ejecutaste los prompts 01–10 del paquete previo y que ahora quieres empujar el proyecto desde un estado de “GA candidate / cobertura parcial alta” hasta una **cobertura total de superficie Cassandra**.

## Qué contiene

- `00_orden_de_ejecucion_y_uso.md`  
  Cómo ejecutar esta tanda y en qué orden.
- `01_prompts_codex_11_a_25.md`  
  Todos los prompts juntos en un solo archivo.
- `02_matriz_de_cobertura_total.md`  
  Qué partes de Cassandra cubre esta tanda y qué prompt cierra cada zona.
- `03_fuentes_y_baseline_de_referencia.md`  
  Fuentes oficiales y módulos del repo que conviene usar como baseline/oráculo.
- `prompts/`  
  Un archivo Markdown por prompt, listo para copiar/pegar en Codex.

## Filosofía de esta tanda

Los prompts 01–10 del paquete anterior cubrían la arquitectura mayor, el scaffold, la implementación base y una ruta inicial a GA.  
Esta tanda 11–25 está orientada a **cerrar la larga cola**:

- long tail de CQL y native protocol
- write/read path fino
- compatibilidad on-disk completa
- system keyspaces y virtual tables
- gossip / snitches / TCM / CMS / Accord si están en baseline
- topology changes / streaming / repair / transient replication
- 2i / SASI / SAI / vector search / materialized views
- seguridad completa (roles, TLS, CIDR authorizer, DDM, audit/FQL)
- toolchain de operador (nodetool, cqlsh, SSTable tools, loaders, stress)
- mixed-version / rollback / restore / shadow traffic
- chaos / soak / performance / fuzzing / release sign-off

## Recomendación de ejecución

1. Ejecuta primero el **Prompt 11** para refrescar alcance y convertir todos los gaps restantes en una matriz ejecutable.
2. Ejecuta luego los prompts 12–25 **en orden**, idealmente en ramas separadas.
3. No mezcles una fase hasta que:
   - compile,
   - pase su subset de CI,
   - y deje evidencia de paridad (diff/golden/dtests/ops tests).
4. Conserva Java como oráculo mientras no exista evidencia formal de cierre.

## Lista rápida de prompts

- **Prompt 11** — Auditoría final de cobertura, freeze definitivo de baseline y matriz ejecutable de gaps
- **Prompt 12** — Frontend completo: native protocol, CQL long tail, prepared statements, eventos, tracing, UDF/UDA y triggers
- **Prompt 13** — Write path estándar completo: mutaciones, consistency levels, batchlog, hints, materialized-view fanout y edge cases
- **Prompt 14** — Read path completo: digest reads, short-read protection, speculative retry, paging, tombstones y semántica de respuesta
- **Prompt 15** — Storage engine final: commit log completo, memtables clásicas y trie, SSTables `big` y `bti`, compaction families, CDC, snapshots y backups
- **Prompt 16** — System keyspaces, schema distribution, tablas de sistema, virtual tables, guardrails y superficies operativas internas
- **Prompt 17** — Control plane completo: partitioners, ring metadata, snitches, failure detector, gossip, internode messaging, TCM/CMS y bridge de compatibilidad
- **Prompt 18** — Streaming y topology changes completos: bootstrap, decommission, removenode, replace, move, rebuild, cleanup y transferencia de rangos
- **Prompt 19** — Repair y anti-entropía completos: full/incremental/preview repair, Merkle trees, anti-compaction, read repair, transient replication e integración con hints
- **Prompt 20** — Consistencia fuerte y transacciones: LWT/Paxos completos, counters, limpieza de estado, Accord y consenso/migración si existen en baseline
- **Prompt 21** — Índices y aceleradores de consulta completos: 2i, SASI, SAI, vistas materializadas, vector search, planners y rebuilds
- **Prompt 22** — Seguridad y cumplimiento completos: authn/authz, TLS/mTLS, network/CIDR authorizers, DDM, audit logging, FQL y crypto providers
- **Prompt 23** — Tooling y management plane completos: cqlsh, nodetool, SSTable tools, cassandra-stress, sstableloader, auditlogviewer, fqltool y scripts de despliegue
- **Prompt 24** — Upgrades, migración y rollback completos: mixed-version, mixed-format, dual-cluster/shadow, restore, CDC continuity y validación de datos
- **Prompt 25** — Cierre final: chaos, soak, performance, fuzzing, seguridad, release candidate y criterio de eliminación del Java oráculo


## Nota importante

Varios prompts contemplan features:
- **estables** de Cassandra 4.x/5.x,
- **experimentales** que siguen existiendo bajo config/feature flags,
- y **módulos nuevos o trunk-only** visibles en árboles actuales (`tcm`, `service.consensus`, `service.accord`).

La regla es simple: si la baseline congelada lo contiene, el trabajo debe **clasificarlo y cerrarlo o aislarlo explícitamente**, nunca ignorarlo en silencio.
