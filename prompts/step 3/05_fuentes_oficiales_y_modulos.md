# Fuentes oficiales y módulos del repo sugeridos para esta serie

Este archivo no sustituye al gap analysis, lo complementa.  
Usa estas fuentes para validar comportamiento observable y localizar implementaciones Java de referencia.

## Documentación oficial
- Native protocol / reference:
  - https://cassandra.apache.org/doc/latest/cassandra/reference/native-protocol.html
- Storage engine:
  - https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html
- Improved internode messaging:
  - https://cassandra.apache.org/doc/latest/cassandra/architecture/messaging.html
- Improved streaming:
  - https://cassandra.apache.org/doc/latest/cassandra/architecture/streaming.html
- Repair:
  - https://cassandra.apache.org/doc/latest/cassandra/managing/operating/repair.html
- Topology changes:
  - https://cassandra.apache.org/doc/latest/cassandra/managing/operating/topo_changes.html
- Nodetool:
  - https://cassandra.apache.org/doc/latest/cassandra/managing/tools/nodetool/nodetool.html
- Stable docs index (CQL, SAI, SASI, materialized views, triggers, JSON, security, guardrails, virtual tables, etc.):
  - https://cassandra.apache.org/doc/stable/
- New features / release notes docs:
  - https://cassandra.apache.org/doc/latest/cassandra/new/index.html

## Módulos Java que esta serie toca especialmente
- `src/java/org/apache/cassandra/transport/**`
- `src/java/org/apache/cassandra/cql3/**`
- `src/java/org/apache/cassandra/db/**`
- `src/java/org/apache/cassandra/io/**`
- `src/java/org/apache/cassandra/net/**`
- `src/java/org/apache/cassandra/gms/**`
- `src/java/org/apache/cassandra/dht/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/service/**`
- `src/java/org/apache/cassandra/tcm/**`
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/repair/**`
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/security/**`
- `src/java/org/apache/cassandra/schema/**`
- `src/java/org/apache/cassandra/index/**`
- `src/java/org/apache/cassandra/tools/**`

## Consejo práctico
Antes de cada prompt:
1. abre el gap analysis,
2. localiza las clases Java listadas,
3. localiza los crates Rust impactados,
4. ejecuta tests/harnesses existentes,
5. y deja evidencia antes/después.