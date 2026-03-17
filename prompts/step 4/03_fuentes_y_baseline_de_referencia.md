# Fuentes y baseline de referencia sugeridas

Este archivo no reemplaza la inspección directa del repo.  
Sirve como mapa rápido de las fuentes oficiales y de los módulos del árbol que conviene usar como oráculo durante la tanda 11–25.

## Documentación oficial / superficies públicas

- New Features (5.0 y 4.x):  
  https://cassandra.apache.org/doc/latest/cassandra/new/index.html
- Índice general de documentación estable (CQL, SAI, 2i, materialized views, JSON, triggers, SASI, vector search, configuración):  
  https://cassandra.apache.org/doc/stable/
- Storage engine:  
  https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html
- Operating Cassandra (backups, CDC, compaction, hints, logging, repair, topology changes, transient replication, virtual tables):  
  https://cassandra.apache.org/doc/stable/cassandra/managing/operating/index.html
- Unified Compaction Strategy (UCS):  
  https://cassandra.apache.org/doc/stable/cassandra/managing/operating/compaction/ucs.html
- Change Data Capture (CDC):  
  https://cassandra.apache.org/doc/latest/cassandra/managing/operating/cdc.html
- Dynamic Data Masking (DDM):  
  https://cassandra.apache.org/doc/stable/cassandra/developing/cql/dynamic-data-masking.html
- Storage-Attached Indexing (SAI) concepts:  
  https://cassandra.apache.org/doc/stable/cassandra/developing/cql/indexing/sai/sai-concepts.html
- SAI read/write path:  
  https://cassandra.apache.org/doc/stable/cassandra/developing/cql/indexing/sai/sai-read-write-paths.html
- Vector Search concepts:  
  https://cassandra.apache.org/doc/latest/cassandra/vector-search/concepts.html
- Vector Search data modeling / indexing:  
  https://cassandra.apache.org/doc/latest/cassandra/vector-search/data-modeling.html
- Audit logging:  
  https://cassandra.apache.org/doc/stable/cassandra/managing/operating/auditlogging.html
- Full query logging:  
  https://cassandra.apache.org/doc/latest/cassandra/managing/operating/fqllogging.html
- Virtual tables:  
  https://cassandra.apache.org/doc/4.1/cassandra/new/virtualtables.html
- Transient replication:  
  https://cassandra.apache.org/doc/4.0/cassandra/new/transientreplication.html
- nodetool docs:  
  https://cassandra.apache.org/doc/latest/cassandra/managing/tools/nodetool/nodetool.html

## Árbol del repo que conviene tratar como oráculo técnico

### Surface runtime “clásica”
- `src/java/org/apache/cassandra/transport/**`
- `src/java/org/apache/cassandra/cql3/**`
- `src/java/org/apache/cassandra/db/**`
- `src/java/org/apache/cassandra/io/sstable/**`
- `src/java/org/apache/cassandra/service/**`
- `src/java/org/apache/cassandra/net/**`
- `src/java/org/apache/cassandra/gms/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/repair/**`
- `src/java/org/apache/cassandra/index/**`
- `src/java/org/apache/cassandra/auth/**`

### Surface nueva / trunk-oriented / control plane evolutivo
- `src/java/org/apache/cassandra/tcm/**`
- `src/java/org/apache/cassandra/service/consensus/**`
- `src/java/org/apache/cassandra/service/accord/**`

### Tooling / operación
- `src/java/org/apache/cassandra/tools/**`
- `tools/bin/**`
- `conf/**`
- `test/**`
- dtests / upgrade tests / stress / harnesses

## Regla práctica

Usa Prompt 11 para convertir esta referencia en:
- baseline exacta,
- matriz ejecutable,
- clasificación estable/experimental/trunk-only,
- y cobertura obligatoria por prompt.