# Fuentes consultadas y baseline factual

## Cómo usar este archivo

- **Fuentes oficiales**: útiles para validar comportamiento esperado y diseño público del sistema.
- **Superficies del repo**: útiles para ver la estructura real del código actual.
- **Fuentes Rust**: útiles para el plan de optimización posterior.

## Hechos de baseline usados para el plan

1. El repositorio público de `apache/cassandra` muestra una estructura de alto nivel con `src`, `test`, `modules`, `conf`, `bin`, `pylib`, `tools` y `build.xml`.
2. La documentación oficial de desarrollo indica que Cassandra es un proyecto Java que se construye con **Ant**.
3. `TESTING.md` describe tres tipos principales de pruebas: **unit tests**, **integration tests** y **dtests**.
4. La documentación de arquitectura describe el storage engine como un árbol **LSM** con **commit log**, **memtables** y **SSTables**.
5. La documentación de mensajería describe el **internode messaging** moderno y la transición a **NIO/Netty** para la mensajería entre nodos.
6. La documentación de arquitectura distribuida describe el **gossip** como mecanismo de propagación de estado de membership y versiones de protocolo.
7. La especificación del protocolo nativo describe el protocolo CQL binario como un protocolo **frame-based**.
8. La documentación de 5.0 enumera features modernas como **Trie Memtables**, **Trie/BTI SSTables**, **UCS**, **vector data type**, **vector similarity functions**, **dynamic data masking** y más.
9. La configuración pública muestra que el formato SSTable por defecto sigue siendo **`big`** y que **`bti`** se soporta como formato trie-indexed en Cassandra 5.0+.
10. La configuración pública muestra dos familias de memtables: la heredada tipo **SkipList** y la nueva **TrieMemtable**.
11. El código público deja ver que el árbol actual incorpora referencias a **Paxos** y también a trabajo ligado a **Accord/consensus**, por lo que un freeze de baseline es imprescindible.
12. La documentación de operación cubre **CDC**, **audit logging**, **full query logging**, **virtual tables**, **read repair**, **repair**, **hints** y seguridad/TLS.

## Fuentes oficiales de Cassandra

- Arquitectura general: https://cassandra.apache.org/doc/stable/cassandra/architecture/index.html
- Overview de arquitectura: https://cassandra.apache.org/doc/latest/cassandra/architecture/overview.html
- Storage engine: https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html
- Dynamo / gossip / consistencia: https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html
- Guarantees / consistency / LWT: https://cassandra.apache.org/doc/stable/cassandra/architecture/guarantees.html
- Improved internode messaging: https://cassandra.apache.org/doc/latest/cassandra/architecture/messaging.html
- Native protocol: https://cassandra.apache.org/doc/stable/cassandra/reference/native-protocol.html
- Development / building / contribución: https://cassandra.apache.org/_/development/index.html
- What’s new / Cassandra 5.0: https://cassandra.apache.org/doc/latest/cassandra/new/index.html
- Cassandra 5.0 announcement: https://cassandra.apache.org/_/blog/Apache-Cassandra-5.0-Announcement.html
- Unified Compaction Strategy (UCS): https://cassandra.apache.org/doc/stable/cassandra/managing/operating/compaction/ucs.html
- Compaction overview: https://cassandra.apache.org/doc/stable/cassandra/managing/operating/compaction/overview.html
- Change Data Capture (CDC): https://cassandra.apache.org/doc/latest/cassandra/managing/operating/cdc.html
- Full Query Logging: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/fqllogging.html
- Audit logging: https://cassandra.apache.org/doc/stable/cassandra/managing/operating/audit_logging.html
- Virtual tables: https://cassandra.apache.org/doc/4.1/cassandra/new/virtualtables.html
- Read repair: https://cassandra.apache.org/doc/stable/cassandra/managing/operating/read_repair.html
- Repair: https://cassandra.apache.org/doc/4.0/cassandra/operating/repair.html
- Hints: https://cassandra.apache.org/doc/4.0/cassandra/operating/hints.html
- Security / TLS: https://cassandra.apache.org/doc/stable/cassandra/managing/operating/security.html
- Configuración `cassandra.yaml`: https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html
- CQL general: https://cassandra.apache.org/doc/stable/cassandra/developing/cql/index.html
- SAI overview: https://cassandra.apache.org/doc/stable/cassandra/developing/cql/indexing/sai/sai-overview.html
- SAI concepts / zero-copy streaming y diseño: https://cassandra.apache.org/doc/stable/cassandra/developing/cql/indexing/sai/sai-concepts.html
- Vector data type: https://cassandra.apache.org/doc/latest/cassandra/reference/vector-data-type.html
- Vector search concepts: https://cassandra.apache.org/doc/latest/cassandra/vector-search/concepts.html
- Dynamic Data Masking: https://cassandra.apache.org/doc/stable/cassandra/developing/cql/dynamic-data-masking.html

## Superficies públicas del repositorio y archivos útiles

- Repositorio principal: https://github.com/apache/cassandra
- README del repo: https://github.com/apache/cassandra/blob/trunk/README.asc
- TESTING.md: https://github.com/apache/cassandra/blob/trunk/TESTING.md
- Config actual: https://github.com/apache/cassandra/blob/trunk/conf/cassandra.yaml
- MessagingService.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/net/MessagingService.java
- StorageProxy.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/service/StorageProxy.java
- StreamSession.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/streaming/StreamSession.java
- Gossiper.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/gms/Gossiper.java
- Stage.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/concurrent/Stage.java
- Config.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/config/Config.java
- StartupChecks.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/service/StartupChecks.java
- SSTableReader.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java
- Memtable.java: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/memtable/Memtable.java

## Herramientas/proyectos útiles para validación y comparación

- Harry (fuzz/model testing): https://github.com/apache/cassandra-harry
- Cassandra diff: https://github.com/apache/cassandra-diff
- Cassandra stress (documentado, aunque hoy se recomienda evaluar NoSQLBench): https://cassandra.apache.org/doc/latest/cassandra/tooling/cassandra-stress.html

## Fuentes para el plan de optimización en Rust

- Cargo profiles: https://doc.rust-lang.org/cargo/reference/profiles.html
- Rustc PGO: https://doc.rust-lang.org/beta/rustc/profile-guided-optimization.html
- Rustc codegen options: https://doc.rust-lang.org/rustc/codegen-options/index.html
- Rust Performance Book / profiling: https://nnethercote.github.io/perf-book/profiling.html
- Rust Performance Book / general tips: https://nnethercote.github.io/perf-book/general-tips.html
- Rust Performance Book / type sizes: https://nnethercote.github.io/perf-book/type-sizes.html

## Nota final

La URL compartida de Code Wiki puede seguir siendo útil como explorador auxiliar del repo:
- https://codewiki.google/github.com/apache/cassandra

Pero para este paquete tomé como prioridad las fuentes anteriores porque son más auditables y estables como base de un plan de reescritura.