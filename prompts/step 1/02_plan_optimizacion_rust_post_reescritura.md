# Plan completo de optimización para Rust después de alcanzar paridad funcional

## Objetivo

Este plan asume que ya existe una implementación Rust de Cassandra **correcta y funcional**. A partir de ahí, la misión deja de ser “portar features” y pasa a ser esta:

- bajar latencia p95/p99/p99.9;
- aumentar throughput sin sacrificar predictibilidad;
- reducir asignaciones, copias, lock contention y read/write amplification;
- endurecer I/O y compaction bajo cargas reales;
- sostener seguridad, observabilidad y operabilidad sin sobrecoste accidental;
- convertir performance en un gate formal de release.

## Reglas de oro

1. **No optimices a ciegas**. Perfil primero.
2. **No sacrifiques correctness por velocidad**. La paridad funcional manda.
3. **No confundas benchmark bonito con mejora real**. Valida micro, macro y end-to-end.
4. **No metas diez optimizaciones a la vez**. Cambia una variable, mide, documenta y conserva rollback.
5. **No copies el modelo de costos del JVM**. Rust te permite otro diseño; aprovéchalo, pero con datos.

## KPIs recomendados

### Latencia
- p50, p95, p99, p99.9 por query type, CL y tamaño de payload;
- latencia durante compaction, repair, bootstrap y failover;
- latencia de LWT/CAS por nivel de contención.

### Throughput
- ops/s sostenidas en writes, reads, mixed, range scans, streaming y repair;
- throughput degradado controlado bajo pérdida parcial de nodos.

### Consumo
- CPU por subsistema;
- memoria RSS y memoria útil por request;
- bytes asignados/copiados por request;
- IOPS, throughput de disco, write amplification, read amplification;
- presión de red y tamaños de colas.

### Salud operativa
- compaction debt;
- flush lag;
- backlog de hints;
- duración y tasa de progreso de repairs;
- tiempo de bootstrap/decommission;
- overhead de TLS, audit logging, FQL y tracing.

## Pipeline recomendado de performance

1. **Establecer baseline**: una versión Rust estable y una baseline Java comparables.
2. **Instrumentar**: métricas, tracing, perfiles CPU/memoria/I/O.
3. **Localizar hot paths**: protocolo, storage, coordinator, compaction, streaming, LWT, índices.
4. **Optimizar por capas**: frontend -> memoria -> concurrencia -> I/O -> estructuras avanzadas -> toolchain.
5. **Automatizar budgets**: CI nocturna, dashboards, alertas y gates por regresión.

## 1. Principio rector: medir antes de tocar

**Líneas guía**

- No optimizar por intuición. Toda decisión debe venir respaldada por perfiles, counters, trazas y benchmarks reproducibles.
- Separar optimización de correctness. Primero paridad, luego velocidad, luego especialización por workload.
- Definir perfiles de referencia: write-heavy, read-heavy, mixed, range scans, repair, compaction, bootstrap, LWT/CAS, CDC y vector/SAI si se soportan.

**Acciones concretas**

- Crear un `perf_baseline/` con datasets, topologías, scripts y budgets versionados.
- Tener siempre tres escenarios: microbench, macrobench de nodo aislado y bench distribuido end-to-end.
- Emitir reportes comparativos Rust vs baseline Java y Rust vs Rust-anterior.

## 2. Instrumentación y observabilidad de performance

**Líneas guía**

- Instrumentar latencias p50/p95/p99/p99.9 por tipo de operación y por stage/shard.
- Medir CPU por subsistema, bytes asignados por request, lock contention, cola por executor, flush lag, compaction debt, bytes streamados y tamaño de caches.
- Correlacionar eventos: spikes de latencia con compaction, repair, gossip churn, backlog de hints, presión de WAL o presión de file cache.

**Acciones concretas**

- Añadir spans estructurados y métricas de alta cardinalidad solo donde sirvan para depuración controlada.
- Mantener exporters para Prometheus/OpenTelemetry y dumps de perfil para `perf`, flamegraphs y heap profilers.
- Introducir perfiles periódicos automatizados en CI nocturna y en canaries.

## 3. Toolchain y perfiles de compilación

**Líneas guía**

- Definir perfiles Cargo separados: `release`, `release-lto`, `release-pgo`, `bench`, `canary`, `debug-perf`.
- Usar LTO y ajuste de `codegen-units` solo después de medir el trade-off entre tiempo de build y throughput/latencia.
- Aplicar PGO sobre workloads representativos del mundo real; no sobre un benchmark artificial único.

**Acciones concretas**

- Versionar en el repo la configuración de perfiles y el procedimiento para generar y consumir datos PGO.
- Usar `target-cpu=native` solo para benchmarks o builds controlados; para distribución pública, mantener targets reproducibles.
- Probar `panic=abort`, stripping selectivo y símbolos suficientes para profiling en builds de canary.

## 4. Modelo de concurrencia y sharding

**Líneas guía**

- Evitar que la versión Rust replique ciegamente el modelo de thread pools del Java actual; rediseñar alrededor de ownership claro y minimizar lock sharing.
- Preferir snapshots inmutables de metadato, colas por shard, single-writer where possible y paso de mensajes sobre locks gruesos.
- Sharding recomendable por token range, CPU core o combinación core+table según el perfil de acceso.

**Acciones concretas**

- Revisar todos los puntos con `Arc<Mutex<...>>` o `RwLock` en rutas calientes y convertirlos, cuando sea posible, en ownership local + message passing.
- Crear métricas de contention y wait time por lock/queue.
- Introducir work stealing solo si mejora el tail latency y no rompe locality.

## 5. Optimización del protocolo y del frontend CQL

**Líneas guía**

- Usar decodificación/encodificación zero-copy donde el protocolo lo permita.
- Reducir al mínimo las conversiones entre `Bytes`, `Vec<u8>`, `String` y estructuras intermedias.
- Internar o cachear identificadores, prepared metadata y planes de consulta cuando la invalidez de esquema esté bien resuelta.

**Acciones concretas**

- Perfilar parseo de frames, decode de valores y materialización de rows.
- Evitar clones de payloads grandes; preferir slices/referencias contadas o buffers compartidos bien acotados.
- Separar parseo, planning y ejecución para poder perfilar y cachear por etapa.

## 6. Memoria y layout de datos

**Líneas guía**

- Diseñar tipos compactos y cache-friendly; controlar tamaños de structs, enums y alignment.
- Reservar estructuras off-heap o mmapped cuando la presión de memoria justifique reducir RSS heap-like del proceso.
- Eliminar asignaciones efímeras en rutas de request, compaction y merge iterators.

**Acciones concretas**

- Medir tamaño real de tipos clave y reducir aquellos que se copian o viven masivamente.
- Introducir arenas/bump allocators por request o por operación de compaction cuando la vida útil sea bien delimitada.
- Benchmarkear allocators alternativos solo después de medir patrones reales de asignación.

## 7. I/O de disco: commit log, SSTables y compaction

**Líneas guía**

- Optimizar write path con batching, group commit, preasignación de segmentos y políticas de fsync medibles.
- Aprovechar mmap o lecturas directas según acceso y plataforma, pero sin asumir que una política sirve para todos los workloads.
- Hacer de compaction un scheduler explícito con budgets, fairness y visibilidad operacional.

**Acciones concretas**

- Mantener perfiles separados para WAL, flush, compaction read, compaction write y streaming I/O.
- Agregar prefetch/readahead selectivo para scans y compactions largas.
- Medir y reducir write amplification, read amplification y churn de archivos temporales.

## 8. Read path, caches y estructuras auxiliares

**Líneas guía**

- Optimizar Bloom filters, índices de partición, summaries, row cache y key cache con enfoque data-oriented.
- Evitar cachear objetos demasiado pesados o altamente volátiles; cachear representaciones ya listas para el acceso dominante.
- Separar hit-rate útil de hit-rate cosmético; una cache que evita poco trabajo real no merece memoria.

**Acciones concretas**

- Medir coste por miss y por hit, no solo porcentajes.
- Perfilar deserialización, saltos de índice, comparación de clustering keys y merge de SSTables.
- Introducir warmup y invalidación explícita en benchmarks para no autoengañarse.

## 9. Networking internodo y streaming

**Líneas guía**

- Reducir syscalls y copias innecesarias: coalescing de mensajes, write batching, pooling de buffers y backpressure real.
- Asegurar que el canal internodo y el canal de streaming tengan límites, budgets y telemetry independientes.
- Optimizar TLS solo después de medir su coste real; a veces el cuello no está ahí.

**Acciones concretas**

- Perfilar serialize/deserialize, colas de envío, latencia de network round-trips y payload sizes.
- Usar vectored I/O y sendfile/splice donde encaje y la plataforma lo permita.
- Aplicar compresión selectiva, no doctrinal.

## 10. Storage engine moderno: TrieMemtable, BTI y UCS

**Líneas guía**

- Una vez conseguida la paridad con el camino conservador, introducir optimizaciones estructurales por oleadas: memtable trie, SSTables BTI, UCS.
- Cada cambio estructural debe validarse contra correctness, performance y coste operacional (debuggability, repair, streaming, tooling).
- No mezclar en el mismo sprint cambios de formato on-disk con cambios de scheduler o cambios de índices.

**Acciones concretas**

- Implementar primero interfaces estables para memtables y formatos SSTable; cambiar implementación después.
- Mantener herramientas de verificación y conversión entre formatos cuando sea viable.
- Acompañar cada optimización estructural con benchmarks de regresión específicos.

## 11. Índices, SAI y vector search

**Líneas guía**

- Tratar SAI y vector search como subsistemas de performance altamente sensibles, no como simple feature de catálogo.
- Optimizar construcción de índices, consultas, streaming de índices y memoria residente por índice.
- Separar benchmarks de precisión funcional, throughput de ingestión y latencia de búsqueda.

**Acciones concretas**

- Medir write amplification introducida por índices.
- Perfilar postings, tries, filtros y scans según tipo de consulta.
- Asegurar que el read path sin índices no paga costes de diseño pensados solo para SAI.

## 12. Seguridad y observabilidad sin coste accidental

**Líneas guía**

- Evitar que audit logging, FQL, tracing o métricas introduzcan clones y serializaciones redundantes en la ruta caliente.
- Aislar formatos de log y sinks del path principal con colas, backpressure y sampling controlado.
- Medir explícitamente el overhead de cada feature operativa.

**Acciones concretas**

- Tener benchmarks con logging off/on, tracing off/on, TLS off/on, FQL off/on.
- Hacer opt-in o dynamic throttling de observabilidad costosa.
- Aplicar redacción/masking sin recomputar resultados completos cuando no haga falta.

## 13. CI de performance y budgets

**Líneas guía**

- Convertir performance en una disciplina de release: budgets, umbrales y reportes automatizados.
- Distinguir regresiones de ruido con benchmarks repetibles y controlados.
- Mantener canaries y soak tests como parte del proceso, no como actividad esporádica.

**Acciones concretas**

- Ejecutar macrobenchmarks periódicos y comparar contra ventanas históricas.
- Bloquear merges que crucen budgets críticos en rutas P0.
- Tener dashboards por versión y por feature flag.

## 14. Orden recomendado de optimización

**Líneas guía**

- Primero: eliminar asignaciones evitables, locks innecesarios y copias superfluas.
- Segundo: estabilizar scheduling, colas y budgets de I/O/compaction/streaming.
- Tercero: aplicar LTO/PGO y ajustes de toolchain.
- Cuarto: cambiar estructuras base (TrieMemtable, BTI, caches nuevas, sharding profundo) solo con datos sólidos.

**Acciones concretas**

- No saltarse este orden salvo evidencia fuerte.
- Acompañar cada iteración con before/after y rollback plan.
- Cerrar cada optimización con documentación y explicación del porqué, no solo con un benchmark aislado.


## Configuración sugerida de perfiles Cargo

Usa perfiles diferenciados. Un ejemplo razonable de partida:

```toml
[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
incremental = false
debug = 1

[profile.bench]
inherits = "release"
debug = 1

[profile.release-pgo]
inherits = "release"
lto = "fat"
codegen-units = 1
```

Notas:

- `panic = "abort"` puede dar beneficios en tamaño y a veces en rendimiento, pero evalúalo con cuidado por implicaciones de diagnóstico.
- `target-cpu=native` es muy útil para benchmarking interno; para builds redistribuibles, úsalo solo si controlas el target.
- PGO solo vale la pena si el perfil proviene de workloads representativos.

## Orden de ataque recomendado

### Semana/iteración 1 de performance
- instrumentación real,
- flamegraphs,
- top hot paths,
- budgets iniciales,
- eliminación de asignaciones/copies obvias.

### Iteración 2
- colas, locks, ownership y sharding;
- tuning de WAL/flush/compaction scheduler;
- revisiones de tamaño de tipos y buffers.

### Iteración 3
- LTO/PGO y perfiles de build;
- caches, prefetch, zero-copy y mejoras de codec;
- optimización de streaming y networking.

### Iteración 4+
- cambios estructurales grandes: memtables trie, BTI, UCS avanzado, rediseños de índices, reshaping profundo de shards.

## Checklists de cierre por subsistema

### Frontend/protocolo
- ¿hay copias evitables?
- ¿prepared statements recalculan demasiado?
- ¿errores y metadatos generan asignaciones innecesarias?
- ¿la paginación materializa de más?

### Storage
- ¿commit log batchéa bien?
- ¿flush se activa demasiado pronto o demasiado tarde?
- ¿el merge de SSTables hace trabajo repetido?
- ¿hay compactions desbalanceadas o hambrientas?

### Coordinator/distribuido
- ¿la metadata de clúster se consulta con locks pesados?
- ¿hay fan-out excesivo?
- ¿hints/read repair reparan de más?
- ¿los timeouts producen cascadas?

### Streaming/repair
- ¿la presión de red se ve?
- ¿las transferencias recalculan o recodifican innecesariamente?
- ¿Merkle trees y validaciones consumen memoria descontrolada?
- ¿repair y compaction pelean por recursos sin budget?

### Seguridad/ops
- ¿TLS y logging están medidos?
- ¿audit/FQL tienen backpressure y límites?
- ¿tracing y métricas están fuera de la ruta caliente crítica?

## Resultado esperado de este plan

Si se ejecuta bien, el estado final no es “un Cassandra en Rust algo más rápido”, sino:

- un Cassandra Rust con paridad funcional y budgets de rendimiento controlados;
- una disciplina estable de profiling y regressions;
- una arquitectura más predecible que la heredada del JVM;
- y un proceso en el que cada release pueda demostrar, con datos, que no degradó correctness ni performance.