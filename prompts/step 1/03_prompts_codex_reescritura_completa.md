# Conjunto de prompts gigantescos para Codex

Estos prompts están diseñados para ejecutarse **en orden**. Cada uno asume que el anterior dejó el repositorio en un estado estable. Están redactados para forzar a Codex a producir código, tests y documentación, no respuestas vagas.

Consejo de uso: ejecuta cada prompt en una rama distinta, exige un reporte final de gaps y solo fusiona cuando el harness diferencial y la CI queden verdes para el alcance de la fase.

## Executed - Prompt 01 — Orquestación maestra, freeze de baseline y creación del workspace Rust

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Congelar la baseline de Cassandra, levantar la matriz de compatibilidad, crear el workspace Rust y dejar el repositorio listo para ejecutar la reescritura incremental sin borrar todavía el código Java.

Trabajo que debes ejecutar ahora:
1. Congela explícitamente una baseline: rama exacta, commit exacto y fecha. Si el target es `trunk`, captura además una lista de componentes trunk-only o inestables que no deben contaminar el núcleo de paridad.
2. Recorre el repositorio y clasifica el producto en dominios funcionales: CQL/native protocol, schema, storage engine, cluster metadata, gossip/snitches/ring, internode messaging, coordinator path, repair/streaming, Paxos/LWT, índices, seguridad, observabilidad, tooling y empaquetado.
3. Crea un workspace Cargo multi-crate bajo `rust/` o en la raíz con crates explícitos y responsabilidades claras. Debe existir al menos el esqueleto de crates para: common, config, types, schema, native-protocol, cql, storage, cluster-metadata, messaging, coordinator, repair, streaming, security, admin y binaries.
4. Añade ADRs iniciales que documenten: criterio de compatibilidad, política de `unsafe`, estrategia de feature flags, tratamiento de formatos on-disk, modelo de runtime/concurrencia y política de mantenimiento del Java como oráculo.
5. Prepara CI mínima: build, test, fmt, clippy, deny, benchmark skeleton y jobs diferenciales listos para conectar luego con el oráculo Java.
6. No implementes aún lógica pesada de Cassandra; prioriza estructura, contrato, gobernanza técnica y compilación limpia del esqueleto.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- `docs/rewrite/charter.md`
- `docs/rewrite/feature_matrix.md` o YAML equivalente
- `docs/rewrite/adrs/` con decisiones fundacionales
- `Cargo.toml` de workspace y crates stub compilables
- Plantilla de status report por fase

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Executed - Prompt 02 — Harness diferencial, fixtures dorados y oráculo Java ejecutable

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Construir la infraestructura que compare automáticamente Cassandra Java contra la implementación Rust y convierta a Java en especificación ejecutable.

Trabajo que debes ejecutar ahora:
1. Levanta un harness dual que pueda arrancar un clúster Cassandra Java de referencia y un clúster Rust sujeto a prueba usando Docker/CCM/pytest o la combinación más razonable para el repo.
2. Integra el conocimiento de `TESTING.md`: unit tests, integration tests y dtests. Reutiliza todo lo reutilizable; no reinventes un framework si ya existe uno aprovechable.
3. Genera fixtures dorados desde Java para: frames del protocolo nativo, resultados de queries, errores, SSTables, snapshots, replay de commit log, topologías de 1/3/N nodos, escenarios de hinted handoff, read repair, repair y bootstrap.
4. Añade comparadores de comportamiento: misma query, mismo schema, mismo payload, mismo error code, mismo orden de filas cuando corresponda y misma semántica de expiración/tombstones.
5. Integra fuzz/model testing donde aporte valor real. Si `Harry`, FQL replay o herramientas de diff son aprovechables, incorpóralas o deja ganchos explícitos y automatizables.
6. Entrega un comando único tipo `make diff-test` o `cargo xtask diff-test` que ejecute al menos un subconjunto significativo end-to-end.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Harness reproducible con README operativo
- Fixtures dorados versionados
- Comandos de ejecución local y CI
- Reporte inicial de gaps entre baseline Java y Rust stub

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 03 — Primitivas, configuración y catálogo de esquema

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Implementar los cimientos semánticos: tipos CQL internos, tokens, clocks, tombstones/TTL, carga de configuración y catálogo de esquema persistente.

Trabajo que debes ejecutar ahora:
1. Implementa tipos de dominio canónicos para partition key, clustering key, row, cell, range tombstone, timestamps, TTL, token, table id, keyspace id y metadato de schema.
2. Añade codecs binarios y comparadores necesarios para que protocolo, schema y storage compartan exactamente las mismas primitivas.
3. Carga y valida configuración a partir de `cassandra.yaml` o un subconjunto bien definido compatible, incluyendo defaults, unidades y errores de validación útiles.
4. Implementa el catálogo de esquema: keyspaces, tables, columns, types, funciones, vistas y propiedades relevantes, incluyendo persistencia en tablas de sistema o estructuras equivalentes.
5. Asegura snapshots inmutables del catálogo para lecturas concurrentes y prepared statements.
6. Acompaña todo con golden tests contra valores y estructuras generadas por Java; no aceptes equivalencia “aproximada” en serialización o ordering.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Crates `common`, `types`, `config` y `schema` funcionales
- Fixtures de serialización y ordering
- Loader de configuración con validaciones
- Persistencia/recuperación del catálogo

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 04 — Native protocol v5, parser CQL y frontend cliente-servidor

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Hacer que drivers y `cqlsh` puedan hablar con el nodo Rust usando el protocolo nativo de Cassandra y que el frontend CQL produzca planes ejecutables.

Trabajo que debes ejecutar ahora:
1. Implementa el protocolo nativo (v5 y compatibilidad requerida por la baseline) con frames, headers, compresión, handshake, auth negotiation, stream ids, eventos, paging y prepared statements.
2. Construye parser/AST/planner de CQL para DDL y DML principales, con infraestructura para ampliar cobertura sin rehacer interfaces.
3. Mapea exactamente los códigos de error, mensajes, flags y metadatos que esperan drivers reales.
4. Soporta `cqlsh` y al menos un driver moderno en tests automatizados. No te limites a un cliente casero.
5. Crea prepared statement cache con invalidación por cambios de schema y tests que prueben los edge cases de invalidación.
6. Evita introducir copias innecesarias de buffers o strings; diseña ya la capa de protocolo con zero-copy razonable y ownership claro.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Servidor del protocolo nativo funcional
- Parser y planner CQL iniciales
- Compat tests con clientes reales
- Conjunto de golden frames y errores

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 05 — Storage engine local completo: commit log, memtable, SSTable `big`, compaction y ejecución single-node

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Conseguir un Cassandra single-node funcional en Rust, con write path y read path reales, persistencia duradera y semántica observable compatible.

Trabajo que debes ejecutar ahora:
1. Implementa commit log/WAL con segmentación, checksums, rotación, replay y políticas de durabilidad configurables.
2. Implementa una memtable conservadora y estable primero (skiplist o equivalente), dejando la interfaz preparada para introducir una memtable trie después.
3. Implementa flush, backpressure y lifecycle de memtables.
4. Implementa SSTables para el formato prioritario de compatibilidad, recomendado `big` primero, con índices, summaries, Bloom filters, compresión y verificación.
5. Implementa compaction inicial (STCS como mínimo), merge de múltiples SSTables, resolución por timestamp, TTL, tombstones y expiración.
6. Cierra el loop single-node: DDL, INSERT/UPDATE/DELETE/SELECT, prepared statements, paging, batches locales, snapshots, backups incrementales y hooks básicos de CDC.
7. Todo debe venir con tests diferenciales que comparen resultados y archivos/fixtures contra Java.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Nodo single-node utilizable por clientes reales
- Formatos on-disk interoperables dentro del alcance soportado
- Benchmarks y tests de durabilidad
- Documentación de límites y TODOs

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 06 — Clúster distribuido: metadato de anillo, gossip, mensajería internodo y coordinator path

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Transformar el nodo single-node en un Cassandra distribuido real con réplicas, CLs, gossip y coordinación de lecturas/escrituras.

Trabajo que debes ejecutar ahora:
1. Implementa cluster metadata snapshots, token ring, replica placement y snitches necesarios para la baseline.
2. Implementa gossip, seeds, heartbeats y failure detection con semántica de convergencia sólida.
3. Implementa mensajería internodo con codecs, verbs, backpressure, métricas y separación clara respecto al streaming de SSTables.
4. Implementa coordinator para escrituras y lecturas: replica selection, CLs, digest reads, timeouts, fallos parciales, batch log distribuido, hinted handoff y read repair.
5. Introduce tracing y métricas suficientes para depurar routing, saturation y latencias por fase.
6. Asegura suites multi-nodo con escenarios de caída, latencia artificial, red parcial y reinicios.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Clúster Rust multi-nodo funcional
- Tests diferenciales por consistency level
- Reportes de routing y failure handling
- Métricas y trazas por subsistema

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 07 — Topología cambiante, streaming, repair, bootstrap y operaciones de movimiento de datos

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Hacer que el clúster Rust sea operable cuando cambian nodos, tokens y ownership de datos.

Trabajo que debes ejecutar ahora:
1. Implementa streaming como canal separado de la mensajería general, con control de flujo, verificación y reintentos.
2. Implementa bootstrap, decommission, replace, removenode y rebuild con su máquina de estados y efectos sobre el ring.
3. Implementa repair full e incremental, árboles de Merkle, anti-entropy y coordinación segura con compaction, snapshots y streaming.
4. Asegura coherencia entre movimientos de datos, hints, repairs en vuelo y cambios de topología.
5. Crea tests largos con failure injection: nodo caído a mitad de streaming, reinicio en mitad de repair, pérdida de red parcial, compaction concurrente con rebuild, etc.
6. No ocultes el estado interno: expón métricas y visibilidad para troubleshooting.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Operaciones de topología soportadas end-to-end
- Repair/streaming con pruebas de estrés
- Runbooks y documentación operativa
- Métricas de progreso y reintentos

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 08 — Consistencia fuerte y features avanzadas: Paxos/LWT, counters, índices, SAI, vectores, vistas y extensibilidad

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Completar las features de mayor complejidad semántica y valor funcional después de que el núcleo distribuido sea sólido.

Trabajo que debes ejecutar ahora:
1. Implementa Paxos/LWT con serial consistency, ballots, prepare/propose/commit y recuperación tras fallos parciales. Añade tests de linealizabilidad y contención.
2. Implementa counters con la semántica correcta y suites específicas de consistencia.
3. Implementa índices secundarios legacy, materialized views, triggers y la extensibilidad necesaria para UDF/UDA si forman parte del alcance target.
4. Integra SAI profundamente con storage y read path; no lo trates como un simple plugin superficial. Debe haber tests de build, query, compaction, streaming e interoperabilidad operativa.
5. Implementa el tipo vector y, si el alcance lo exige, las funciones y rutas necesarias para vector search.
6. Para cualquier feature avanzada cuya paridad completa no sea viable en una sola pasada, deja feature flag, tests marcados, documentación del gap y camino explícito de cierre.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- LWT/CAS correctos y probados
- Features avanzadas protegidas por matriz de soporte
- Benchmarks específicos para SAI/vector/counters
- Informe de gaps restantes

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 09 — Seguridad, observabilidad, plano administrativo y herramientas operativas

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Cerrar la brecha de operación real: TLS, auth, roles, auditoría, FQL, virtual tables, métricas y CLI/admin equivalentes a la experiencia operativa de Cassandra.

Trabajo que debes ejecutar ahora:
1. Implementa TLS cliente-nodo e internodo, gestión de certificados y políticas de seguridad razonables.
2. Implementa autenticación, autorización, roles, permisos, authorizers de red/CIDR y Dynamic Data Masking dentro del alcance elegido.
3. Implementa audit logging y full query logging con sinks configurables y overhead medible.
4. Expón métricas, tracing, virtual tables y un plano administrativo utilizable: CLI tipo `nodetool` o equivalente por intención, más herramientas para SSTables, snapshots y estado interno.
5. Asegura que estas features no introducen coste accidental desproporcionado en la ruta caliente; mide overhead explícitamente.
6. Entrega runbooks y documentación de operación diaria, incidentes, backup/restore y troubleshooting.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Plano de seguridad funcional
- Herramientas operativas reales
- Métricas/virtual tables/admin API o CLI
- Documentación de operación

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


## Prompt 10 — Compatibilidad mixta Java↔Rust, migración, hardening de performance y release GA

```text
Actúa como Principal Engineer y dueño técnico de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino construir una base productiva, mantenible y verificable que pueda llegar a sustituir al servidor Java con compatibilidad observable.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial y los tests existentes complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos y operativa.

2. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, operación y tooling dentro del alcance de la fase.
   - Mantén el código Java existente como referencia/oráculo hasta que la versión Rust pase los gates diferenciales correspondientes.
   - No borres Java prematuramente. Si decides dejar un stub o una incompatibilidad temporal, documenta el gap de forma explícita.

3. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable y testeable.
   - Divide el trabajo por subsistemas claros y límites de crate/módulo bien definidos.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales y golden tests donde aplique.

4. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes y tests.
   - Evita clones, asignaciones y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.

5. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una pasada, deja:
     - interfaz estable,
     - feature flag o guard-rail claro,
     - TODOs accionables,
     - tests pendientes marcados,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses si hacen falta,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos.

Objetivo específico de esta fase:
Terminar la plataforma con una ruta migrable, medible y liberable: mixed-cluster si aplica, shadow traffic, rollback, benchmarks, soak tests y empaquetado listo para producción.

Trabajo que debes ejecutar ahora:
1. Define y ejecuta la matriz de compatibilidad Java↔Rust: protocolo cliente, internodo, formatos on-disk, snapshots, repair, streaming, LWT, índices, seguridad y tooling.
2. Si mixed-cluster es parte del objetivo, impleméntalo y pruébalo; si no lo es, construye una ruta de dual-cluster y shadow traffic suficientemente segura y automatizada.
3. Integra replay de FQL, herramientas de diff de datos y comparadores de divergencia semántica durante migraciones.
4. Ejecuta el plan completo de performance: perfiles Cargo, LTO/PGO donde aporte valor, eliminación de asignaciones y locks innecesarios, optimización de I/O y budgets de latencia/throughput.
5. Añade soak tests, chaos tests, recovery tests, backup/restore drills, rollback probado y criterio formal de release GA.
6. Entrega empaquetado, imágenes, scripts de despliegue y guía de migración/rollback orientada a operadores.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables y documentación del límite.
- Añade al menos una batería de tests que pruebe el valor real de esta fase.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- Donde exista un comportamiento Java verificable, crea comparación diferencial o golden tests.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.

Entregables mínimos esperados:
- Ruta de migración y rollback validada
- Budgets de performance y dashboards
- Empaquetado release-ready
- Checklist GA cerrada

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


