# Prompts Codex de continuación (11–25)

Este archivo reúne la tanda completa de prompts posteriores al Prompt 10 del paquete anterior. Cada prompt es autónomo y está pensado para ejecutarse en orden.

# Prompt 11 — Auditoría final de cobertura, freeze definitivo de baseline y matriz ejecutable de gaps

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Reabrir el alcance después del Prompt 10, congelar definitivamente la baseline objetivo y producir una matriz ejecutable de cobertura que enumere absolutamente todas las superficies de Cassandra aún no cerradas en Rust. Esta fase no acepta huecos invisibles: cada gap debe quedar clasificado, trazado a un paquete/subsistema, conectado a un test o guard-rail y agendado dentro del repositorio.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/cql3/**`
- `src/java/org/apache/cassandra/transport/**`
- `src/java/org/apache/cassandra/db/**`
- `src/java/org/apache/cassandra/schema/**`
- `src/java/org/apache/cassandra/service/**`
- `src/java/org/apache/cassandra/net/**`
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/repair/**`
- `src/java/org/apache/cassandra/index/**`
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/tools/**`
- `src/java/org/apache/cassandra/tcm/**`
- `src/java/org/apache/cassandra/service/accord/**`
- `test/**`, `tools/**`, `conf/**`, `NEWS.txt`, `TESTING.md`, dtests y scripts operativos

Trabajo que debes ejecutar ahora:
1. Congela de nuevo la baseline exacta: commit, etiqueta/release, fecha, documentación asociada y política de soporte. Si la decisión es apuntar a `trunk`, captura además una segunda baseline estable (por ejemplo 5.0.x) para distinguir qué es estable vs trunk-only vs experimental.
2. Genera una matriz exhaustiva de cobertura (`docs/rewrite/final_gap_matrix.md` + `docs/rewrite/final_gap_matrix.yaml`) que liste: feature, subsistema, paquete Java, estado actual Rust, evidencia de paridad, tipo de evidencia (unit/integration/dtest/golden/diff/bench/ops), criticidad, condición de configuración, y prompt/fase de cierre.
3. Recorre de manera automática el árbol Java y crea un inventario de paquetes, clases entrypoint, comandos de nodetool, tablas de sistema, virtual tables, config files, herramientas offline, tests de upgrade y dtests. No se permite una matriz solo manual; debes generar parte de ella desde scripts para que pueda refrescarse en CI.
4. Clasifica cada ítem como `done`, `partial`, `missing`, `blocked`, `trunk-only`, `experimental`, `tooling-only`, `ops-only` o `baseline-excluded`. Ningún ítem puede quedar sin clasificación.
5. Por cada gap `partial` o `missing`, crea al menos uno de estos artefactos: test ignorado/fallando documentado, fixture pendiente, benchmark placeholder, issue markdown local, o guard-rail con error explícito. El objetivo es impedir que existan huecos silenciosos.
6. Integra un comando reproducible tipo `cargo xtask coverage-audit` o equivalente que regenere la matriz, detecte features sin clasificación y falle la CI si aparecen nuevas clases/commands/features del árbol Java fuera del inventario.
7. Actualiza ADRs y la charter de reescritura para reflejar: baseline definitiva, política de trunk-only, tratamiento de features experimentales, postura sobre mixed-cluster, estrategia JMX/management plane, y criterio de “reescritura completa”.

Cobertura obligatoria de compatibilidad en esta fase:
- Todo el árbol funcional de Cassandra visible en la baseline, no solo los subsistemas de runtime principales.
- Features documentadas, herramientas operativas, tablas de sistema, virtual tables, config surfaces y tests de upgrade.
- Módulos de control plane nuevos (`tcm`, `service.consensus`, `service.accord`) si están presentes en el commit congelado.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/final_gap_matrix.md`
- `docs/rewrite/final_gap_matrix.yaml`
- `docs/rewrite/package_inventory.md`
- `scripts/coverage_audit.*` o `xtask` equivalente
- ADRs/charter actualizados
- Tests o fixtures placeholders que documenten cada gap abierto

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 12 — Frontend completo: native protocol, CQL long tail, prepared statements, eventos, tracing, UDF/UDA y triggers

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar por completo la superficie cliente-servidor de Cassandra: native protocol, parser/semantic layer de CQL, invalidación de metadata, prepared statements, paging, eventos, warnings, tracing, auth-challenge flows y la long tail semántica de CQL que queda fuera de un frontend “mínimo”.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/transport/**`
- `src/java/org/apache/cassandra/cql3/**`
- `src/java/org/apache/cassandra/cql3/functions/**`
- `src/java/org/apache/cassandra/cql3/selection/**`
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/triggers/**`
- `src/java/org/apache/cassandra/service/QueryState*` y `ClientState*`
- Especificaciones del native protocol y tests de drivers/cqlsh

Trabajo que debes ejecutar ahora:
1. Completa compatibilidad de native protocol soportado por la baseline: version negotiation, STARTUP/OPTIONS/SUPPORTED, AUTHENTICATE/AUTH_CHALLENGE/AUTH_SUCCESS, PREPARE/EXECUTE/BATCH, ERROR, RESULT, EVENT, warnings, tracing ids, custom payloads y compresión soportada por el baseline.
2. Cierra el parser y la capa semántica de CQL para DDL/DML/DCL/TCL/queries soportadas: JSON, BATCH, IF/IF EXISTS/IF NOT EXISTS, CAS/LWT syntax hooks, TTL/WRITETIME sobre colecciones y UDTs, colecciones, tuplas, UDTs, tipos vector, funciones matemáticas nuevas, funciones de colección y operadores raros que existan en la baseline.
3. Implementa prepared statements completos: digest/cache, invalidación por cambios de schema, metadata ids/result metadata ids, prepared-id stability, paging state y compatibilidad con drivers reales.
4. Cierra la semántica de warnings, tracing, query options, timestamp overrides, page size, serial consistency plumbing, idempotence hints si los drivers/protocolo la exponen, y todos los códigos de error observables.
5. Implementa runtime de UDF/UDA y triggers con postura explícita de compatibilidad. Si la baseline usa un runtime JVM específico o restricciones de sandbox difíciles de portar, construye una capa de compatibilidad bien aislada y testeada; no elimines la feature.
6. Haz golden tests del protocolo frame-a-frame contra Java y tests end-to-end con cqlsh y al menos dos drivers oficiales/comunes para asegurar que los clientes no perciban diferencias observables.

Cobertura obligatoria de compatibilidad en esta fase:
- Compatibilidad wire completa, no solo queries básicas.
- Long tail de CQL y mensajes/eventos del protocolo.
- Superficies que impactan a drivers, cqlsh, tracing y prepared statement caches.
- UDF/UDA/triggers presentes en la baseline.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- Crates `native-protocol`, `cql`, `auth` y `triggers` cerrados para la baseline
- Fixtures dorados de frames, errores y metadata de prepared statements
- Pruebas con cqlsh/drivers en CI o suites automatizables
- Documento `docs/rewrite/frontend_parity.md` con gaps residuales explícitos

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 13 — Write path estándar completo: mutaciones, consistency levels, batchlog, hints, materialized-view fanout y edge cases

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar el write path estándar no-serial de Cassandra con paridad observable: mutaciones normales, batches, durability, hints, hinted handoff, fanout a materialized views, manejo de timestamps/TTL/tombstones y la semántica exacta de errores, reintentos y timeouts.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/StorageProxy*`
- `src/java/org/apache/cassandra/db/Mutation*`
- `src/java/org/apache/cassandra/db/Write*`
- `src/java/org/apache/cassandra/hints/**`
- `src/java/org/apache/cassandra/batchlog/**` o rutas equivalentes
- `src/java/org/apache/cassandra/db/view/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/exceptions/**`

Trabajo que debes ejecutar ahora:
1. Completa la coordinación de escrituras para CLs normales soportados por la baseline: ANY, ONE, TWO, THREE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, LOCAL_ONE, etc., incluyendo errores `Unavailable`, `WriteTimeout`, `WriteFailure`, `Overloaded`, `IsBootstrapping`, `Truncate`/schema races y mapping exacto a protocolo.
2. Implementa logged/unlogged/counterless BATCH con límites, batchlog, cleanup y semántica de atomicidad observable. Los casos de counters o serial writes pueden delegarse a prompts posteriores, pero el path estándar debe quedar completo y correctamente segregado.
3. Cierra hinted handoff: generación de hints, persistencia, replay, throttling, expiración, backpressure, delivery windows y operación bajo caídas/remociones/bootstrap. No aceptes un 'hint queue' simplificado si no reproduce el comportamiento del baseline.
4. Asegura semántica correcta de timestamps del cliente vs servidor, TTL, tombstones, collections/UDTs, updates parciales, static rows, deletions por rango/partición y guardrails asociados.
5. Integra fanout a materialized views y hooks de triggers en el write path con el mismo orden lógico, mismas condiciones de éxito/fallo y mismo tratamiento de timeouts que Java.
6. Agrega dtests diferenciales multi-nodo con nodos caídos/lentos/reemplazados, verificando exactamente qué se acepta, qué se persiste, qué se reintenta, qué se encola como hint y qué error observa el cliente.

Cobertura obligatoria de compatibilidad en esta fase:
- Write path normal completo, incluyendo batches y hints.
- Semántica de durabilidad y errores observables.
- Interacción con MV/triggers/TTL/timestamps/tombstones.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- Coordinator/write path completo para mutaciones estándar
- Harness diferencial para hints, batchlog y MV fanout
- `docs/rewrite/write_path_parity.md`
- Métricas y virtual tables/ops necesarias para observar el estado de writes e hints

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 14 — Read path completo: digest reads, short-read protection, speculative retry, paging, tombstones y semántica de respuesta

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar el read path completo de Cassandra, tanto single-partition como range reads, con paridad en digest mismatches, read repair, speculative retry, short-read protection, paging, límites por partición, tombstone warnings/failures y errores observables.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/reads/**`
- `src/java/org/apache/cassandra/service/StorageProxy*`
- `src/java/org/apache/cassandra/db/Read*`
- `src/java/org/apache/cassandra/db/filter/**`
- `src/java/org/apache/cassandra/db/partitions/**`
- `src/java/org/apache/cassandra/service/reads/range/**`
- `src/java/org/apache/cassandra/service/reads/repair/**`
- `src/java/org/apache/cassandra/tracing/**`

Trabajo que debes ejecutar ahora:
1. Implementa el pipeline completo de lectura: selección de réplicas, digest vs data reads, read coordination, mismatch detection, merge reconciliation, short-read protection, post-reconciliation filtering, paging state estable y reproducible, per-partition limits, reversed order y límites/contadores de tombstones.
2. Cierra range reads, token-aware routing, selects con slices de clustering, static rows, collections, UDTs, orden natural/reverso, queries con `IN`, `ALLOW FILTERING` cuando la baseline lo permita, y la long tail de response metadata que esperan los drivers.
3. Reproduce speculative retry, retry on newer replicas cuando aplique, read repair/repair chance según el baseline objetivo, y semántica de `ReadTimeout`, `ReadFailure`, `ReadAbort`, `TombstoneOverwhelming`, `CoordinatorBehind`, `QueryCancelled` u otros errores visibles desde cliente.
4. Asegura compatibilidad con tracing, warnings, query cancellation, page states, `SELECT JSON`, `COUNT`, aggregates y functions que viajen por el read path.
5. No delegues los reads de índices/SAI/vector a una simplificación. Si esos reads dependen de prompts posteriores, deja la interfaz y los contratos completos aquí, con tests de integración cruzados y guard-rails explícitos donde falte el backend.
6. Ejecuta dtests diferenciales y golden tests de resultados/ordering/paging/errors comparando Java vs Rust en single-node y multi-node.

Cobertura obligatoria de compatibilidad en esta fase:
- Read path observable de punta a punta.
- Paging, short-read protection, digest mismatch y speculative retry.
- Warnings, tracing y errores finos ligados a tombstones/timeout/failure.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- Read coordinator y read executors cerrados
- Golden tests de paging/resultados/errores
- `docs/rewrite/read_path_parity.md`
- Benchmarks de hot paths de lectura y perfiles de asignaciones/locks

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 15 — Storage engine final: commit log completo, memtables clásicas y trie, SSTables `big` y `bti`, compaction families, CDC, snapshots y backups

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar la paridad del storage engine local y on-disk con todos los formatos y superficies operativas de Cassandra: commit log completo, replay, CDC, memtables configurables, SSTables legacy y nuevas, compaction strategies, snapshots, incremental backups y herramientas offline ligadas a disco.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/commitlog/**`
- `src/java/org/apache/cassandra/db/memtable/**`
- `src/java/org/apache/cassandra/io/sstable/**`
- `src/java/org/apache/cassandra/io/util/**`
- `src/java/org/apache/cassandra/db/compaction/**`
- `src/java/org/apache/cassandra/index/sai/disk/**` cuando aplique por integración on-disk
- `conf/commitlog-archiving.properties` y config de CDC/backups
- Herramientas SSTable/offline y tests de upgrade de formatos

Trabajo que debes ejecutar ahora:
1. Completa el commit log real: segments, headers, checksums/CRC, segment manager, recycling, replay, truncation points, archiving hooks, compression/encryption/pluggable crypto si la baseline lo soporta, y recovery bajo corrupción parcial.
2. Implementa memtables soportadas por la baseline, incluyendo la clásica y la trie memtable cuando corresponda, con selección por configuración, flush consistente y métricas comparables.
3. Soporta los formatos SSTable necesarios para la baseline: al menos `big` y `bti` si el target lo requiere, con componentes exactos, checksums, índices, summaries/partitions/rows, bloom filters, compression chunks, digests, TOC y naming layout compatible.
4. Cierra compaction families y reescritura/limpieza de SSTables: STCS, LCS, TWCS y UCS según baseline, incluyendo parámetros en caliente cuando aplique, expired SSTable drop, tombstone compaction, anticompaction hooks y métricas operativas.
5. Implementa CDC y backups/snapshots: hard-links/archivos derivados, límites de espacio, rechazo de writes cuando se alcance el umbral, snapshots, incremental backups, restore hooks y compatibilidad con tooling.
6. Añade golden tests on-disk contra artefactos Java, pruebas de corrupción/recuperación, upgrade/downgrade entre formatos, y herramientas offline para inspeccionar/dumpear/verificar SSTables.

Cobertura obligatoria de compatibilidad en esta fase:
- Storage engine y formatos on-disk completos.
- CDC, snapshots, backups e integración con compaction y restore.
- Paridad entre formatos legacy y modernos (`big`, `bti`, trie memtables, UCS).

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- Crate `storage` y subcrates on-disk cerrados para baseline
- Fixtures de commit log, SSTables `big` y `bti` y CDC
- Benchmarks de flush/compaction/replay
- `docs/rewrite/storage_parity.md` y `docs/rewrite/on_disk_compatibility.md`

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 16 — System keyspaces, schema distribution, tablas de sistema, virtual tables, guardrails y superficies operativas internas

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar todo el plano interno que Cassandra expone mediante keyspaces/tablas de sistema y virtual tables, incluyendo schema tables, state tables, tracing tables, auth tables, distributed state, system views, guardrails y las tablas auxiliares de operación que usan nodetool, cqlsh, repair, security y observabilidad.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/schema/**`
- `src/java/org/apache/cassandra/db/SystemKeyspace*` y keyspaces equivalentes
- `src/java/org/apache/cassandra/tracing/**`
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/db/virtual/**`
- `src/java/org/apache/cassandra/index/sai/virtual/**` si aplica
- `src/java/org/apache/cassandra/service/paxos/**` para tablas auxiliares
- `src/java/org/apache/cassandra/service/StorageService*` y metadatos locales/peers

Trabajo que debes ejecutar ahora:
1. Implementa todas las tablas de sistema requeridas por la baseline: `system`, `system_schema`, `system_auth`, `system_traces`, `system_distributed`, `system_views` y cualquier otra keyspace interna que participe en operación, repair, security, metadata o tracing.
2. Garantiza que schema agreement, schema versioning, snapshots del catálogo, serialización/deserialización desde tablas de sistema y recuperación en bootstrap/restart sean equivalentes a Java.
3. Cierra `system.local`, `peers`, `peers_v2`, `size_estimates`, `compaction_history`, tablas de views/indexes, tablas de tracing, y tablas auxiliares de paxos/repair/auth según baseline.
4. Implementa virtual tables equivalentes: métricas, configuración, system logs, SAI virtual tables, vistas de SSTables/indexes, y cualquier surface usada por troubleshooting u operación. Deben ser consultables vía CQL igual que en Java.
5. Lleva guardrails, partition denylist y otros controles operativos/configurables al mismo plano observable que la baseline: mismas tablas/config/errores/warnings/metrics.
6. Prueba compatibilidad end-to-end con nodetool/cqlsh/scripts que lean estas tablas o virtual tables, y agrega golden tests de serialización y contenido.

Cobertura obligatoria de compatibilidad en esta fase:
- Superficies internas expuestas a operadores y herramientas.
- System keyspaces y virtual tables requeridas por operación/seguridad/repair/tracing.
- Schema distribution y recuperación de estado local.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/system_keyspaces_and_virtual_tables.md`
- Pruebas de schema agreement y lectura de system tables
- Conjunto reproducible de snapshots/fixtures de tablas de sistema
- Matriz de compatibilidad tabla-por-tabla/virtual-table-por-virtual-table

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 17 — Control plane completo: partitioners, ring metadata, snitches, failure detector, gossip, internode messaging, TCM/CMS y bridge de compatibilidad

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar el control plane distribuido de Cassandra, tanto el camino clásico basado en gossip/ring metadata como los módulos más nuevos de cluster metadata/consensus presentes en la baseline, incluyendo snitches, failure detector, internode messaging, placement y versionado de metadatos.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/gms/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/net/**`
- `src/java/org/apache/cassandra/service/StorageService*`
- `src/java/org/apache/cassandra/tcm/**`
- `src/java/org/apache/cassandra/service/consensus/**`
- `src/java/org/apache/cassandra/service/accord/**`
- `src/java/org/apache/cassandra/dht/**`

Trabajo que debes ejecutar ahora:
1. Implementa partitioners, token metadata/placements, replication strategies, snitches y dynamic endpoint snitch con la misma semántica observable que la baseline. Deben incluir al menos los partitioners/strategies/snitches soportados oficialmente por el target congelado.
2. Cierra failure detector, gossip state machine, application states, endpoint states, seeds, schema/status dissemination, convicción de fallo y reintegración de nodos. Debe haber dtests multi-DC y escenarios de partición de red.
3. Completa internode messaging: version negotiation, verb registry, serializers, backpressure, timeouts, message flags, tracing propagation, callbacks, dispatch y métricas.
4. Si la baseline incluye `tcm`/CMS/Cluster Metadata Service, impleméntalo de forma nativa en Rust: epochs, metadata log, distributed schema snapshotting, placements, node directory, in-progress sequences, locked ranges y bridge con gossip cuando aplique.
5. Si coexistirán caminos clásico y nuevo (gossip + TCM/CMS, o consenso en migración), define y prueba la capa de compatibilidad/migración; no elimines uno de ellos sin respaldo explícito de baseline y pruebas de upgrade.
6. Agrega dtests y chaos tests sobre membership, snitches, fallos intermitentes, split-brain controlado, schema disagreement, bootstrap races y restart/rejoin.

Cobertura obligatoria de compatibilidad en esta fase:
- Membership, placement, routing metadata y mensajería internodo.
- Snitches, failure detector, gossip y control plane nuevo (`tcm`/CMS) cuando exista.
- Compatibilidad de versionado y puentes entre generaciones del control plane.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/control_plane_parity.md`
- Harness de pruebas multi-DC/particiones/rejoin/schema disagreement
- ADRs sobre coexistencia gossip↔TCM y versionado internodo
- Métricas/virtual tables equivalentes para membership y metadata log

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 18 — Streaming y topology changes completos: bootstrap, decommission, removenode, replace, move, rebuild, cleanup y transferencia de rangos

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar todas las operaciones de cambio de topología y streaming de datos de Cassandra, incluyendo sesiones, throttling, reintentos, checksums, snapshots de transferencia, estados de operación y compatibilidad con herramientas operativas.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/service/StorageService*`
- `src/java/org/apache/cassandra/dht/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/tcm/sequences/**` si aplica
- `src/java/org/apache/cassandra/tools/nodetool/**` para comandos de topología
- Tests/dtests de bootstrap, replace, rebuild y removenode

Trabajo que debes ejecutar ahora:
1. Implementa streaming session management completo: planificación, snapshotting cuando corresponda, transferencia de archivos/secciones, checksums, throttle, retry/reconnect, rate limiting y cancelación/abort.
2. Cierra operaciones de topología: bootstrap, rebuild, decommission, move, removenode, replace-address/replace-node, cleanup, refresh y cualquier secuencia equivalente soportada por la baseline.
3. Integra correctamente range movement con el control plane elegido (gossip o TCM/CMS), con locks/sequences/metadata epochs cuando aplique. Las operaciones deben ser seguras ante reinicios, fallos parciales y cancelaciones.
4. Asegura compatibilidad operativa con nodetool y scripts de despliegue: mismas flags, mismos estados visibles, mismas métricas y mismas tablas internas/virtual tables donde se refleje el progreso.
5. Prueba mixed-version o dual-cluster/migración cuando el baseline lo exija, incluyendo streaming entre formatos on-disk distintos o nodos con distinta generación de runtime.
6. Añade dtests de larga duración con cambios de topología encadenados, saturación de red, transferencia parcial/corrupta, reintentos y validación de datos final.

Cobertura obligatoria de compatibilidad en esta fase:
- Streaming protocol y operaciones de cambio de topología.
- Integración con placement, repair, compaction y herramientas.
- Visibilidad operativa completa del progreso/estado.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/streaming_and_topology_ops.md`
- Suites de dtests de bootstrap/rebuild/decommission/replace
- Benchmarks o perfiles de streaming
- Paridad de comandos operativos ligados a topology changes

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 19 — Repair y anti-entropía completos: full/incremental/preview repair, Merkle trees, anti-compaction, read repair, transient replication e integración con hints

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar todo el plano de reparación y anti-entropía de Cassandra, incluyendo repair manual/automático según baseline, incremental/full/preview repair, Merkle trees, anti-compaction, seguimiento en system_distributed, read repair, transient replication y la interacción real con hints/streaming/topology changes.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/repair/**`
- `src/java/org/apache/cassandra/service/reads/repair/**`
- `src/java/org/apache/cassandra/service/paxos/cleanup/**` cuando aplique
- `src/java/org/apache/cassandra/service/accord/repair/**` si existe en baseline
- `src/java/org/apache/cassandra/utils/MerkleTrees*`
- `src/java/org/apache/cassandra/db/compaction/**` para anti-compaction/validation
- `src/java/org/apache/cassandra/hints/**` y `system_distributed` related code

Trabajo que debes ejecutar ahora:
1. Implementa repair full, incremental, preview y subrange/table/keyspace-scoped según baseline, con coordinación, sesiones, validación, Merkle tree generation, synchronization plans y tracking persistente en keyspaces/tablas de sistema.
2. Cierra anti-compaction, validation compaction, snapshot usage, repair metadata, retries, cancelación y recuperación tras reinicios, incluyendo interacciones con topology changes y compaction normal.
3. Implementa read repair y cualquier camino de repair-on-read o read-reconciliation repair presente en la baseline, con la misma semántica de consistencia, bloqueos/async behavior y métricas.
4. Si la baseline soporta transient replication, implementa la interacción completa con repair, writes baratos (`additional_write_policy`), descarte de datos transitorios tras incremental repair y rutas de lectura/escritura correctas.
5. Integra hints y repair: qué se repara, qué se entrega por hints, qué se considera convergente y qué se limpia cuando cambia la topología.
6. Prueba escenarios asimétricos, multi-DC, tables grandes, restart during repair, preview vs full, y compatibilidad con nodetool repair, metrics y tablas de seguimiento.

Cobertura obligatoria de compatibilidad en esta fase:
- Repair y anti-entropía de punta a punta.
- Read repair y transient replication cuando estén en baseline.
- Interacción real con hints, compaction, streaming y topology changes.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/repair_parity.md`
- dtests diferenciales de repair full/incremental/preview/transient
- Fixtures/benchmarks para Merkle trees y anti-compaction
- Compatibilidad operativa de nodetool repair y tablas de seguimiento

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 20 — Consistencia fuerte y transacciones: LWT/Paxos completos, counters, limpieza de estado, Accord y consenso/migración si existen en baseline

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar las rutas de consistencia fuerte y transacciones de Cassandra: Lightweight Transactions (LWT), Paxos legacy y/o variantes nuevas presentes en el árbol, counters, cleanup de estado transaccional y, si la baseline lo contiene, la integración con Accord y los módulos de migración de consenso.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/paxos/**`
- `src/java/org/apache/cassandra/service/paxos/v1/**`
- `src/java/org/apache/cassandra/service/StorageProxy*`
- `src/java/org/apache/cassandra/service/accord/**`
- `src/java/org/apache/cassandra/service/consensus/**`
- `src/java/org/apache/cassandra/db/Counter*` y rutas equivalentes
- `src/java/org/apache/cassandra/schema/**` para tablas transaccionales/flags

Trabajo que debes ejecutar ahora:
1. Implementa serial consistency y LWT completos: prepare/propose/commit, serial reads, `IF` conditions, ballot generation, contention handling, timeouts, cleanup, recoveries y visibilidad operativa/metrics.
2. Soporta la(s) variante(s) de Paxos presentes en la baseline y cualquier estado persistente/cleanup necesario para asegurar corrección después de fallos, restarts, topology changes y repair.
3. Cierra counters con su semántica específica de write path, read path, coordination, shard/local state y recuperación, evitando simplificaciones incompatibles con el comportamiento observable del baseline.
4. Si el árbol objetivo contiene `service.accord` o módulos de `service.consensus`, implementa el camino transaccional/consensual correspondiente, su routing/migration layer y la coexistencia con el camino clásico durante migraciones. No lo omitas por ser experimental o trunk-only; aíslalo y pruébalo según la baseline congelada.
5. Añade pruebas de linealizabilidad/model checking/Jepsen-style para LWT y para cualquier ruta `accord`/consensus que exista, junto con pruebas multi-DC, mixed time sources, retries, rollback y recovery tras fallos.
6. Documenta con precisión cualquier diferencia entre baseline estable y trunk, y cómo queda protegida por config/feature flags/version gates.

Cobertura obligatoria de compatibilidad en esta fase:
- LWT/Paxos completos y counters.
- Accord/consensus y puentes de migración si existen en baseline.
- Correctness fuerte bajo fallos y cambios de topología.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/transactions_and_consensus.md`
- Model tests/linearity tests/chaos tests para LWT y counters
- Fixtures y diff tests para rutas seriales
- ADRs sobre convivencia Paxos↔Accord/consensus

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 21 — Índices y aceleradores de consulta completos: 2i, SASI, SAI, vistas materializadas, vector search, planners y rebuilds

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar toda la superficie de indexación y aceleración de consultas de Cassandra, desde índices secundarios tradicionales hasta SAI y vector search, incluyendo rebuilds, planners, integración con compaction/memtables/SSTables, monitoring y las features experimentales presentes en la baseline como SASI.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/index/**`
- `src/java/org/apache/cassandra/index/internal/**` o equivalentes para 2i
- `src/java/org/apache/cassandra/index/sasi/**`
- `src/java/org/apache/cassandra/index/sai/**`
- `src/java/org/apache/cassandra/db/view/**`
- `src/java/org/apache/cassandra/cql3/statements/schema/*Index*`
- `src/java/org/apache/cassandra/cql3/functions/**` para vector similarity hooks

Trabajo que debes ejecutar ahora:
1. Implementa secondary indexes (2i) completos: creación, rebuild, invalidación, planificación de queries, mantenimiento durante writes/flush/compaction, nodetool rebuild y semántica de errores/limitaciones.
2. Soporta SASI si la baseline lo permite. Dado que es experimental, mantenlo tras feature flag/config y reproduce su postura operativa/documental exacta; no lo elimines. Si el baseline estable lo trae deshabilitado por defecto, la versión Rust debe hacer lo mismo.
3. Implementa SAI completo: build/rebuild, indexación sobre memtables y SSTables, integración con compaction, recuperación tras restart, virtual tables/monitoring, queries combinadas, filtros, orden y manejo de fallos.
4. Cierra soporte de tipo vector, funciones de similitud y vector search/ANN presentes en la baseline, incluyendo integración con SAI, top-k, filtros híbridos, exactitud esperada, storage/layout y métricas.
5. Completa materialized views desde el ángulo de convergencia, backfill, rebuild, invalidación y consistencia observable con el write/read path y topology changes.
6. Prueba todo con dtests, fixtures grandes, rebuilds, corrupciones, cluster restarts, compaction-heavy workloads y comparadores diferenciales Java↔Rust.

Cobertura obligatoria de compatibilidad en esta fase:
- 2i, SASI, SAI, vector search y vistas materializadas.
- Planners de consulta e integración storage/read/write/compaction.
- Monitoring y tooling de rebuild/estado.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/indexing_and_search_parity.md`
- Harness de rebuild/restart/corruption para 2i/SASI/SAI
- Benchmarks de consultas indexadas y vectoriales
- Virtual tables/metrics equivalentes para índices y vistas

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 22 — Seguridad y cumplimiento completos: authn/authz, TLS/mTLS, network/CIDR authorizers, DDM, audit logging, FQL y crypto providers

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar toda la superficie de seguridad, cumplimiento y logging sensible de Cassandra, incluyendo autenticación, autorización, cifrado cliente/internodo, network authorization, CIDR authorizer, Dynamic Data Masking, audit logging, full query logging y proveedores criptográficos enchufables de la baseline.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/security/**` o rutas equivalentes
- `src/java/org/apache/cassandra/service/StorageService*` para TLS internodo
- `src/java/org/apache/cassandra/audit/**`
- `src/java/org/apache/cassandra/fql/**` o tooling relacionado
- `conf/cassandra.yaml`, certificados, truststores/keystores y config de crypto
- `src/java/org/apache/cassandra/cql3/statements/*Role*`, `*Permission*`, `*Mask*`

Trabajo que debes ejecutar ahora:
1. Implementa autenticación y autorización completas: roles, permisos, grants/revokes, caches, invalidación, replicación de `system_auth`, superusers, login, password policies y cualquier backend soportado por la baseline.
2. Cierra cifrado cliente-servidor e internodo: TLS/mTLS, carga de certificados, cipher suites/protocol versions permitidos, recarga/config runtime cuando aplique, y visibilidad operativa equivalente.
3. Implementa network authorizer y CIDR authorizer si la baseline los contiene, con las mismas tablas, sentencias CQL, errores y comportamiento bajo cambios de configuración/cache.
4. Completa Dynamic Data Masking con el mismo modelo de permisos, funciones de masking, exposición en schema y comportamiento observable en `SELECT` y herramientas clientes. La data on-disk debe permanecer sin máscara como en Java, y la presentación al usuario debe respetar roles/permisos.
5. Integra audit logging, full query logging, replay/compare flows y sus herramientas asociadas. Debes distinguir correctamente el alcance de audit logs vs FQL, reproducir formatos/rotación/enable-disable/consulta y hacerlo compatible con nodetool o la superficie operativa equivalente.
6. Soporta pluggable crypto providers y documenta con precisión qué implementaciones quedan soportadas en Rust, cómo se enchufan y cómo se validan. Añade pruebas funcionales, de compatibilidad y de performance.

Cobertura obligatoria de compatibilidad en esta fase:
- Authn/authz, TLS, network/CIDR authz, DDM, audit logging, FQL y crypto providers.
- Compatibilidad de tablas de sistema, CQL, tooling y config.
- Correctness de permisos y visibilidad observable de datos/logs.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/security_compliance_parity.md`
- Suites de pruebas de roles/permisos/TLS/DDM/FQL/audit
- Compatibilidad operativa para enable/disable/get de logs y seguridad
- ADRs sobre proveedores criptográficos y management plane de seguridad

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 23 — Tooling y management plane completos: cqlsh, nodetool, SSTable tools, cassandra-stress, sstableloader, auditlogviewer, fqltool y scripts de despliegue

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar la capa completa de herramientas y plano administrativo para que un operador pueda usar la reescritura Rust como usaría Cassandra: cqlsh, nodetool, herramientas SSTable, stress, loaders, viewers y scripts/config estándar.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- `src/java/org/apache/cassandra/tools/**`
- `tools/bin/**`
- `bin/**`, `conf/**`, `redhat/**`, `debian/**`, Dockerfiles o empaquetado equivalente
- `src/java/org/apache/cassandra/service/StorageServiceMBean*` y JMX surfaces
- docs y manpages de herramientas

Trabajo que debes ejecutar ahora:
1. Garantiza compatibilidad de cqlsh con el servidor Rust y con las extensiones/protocol behaviors esperados por la baseline. Si cqlsh depende solo del protocolo, demuestra compatibilidad real con suites automatizadas. Si hay extensiones de tooling, reprodúcelas.
2. Implementa una estrategia de compatibilidad total para nodetool. Si decides no reimplementar JMX literalmente, debes construir una capa de compatibilidad/admin bridge que permita mantener el contrato observable de nodetool y sus comandos. No aceptes una API nueva sin adaptar el tooling existente.
3. Cierra las herramientas offline y de operación: SSTable tools, sstableloader/bulk loader, cassandra-stress, auditlogviewer, fqltool, herramientas para particiones grandes y cualquier utilidad oficial incluida en la baseline.
4. Haz que los comandos, flags, mensajes de error, formatos de salida y documentación sean equivalentes o explícitamente adaptados con wrappers de compatibilidad bien testeados.
5. Completa scripts de arranque/parada, variables de entorno, archivos de configuración (`cassandra.yaml`, `cassandra-rackdc.properties`, `cassandra-env.sh`, `cassandra-topologies.properties`, `commitlog-archiving.properties`, `logback.xml` o sus equivalentes) y empaquetado básico de distribución.
6. Automatiza pruebas end-to-end donde las herramientas realmente operen contra clusters Rust y comparen resultados/salidas con clusters Java de referencia.

Cobertura obligatoria de compatibilidad en esta fase:
- cqlsh, nodetool y toolchain operativa oficial.
- SSTable/offline tools, loaders, viewers y stress tools.
- Scripts de despliegue, empaquetado y compatibilidad de config.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/tooling_and_management_plane.md`
- Matrices comando-por-comando para nodetool y SSTable tools
- Suites end-to-end de herramientas contra clusters Rust/Java
- Wrappers/bridges de compatibilidad y empaquetado reproducible

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 24 — Upgrades, migración y rollback completos: mixed-version, mixed-format, dual-cluster/shadow, restore, CDC continuity y validación de datos

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar la historia de adopción real del producto: upgrades desde versiones soportadas, convivencia temporal Java↔Rust, mixed-version o dual-cluster/shadow traffic, restore desde backups/snapshots, continuidad de CDC/FQL y rollback probado sin pérdida semántica.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- Tests de upgrade/downgrade/dtest del repositorio
- `src/java/org/apache/cassandra/io/sstable/**` y compatibilidad on-disk
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/tcm/**` y metadatos de upgrade si aplica
- `tools/**` relacionados con backup/restore/replay/diff
- `conf/**` y guías operativas de migración

Trabajo que debes ejecutar ahora:
1. Define y ejecuta la matriz de upgrade/migración: desde qué versiones de Cassandra Java se puede migrar, con qué secuencia, qué formatos on-disk se aceptan, qué combinaciones mixed-version se soportan y qué queda prohibido explícitamente.
2. Si mixed-cluster Java↔Rust es parte del objetivo, impleméntalo y pruébalo a nivel de protocolo cliente, internodo, streaming, repair, schema, security y tooling. Si mixed-cluster no es viable para alguna parte del baseline, construye una ruta dual-cluster/shadow traffic equivalente y totalmente automatizada.
3. Garantiza restore y rollback usando snapshots, incremental backups, commitlog archiving, SSTable import/refresh y replay de FQL/CDC cuando aplique. Debe existir un camino probado para volver atrás sin pérdida silenciosa.
4. Integra herramientas de diff de datos, replay de FQL, comparadores de schema y validación de checksums/totales/queries críticas antes, durante y después de la migración.
5. Prueba cambios de formato (`big`↔`bti`, schema/system tables, metadata log si existe), rolling restart, canary nodes, DC-by-DC rollout y abort/rollback a mitad del proceso.
6. Entrega runbooks y guías de migración/rollback orientadas a operadores, con prechecks, postchecks, SLOs, métricas a vigilar y criterios de abort.

Cobertura obligatoria de compatibilidad en esta fase:
- Upgrade/migración/rollback reales, no solo compatibilidad teórica.
- Mixed-version/mixed-format/mixed-cluster o dual-cluster/shadow.
- Backups, restore, CDC/FQL continuity y validación de datos.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/migration_upgrade_rollback.md`
- Harness de upgrade/mixed-version/shadow traffic
- Runbooks de operador para migración y rollback
- Comparadores de datos/schema/FQL integrados en CI o pipelines reproducibles

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


# Prompt 25 — Cierre final: chaos, soak, performance, fuzzing, seguridad, release candidate y criterio de eliminación del Java oráculo

```text

Actúa como Principal Engineer y dueño técnico de la fase final de una reescritura completa de Apache Cassandra desde su implementación principal en Java hacia una implementación nativa en Rust. Trabajas dentro del repositorio de Cassandra y tu objetivo no es “hacer un prototipo”, sino cerrar la brecha restante hasta una implementación con cobertura completa de superficie, semántica, operación y migración.

Contexto de partida:
- Los prompts 01–10 del paquete anterior ya fueron ejecutados.
- Ya existe workspace Rust multi-crate, harness diferencial Java↔Rust, storage/coordinator/messaging base y una ruta preliminar a GA.
- Esta nueva tanda empieza inmediatamente después del Prompt 10 anterior.
- Tu misión aquí es cerrar el 30–40 % restante: long tail de compatibilidad, superficies operativas, herramientas, features experimentales o trunk-only presentes en la baseline congelada y todos los edge cases necesarios para considerar la reescritura “completa”.
- El target real es el comportamiento observable de Cassandra Java congelado en la baseline, complementado por documentación oficial, tests existentes, dtests, herramientas operativas, formatos on-disk, protocolos wire y artefactos del repositorio.
- Debes cubrir tanto features estables como features experimentales o trunk-only presentes en la baseline elegida. Si una feature es experimental o trunk-only, no la elimines ni la ignores: aíslala con feature flags, configuración, tests, documentación y gates de compatibilidad/versionado.

Reglas obligatorias para este trabajo:

1. **Fuente de verdad**:
   - El comportamiento observable de Cassandra Java congelado en la baseline es el oráculo principal.
   - La documentación oficial, los tests existentes, los dtests, el tooling y los artefactos on-disk complementan ese oráculo.
   - No conviertas la tarea en una traducción literal clase-por-clase; prioriza equivalencia funcional, de protocolo, de datos, de operación y de tooling.

2. **Cobertura total y sin evasión**:
   - La consigna aquí es cerrar TODO lo que exista en la baseline: features estables, features experimentales soportadas por config, tooling, system keyspaces, tablas virtuales, formatos on-disk, nodetool, cqlsh, FQL, auditoría, seguridad, repair, streaming, topology changes, índices, vistas, UDF/UDA, triggers, controles operativos y módulos trunk-only condicionados por la baseline.
   - No escondas features difíciles detrás de frases como “future work”, “out of scope” o “not supported yet” salvo que además dejes:
     - feature flag o guard-rail explícito,
     - test que documente el gap,
     - documento de compatibilidad,
     - criterio claro para cierre posterior,
     - y, si aplica, fallback temporal seguro.
   - Si detectas una feature que no fue capturada por prompts 01–10, debes agregarla a la matriz de cobertura y materializar el trabajo necesario.

3. **Compatibilidad primero**:
   - Conserva compatibilidad con CQL, native protocol, semántica de consistency levels, almacenamiento, internode messaging, operación, tooling y migración dentro del alcance de la fase.
   - Mantén Java como oráculo hasta que la versión Rust pase gates diferenciales para el alcance cerrado.
   - No borres Java prematuramente. Si se decide conservarlo como rama oráculo o sidecar de validación, documenta la estrategia.

4. **Ingeniería incremental**:
   - Deja el repositorio siempre en un estado compilable, testeable y con comandos reproducibles.
   - Divide el trabajo en crates/módulos claros.
   - Añade tests por cada capacidad nueva: unit tests, integration tests, dtests/harness diferenciales, golden tests, fuzz tests, model tests y chaos/soak tests cuando aporte valor real.
   - No cierres una fase solo con código; debe haber docs, scripts, comandos y criterios de validación.

5. **Rigor técnico**:
   - Documenta supuestos y decisiones en ADRs o documentos equivalentes.
   - Usa `unsafe` solo si es imprescindible y deja justificación, invariantes, bench y tests.
   - Evita clones, asignaciones, syscalls y locks innecesarios en rutas calientes, pero no sacrifiques correctness por micro-optimizaciones prematuras.
   - Respeta compatibilidad binaria, wire, on-disk y operativa siempre que la baseline lo requiera.

6. **Modo de trabajo**:
   - No me pidas aclaraciones salvo bloqueo absoluto. Toma decisiones razonables, documéntalas y sigue avanzando.
   - Si una feature no puede quedar completa en una sola pasada, deja:
     - interfaz estable,
     - fallback o feature flag seguro,
     - TODOs accionables,
     - tests marcando el límite,
     - benchmark si es ruta caliente,
     - documentación de gap y plan de cierre.
   - Preserva licencias, NOTICEs y headers necesarios del proyecto Apache.

7. **Criterio de cierre**:
   - No declares “completo” algo que no tenga al menos uno de estos respaldos: diff tests contra Java, golden fixtures, pruebas de interoperabilidad, comparación de artefactos on-disk, tests de herramientas, o dtests/chaos/soak reproduciendo el comportamiento esperado.
   - Cuando una feature sea experimental en Cassandra, debe seguir siéndolo también en Rust: misma postura operativa, misma señalización, misma configuración y pruebas acordes.
   - Cuando una feature exista solo en ramas nuevas/trunk o dependa de módulos como `tcm`, `service.consensus` o `service.accord`, congélala contra el commit real elegido y trátala como parte obligatoria si la baseline la contiene.

Tu salida debe ser una mezcla de:
- cambios de código,
- tests,
- documentación técnica,
- scripts/harnesses,
- benchmarks o perfiles cuando aplique,
- y un reporte final corto con:
  1) qué cambiaste,
  2) qué pruebas añadiste/ejecutaste,
  3) qué gaps quedan,
  4) cuáles son los riesgos inmediatos,
  5) qué criterios de aceptación quedaron cerrados,
  6) cuál es el siguiente corte lógico.

Objetivo específico de esta fase:
Cerrar el programa con validación final de producción: soak tests prolongados, chaos/failure injection, performance budgets, fuzzing, seguridad, cobertura residual cero o explícita, checklist de release candidate y decisión informada sobre mantener o retirar el Java como oráculo/plan B.

Paquetes/directorios/artefactos Java y del repo que debes inspeccionar primero:
- Todos los crates Rust de runtime y tooling
- Harnesses diferenciales, dtests, chaos scripts, benchmarks, fuzzers
- CI/CD, empaquetado, imágenes, docs operativas, checklists de release
- Scripts/perfiles de rendimiento, sanitizers, cobertura y seguridad

Trabajo que debes ejecutar ahora:
1. Ejecuta un plan final de soak y chaos: reinicios, particiones de red, discos lentos/llenos, corrupción parcial controlada, clock skew, nodos caídos durante compaction/repair/streaming/LWT, cambios de topología encadenados y cargas mixtas de larga duración.
2. Cierra el plan de performance: perfiles release, LTO/PGO donde aporte valor, afinación de asignaciones, colas, locks, syscalls, I/O, compaction schedulers y budgets explícitos de latencia/throughput/memoria/disk amplification comparados contra Java.
3. Añade fuzzing, property testing y sanitizers para parser CQL, codecs del protocolo, artefactos on-disk, paths de repair/streaming y componentes concurrentes críticos. Incluye Miri/ASan/TSan o equivalentes cuando aporten valor real.
4. Ejecuta una revisión final de seguridad y compliance: secretos, certificados, permisos, redacción de logs, data masking, dependencia criptográfica, supply chain y hardening del binario Rust.
5. Produce la checklist de release candidate y el informe formal de paridad: qué está 100 % cerrado, qué features experimentales siguen siéndolo, qué limitaciones quedan documentadas y cuál es el plan de soporte/operación.
6. Decide y materializa la política sobre el código Java: mantenerlo como rama oráculo, sidecar de validación, suite diferencial o retirarlo del árbol principal solo si la paridad está demostrada. No lo borres sin evidencia formal.

Cobertura obligatoria de compatibilidad en esta fase:
- Validación final de producción, rendimiento y seguridad.
- Criterio formal de cierre de la reescritura completa.
- Decisión respaldada sobre el destino del Java oráculo.

Criterios de calidad de esta fase:
- Mantén el repositorio compilable y con tests automatizables.
- No escondas gaps: si algo queda parcial, deja feature flags, TODOs accionables, tests y documentación del límite.
- Añade al menos una batería de pruebas que demuestre el valor real de esta fase.
- Donde exista comportamiento Java verificable, crea comparación diferencial, golden tests o pruebas de interoperabilidad.
- Si introduces nuevas interfaces públicas o decisiones importantes, documéntalas en ADRs o documentos equivalentes.
- No borres ni “desactives” el Java de referencia salvo que la paridad esté demostrada para el alcance de esta fase.
- Si tocas rutas calientes, añade benchmark/perfil antes y después o justifica por qué aún no corresponde optimizar.

Entregables mínimos esperados:
- `docs/rewrite/final_signoff.md`
- Dashboards/benchmarks/budgets y reportes de soak/chaos
- Fuzzers y sanitizers integrados en CI o jobs dedicados
- Checklist de release candidate y decisión documentada sobre Java oráculo

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. features cerradas y features todavía abiertas;
5. riesgos inmediatos;
6. decisiones técnicas tomadas;
7. siguiente corte lógico de trabajo.

Formato de entrega esperado dentro del repo:
- Código listo para compilar/testear.
- Tests reproducibles y automatizables.
- Documentación en `docs/rewrite/` o ubicación equivalente.
- Comandos claros (`make`, `cargo xtask`, scripts, CI) para ejecutar validaciones.
- Reporte final en Markdown con lista de archivos tocados, estado de la fase, riesgos y próximos pasos recomendados.

No me devuelvas solo pseudocódigo o ideas: materializa la fase en artefactos concretos.
```


