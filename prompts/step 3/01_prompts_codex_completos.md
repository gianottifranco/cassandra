# Pack completo de prompts Codex para cerrar el gap analysis

Este archivo reúne todos los prompts en un solo lugar. Se recomienda ejecutarlos en orden.

# Prompt 00 — Orquestación maestra, freeze de baseline y backlog ejecutable a partir del gap analysis

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Convertir `INPUT_full_gap_analysis.md` en un plan ejecutable dentro del repo: congelar baseline exacta, validar que los gaps listados siguen existiendo, descomponerlos por crate/PR y dejar un tablero técnico que impida perder coverage durante el resto de la serie.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `INPUT_full_gap_analysis.md`
- `src/java/org/apache/cassandra/**`
- `rust/crates/**`
- `test/**`, dtests, diff tests, gap guards
- CI actual, scripts de build, docs de arquitectura y ADRs existentes

Trabajo que debes ejecutar ahora:
1. Leer completo el gap analysis incluido en el paquete y convertir cada gap P0/P1/P2, cada TODO/stub y cada gap guard ignorado en una matriz ejecutable dentro del repo.
2. Congelar la baseline exacta de Java (commit/tag/branch) y la baseline exacta de Rust que se usará como punto de partida para esta serie.
3. Crear una matriz `gap_id -> paquete Java -> crate Rust -> prompt dueño -> test esperado -> severidad -> estado` y hacerla parte del repositorio.
4. Revalidar automáticamente los top gaps del análisis para detectar si alguno ya fue parcialmente cerrado y ajustar el backlog sin perder trazabilidad.
5. Clasificar gaps en: bloqueo de arranque, bloqueo de beta, bloqueo de GA, deuda técnica no bloqueante y divergencia aceptada temporalmente.
6. Crear dashboards o archivos de seguimiento para: TODOs, stubs, ignored tests, gaps de wire compatibility, gaps on-disk, gaps de tooling y gaps de seguridad.
7. Añadir checks de CI que fallen si reaparecen nuevos stubs/TODOs en subsistemas críticos sin una etiqueta de seguimiento aprobada.
8. Preparar documentos de trabajo para el resto de la serie: orden de ejecución, criterios de aceptación por prompt y política de no degradación.

Criterios de aceptación de esta fase:
- Existe `docs/rewrite/gap_backlog.md` o equivalente con todos los gaps trazados.
- Existe una matriz de ownership y estado por prompt.
- Hay checks de CI o scripts que vigilan TODOs/stubs/gap guards.
- El repo queda listo para ejecutar el Prompt 01 sin ambigüedad de baseline.

Entregables mínimos:
- Matriz completa de gaps
- Freeze de baseline documentado
- Script de auditoría de TODO/stub/ignored-tests
- Docs de ejecución y criterios de cierre

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 01 — Servidor CQL real: TCP listener, daemon bootstrap, pipeline, TLS y dispatcher base

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el P0 más grave del gap analysis: no existe un servidor TCP real para el protocolo nativo. Debes montar un servicio ejecutable que escuche en el puerto configurado, acepte conexiones, negocie protocolo, aplique límites y encamine requests hacia la capa de ejecución.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/transport/Server.java`
- `src/java/org/apache/cassandra/transport/Dispatcher.java`
- `src/java/org/apache/cassandra/transport/CQLMessageHandler.java`
- `src/java/org/apache/cassandra/service/NativeTransportService.java`
- `src/java/org/apache/cassandra/service/CassandraDaemon.java`
- Crates Rust equivalentes del protocolo nativo, server, auth y config

Trabajo que debes ejecutar ahora:
1. Implementar un listener TCP real para native protocol con Tokio o runtime equivalente, incluyendo accept loop, shutdown limpio y metrics básicas por conexión.
2. Construir la pipeline de conexión: detección/negociación de versión, frame decode/encode, compresión, TLS opcional, autenticación inicial y handoff al dispatcher.
3. Implementar `Dispatcher` o equivalente que traduzca mensajes del protocolo a invocaciones de la capa de ejecución, con separación clara entre framing, auth y CQL execution.
4. Añadir límites de conexiones simultáneas, límites de memoria por cliente, backpressure de colas y métricas de saturación.
5. Soportar handshake/auth/startup/options/register/eventos y cerrar correctamente conexiones inválidas o con protocolo corrupto.
6. Integrar `ClientState`/`QueryState` placeholders mínimos si aún no existen, de forma que Prompt 02 pueda colgarse sin reescribir el server.
7. Crear un binario daemon o `main` equivalente que arranque el servicio, lea config, inicialice subsistemas básicos y exponga shutdown ordenado.
8. Añadir tests de integración usando drivers reales o clientes mínimos que verifiquen connect/startup/options/register/auth y errores de protocolo.

Criterios de aceptación de esta fase:
- Un cliente puede abrir conexión TCP, negociar protocolo y recibir respuestas reales del servidor.
- Hay tests de integración para startup, auth y eventos básicos.
- Existen límites de conexión/backpressure configurables.
- El daemon arranca y apaga limpiamente.

Entregables mínimos:
- Native transport server funcional
- Dispatcher base
- Daemon/bootstrap
- Tests de integración CQL over TCP

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 02 — QueryProcessor completo, QueryState/ClientState, QueryOptions, ResultSet y ejecución central

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el hueco entre el servidor de protocolo y la ejecución real de queries. Debes implementar el equivalente a `QueryProcessor` y su contexto operativo completo para que las queries pasen de bytes a ejecución coordinada con resultados paginables y errores correctos.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/cql3/QueryProcessor.java`
- `src/java/org/apache/cassandra/cql3/QueryHandler.java`
- `src/java/org/apache/cassandra/service/ClientState.java`
- `src/java/org/apache/cassandra/service/QueryState.java`
- `src/java/org/apache/cassandra/cql3/QueryOptions.java`
- `src/java/org/apache/cassandra/cql3/ResultSet.java`, `UntypedResultSet.java`, `UpdateParameters.java`

Trabajo que debes ejecutar ahora:
1. Implementar `QueryProcessor` o equivalente como punto único de parse, prepare, validate, authorize, execute y format de resultados.
2. Definir e implementar `ClientState`, `QueryState` y `QueryOptions`, incluyendo paging, consistency, serial consistency, timestamp del cliente, TTL y custom payload si aplica.
3. Implementar `ResultSet`/`UntypedResultSet` con paginación, metadata, warnings, tracing id y estados compatibles con el protocolo nativo.
4. Conectar prepared statements con invalidación por cambios de schema, IDs determinísticos y caches con métricas.
5. Mapear correctamente exceptions internas a errores del protocolo, respetando categorías y metadata esperadas.
6. Crear un adaptador estable entre QueryProcessor y la capa coordinadora/read-write path, sin acoplar el protocolo directamente a storage.
7. Añadir queries internas contra system keyspaces usando `UntypedResultSet` o equivalente para que futuras fases de schema/auth/tracing puedan reutilizarlo.
8. Escribir tests end-to-end desde frame nativo hasta resultado CQL y tests diferenciales con Java en consultas básicas y prepared statements.

Criterios de aceptación de esta fase:
- Existe un flujo completo parse/prepare/execute/result desde el servidor hasta la respuesta.
- Prepared statements funcionan con cache y invalidación mínima.
- Paging y metadata de resultados están soportados.
- Errores del protocolo se corresponden con el fallo real.

Entregables mínimos:
- QueryProcessor funcional
- ClientState/QueryState/QueryOptions
- ResultSet/UntypedResultSet
- Tests end-to-end y diferenciales

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 03 — CQL semántico: restrictions, selection, conditions, built-in functions y statements faltantes

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la semántica CQL que el gap analysis marca como prácticamente ausente: restricciones WHERE, evaluación SELECT, LWT IF conditions, built-in functions, overload resolution y los statements faltantes o incompletos dentro de la baseline.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/cql3/restrictions/**`
- `src/java/org/apache/cassandra/cql3/selection/**`
- `src/java/org/apache/cassandra/cql3/conditions/**`
- `src/java/org/apache/cassandra/cql3/functions/**`
- `src/java/org/apache/cassandra/cql3/statements/**`
- AST/parser Rust actual y planner CQL actual

Trabajo que debes ejecutar ahora:
1. Implementar validación real de `StatementRestrictions`, restricciones de partition key, clustering, índices, LIKE/IN/range y su interacción con metadata.
2. Implementar la infraestructura de `Selection`/`Selector`/`ResultSetBuilder`, incluyendo selectors de columnas, collections, writetime/TTL, aliases y agregaciones básicas.
3. Implementar `ColumnCondition` y evaluación real de condiciones `IF`, `IF EXISTS`, `IF NOT EXISTS` y `IF col = ...` necesarias para LWT.
4. Implementar built-in functions críticas: count/sum/avg/min/max, now/toTimestamp/toDate, uuid/timeuuid helpers, token(), cast, conversions blob, math, length, operations, toJson/fromJson y resolver de overloads.
5. Agregar las funciones vectoriales y de masking si la baseline las contiene; si dependen de fases posteriores, dejar interfaz estable y tests de integración pendientes marcados pero no rotos.
6. Completar statements faltantes del parser/AST/semántica: `ALTER TYPE`, `ALTER VIEW`, `DESCRIBE`, `LIST USERS/SUPERUSERS`, statements de comentario o security labels si están en baseline, y los transaction/condition statements si apuntan a Accord.
7. Crear tests semánticos exhaustivos por statement y tests diferenciales con Java para errores de validación, selection, functions y conditions.
8. Conectar esta capa con QueryProcessor sin romper compatibilidad wire ni prepared statements.

Criterios de aceptación de esta fase:
- WHERE clause validation real existe y rechaza queries inválidas como Java.
- SELECT procesa selectors/aliases/agregados básicos correctamente.
- IF conditions tienen evaluación real.
- Built-ins críticas funcionan y pasan diff tests.

Entregables mínimos:
- Restrictions framework
- Selection/selector engine
- Conditions/LWT IF evaluation
- Built-in functions y tests

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 04 — Marshal/type system completo, serializers faltantes, term binding y compatibilidad de tipos

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar uno de los gaps estructurales más profundos: no existe el subsistema `marshal/` equivalente. Debes crear el framework de tipos, comparación, parsing, binding y serialización necesario para compatibilidad CQL, índices, scans y wire correctness.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/marshal/**`
- `src/java/org/apache/cassandra/serializers/**`
- `src/java/org/apache/cassandra/cql3/terms/**` o equivalentes
- `src/java/org/apache/cassandra/utils/bytecomparable/**`
- Crates Rust de tipos, CQL, schema y protocolo

Trabajo que debes ejecutar ahora:
1. Diseñar e implementar el equivalente a `AbstractType` y la familia de tipos nativos, collections, tuple, user type, composite, reversed, frozen y parsers asociados.
2. Introducir `ValueAccessor` o un modelo equivalente que permita comparar y serializar valores tipados sin tratarlos como `Vec<u8>` opaco en todo el sistema.
3. Completar serializers faltantes: duration, counter, decimal, integer/bigint arbitrario, simple date, time y errores de marshal.
4. Implementar parsing y binding de `Term` a tipos reales, con coerciones, validación de schema, `bind markers`, IN markers, multi-elements y user types.
5. Definir comparación, ordenamiento y hashing consistentes para partition/clustering keys y para interoperar con índices, range scans y protocol metadata.
6. Integrar el framework con QueryProcessor, schema metadata, rows/partitions, index planning y native protocol results.
7. Añadir pruebas de round-trip encode/decode, ordering, comparison y diff tests con Java para tipos simples, collections, tuple, UDT y edge cases.
8. Documentar invariantes y dejar APIs estables para futuras optimizaciones (off-heap, byte-comparable, BTI/SAI, row filtering).

Criterios de aceptación de esta fase:
- El runtime ya no depende de `Vec<u8>` raw para toda la lógica de tipos.
- Existe un framework tipado comparable al `marshal/` de Java.
- Serializers faltantes principales están implementados.
- Hay diff tests de tipos y serialización contra Java.

Entregables mínimos:
- Framework de tipos/marshal
- Serializers completos
- Binding de terms a tipos
- Tests de compatibilidad de tipos

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 05 — Rows, partitions, filters, transforms, read commands, memtables reales y tries

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Reemplazar las estructuras simplificadas por un modelo de lectura/escritura cercano al Java real: filas, cells complejas, tombstones de rango, iteradores, partitions, filters, transforms, read commands y memtables/tries no triviales.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/rows/**`
- `src/java/org/apache/cassandra/db/partitions/**`
- `src/java/org/apache/cassandra/db/filter/**`
- `src/java/org/apache/cassandra/db/transform/**`
- `src/java/org/apache/cassandra/db/ReadCommand*`
- `src/java/org/apache/cassandra/db/memtable/**`
- `src/java/org/apache/cassandra/db/tries/**`

Trabajo que debes ejecutar ahora:
1. Implementar un subsistema de rows/partitions con `Row`, `Cell`, `ComplexColumnData`, `RangeTombstoneMarker`, `DeletionInfo`, iteradores `Unfiltered` y encoding stats equivalentes.
2. Introducir `DecoratedKey`, jerarquía `Clustering*`, `PartitionUpdate`, `FilteredPartition`, `PartitionIterator` y tipos relacionados necesarios para read/write correctness.
3. Implementar `ReadCommand` y variantes principales, con separación entre command, execution, filtering y result materialization.
4. Construir `ColumnFilter`, `RowFilter`, `DataLimits`, clustering slice/names filters y enlazarlos al read path.
5. Implementar pipeline de `Transformation`/`FilteredRows`/`FilteredPartitions` o equivalente composable para que el read path no sea una colección de ifs ad hoc.
6. Mejorar memtables: sustituir el uso demasiado grueso de `RwLock<BTreeMap>` cuando sea necesario, añadir sharding u optimizaciones estructurales compatibles con el modelo elegido y crear una TrieMemtable real o claramente compatible con la baseline.
7. Cerrar gaps de tries usados por BTI/TrieMemtable y dejar interfaces preparadas para row-level indexes y scans eficientes.
8. Añadir tests de correctness de iteradores, slice queries, range tombstones, tombstone GC interactions y diff tests con Java.

Criterios de aceptación de esta fase:
- El read path usa estructuras de rows/partitions/filters reales, no structs simplificados.
- ReadCommand y filters existen y soportan queries parciales.
- Hay iteradores y transform pipeline verificables.
- Trie/memtable tienen implementación seria y testeada.

Entregables mínimos:
- Rows/partitions framework
- ReadCommand hierarchy
- Filter/transform pipeline
- Memtable/trie improvements y tests

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 06 — StorageService, StorageProxy, reads/writes coordinados, hints y batchlog verb handlers

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el corazón coordinador del sistema. Debes implementar el equivalente funcional de `StorageService`/`StorageProxy`, response handlers, read/write coordination, startup checks y la integración de hints y batchlog con mensajería real.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/StorageService.java`
- `src/java/org/apache/cassandra/service/StorageProxy.java`
- `src/java/org/apache/cassandra/service/reads/**`
- `src/java/org/apache/cassandra/service/WriteResponseHandler*`
- `src/java/org/apache/cassandra/hints/**`
- `src/java/org/apache/cassandra/batchlog/**`
- `src/java/org/apache/cassandra/service/StartupChecks.java`

Trabajo que debes ejecutar ahora:
1. Diseñar e implementar una capa coordinadora robusta para reads y writes con consistency levels, callbacks, timeouts, speculative retry y manejo de réplicas.
2. Materializar `StorageProxy` o equivalente como adaptador estable entre QueryProcessor y los servicios distribuidos.
3. Implementar response handlers de escritura, lectura y batchlog, incluyendo success paths, timeout/failure mapping y métricas.
4. Integrar hints y batchlog con mensajería real: verb handlers, catálogos on-disk, background dispatch y replay seguro.
5. Añadir `StartupChecks` y validaciones de arranque para directories, puertos, config, schema y dependencias mínimas.
6. Integrar materialized views en el write path donde ya existan hooks: evaluar WHERE clause real, recomputar PKs, read-repair/backfill hooks y persistencia coherente.
7. Conectar esta capa con tracing, authz, guardrails, metrics y service state sin formar ciclos de dependencia difíciles de mantener.
8. Escribir tests multi-nodo que verifiquen writes/reads coordinados, hints, batchlog y errores de consistency con diff tests donde sea posible.

Criterios de aceptación de esta fase:
- Existe una capa coordinadora real para reads/writes.
- Hints y batchlog usan verb handlers y flujo distribuido real.
- StartupChecks existen y fallan temprano cuando corresponde.
- MV write fanout deja de depender de TODOs críticos.

Entregables mínimos:
- StorageService/StorageProxy funcionales
- Read/write handlers
- Hints/batchlog integrados
- Tests multi-nodo

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 07 — Gossip real, shadow round, schema exchange y descubrimiento de cluster

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el P0 de gossip: hoy existen estructuras, pero no el loop real de SYN/ACK/ACK2 ni el descubrimiento inicial. Debes construir un gossip operativo con subscribers, quarantine/shutdown y propagación de schema/topology mínima.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/gms/Gossiper.java`
- `src/java/org/apache/cassandra/gms/**`
- `src/java/org/apache/cassandra/schema/SchemaPullVerbHandler*`, `SchemaPushVerbHandler*`, `VersionVerbHandler*`
- `src/java/org/apache/cassandra/service/StorageService.java` (gossip integration)
- `src/java/org/apache/cassandra/tcm/**` para coexistencia con metadata moderna

Trabajo que debes ejecutar ahora:
1. Implementar el loop periódico de gossip con mensajes SYN/ACK/ACK2, heartbeats, selección de peers y propagation budgets.
2. Añadir verb handlers reales, estado de endpoint, subscribers, notifications y transiciones de estado (alive, dead, removed, quarantined, shutdown).
3. Soportar shadow round y descubrimiento inicial para bootstrap y arranque en clusters parcialmente conocidos.
4. Integrar failure detector, seed providers, quarantine logic y clean shutdown gossip.
5. Conectar schema/version exchange y eventos de topology/status para que el cluster pueda converger y notificar cambios.
6. Definir la convivencia entre gossip clásico y los módulos más nuevos de metadata/TCM si la baseline los contiene, evitando inconsistencia de ownership.
7. Agregar pruebas multi-nodo con pérdida/retraso de mensajes, node flaps, seed failures y schema agreement básico.
8. Exponer métricas y virtual tables/herramientas mínimas necesarias para observar el estado de gossip en prompts posteriores.

Criterios de aceptación de esta fase:
- Los nodos se descubren, intercambian estado y convergen.
- Hay loop real de gossip y verb handlers reales.
- Schema/topology notifications básicas existen.
- Pruebas multi-nodo muestran convergencia.

Entregables mínimos:
- Gossip loop operativo
- Subscribers y handlers
- Schema/version exchange base
- Tests de convergencia

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 08 — Mensajería internodo real: conexiones persistentes, 3 canales, CRC/LZ4, resource limits y forwarding

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Eliminar el antipatrón de abrir una TCP nueva por `send()` y llevar la mensajería internodo a un estado utilizable en producción: conexiones persistentes, colas, integridad de frames, límites y soporte de forwarding.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/net/OutboundConnection*`
- `src/java/org/apache/cassandra/net/OutboundConnections.java`
- `src/java/org/apache/cassandra/net/FrameDecoder*`, `FrameEncoder*`
- `src/java/org/apache/cassandra/net/OutboundMessageQueue.java`
- `src/java/org/apache/cassandra/net/HandshakeProtocol*`
- `src/java/org/apache/cassandra/net/ResourceLimits*`, `ForwardingInfo.java`

Trabajo que debes ejecutar ahora:
1. Implementar conexiones salientes persistentes por peer con reconexión, heartbeats, retry policy, expiración de mensajes y shutdown correcto.
2. Soportar el modelo de tres canales/clases de tráfico (urgent/small/large) o un equivalente explícitamente documentado y medible.
3. Añadir colas de salida con pruning, expiración, backpressure y métricas de dropped/expired/throttled messages.
4. Implementar CRC y compresión LZ4/Snappy de frames internodo si aplica a la baseline, con recuperación ante frames corruptos y telemetría.
5. Completar handshake protocol y negociación de versión/capacidades con retries y downgrade cuando corresponda.
6. Añadir `ForwardingInfo`, headers ricos y soporte para request forwarding si el read/write path lo necesita.
7. Incorporar `ResourceLimits` por endpoint y globales para evitar blow-ups de memoria bajo carga o peers lentos.
8. Escribir pruebas de interoperabilidad internodo, tests de red degradada y benchmarks comparando la nueva capa con la implementación anterior de nueva TCP por envío.

Criterios de aceptación de esta fase:
- Ya no se abre una TCP nueva por mensaje.
- Hay colas, backpressure y reconexión persistente.
- Los frames internodo tienen integridad y compresión donde corresponde.
- Pruebas y benchmarks demuestran mejora clara.

Entregables mínimos:
- Persistent messaging layer
- Frame integrity/compression
- Resource limits y forwarding
- Tests/benchmarks internodo

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 09 — CompactionManager, lifecycle transactions, execution engine, journal y eventos de storage

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el hueco entre estrategias de compaction ya implementadas y la ejecución real. Debes materializar el engine, task hierarchy, control, trackers, lifecycle transactions crash-safe y la instrumentación/eventos asociados.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/compaction/**`
- `src/java/org/apache/cassandra/db/lifecycle/**`
- `src/java/org/apache/cassandra/db/journal/**`
- `src/java/org/apache/cassandra/notifications/**`
- `src/java/org/apache/cassandra/db/monitoring/**`

Trabajo que debes ejecutar ahora:
1. Implementar `CompactionManager` o equivalente con scheduling, throttling, ejecución concurrente, cancelación y observabilidad.
2. Crear `CompactionTask` hierarchy, iterators/controladores y writers necesarios para ejecutar STCS/LCS/TWCS/UCS de forma real.
3. Implementar lifecycle transactions crash-safe para mutaciones del set de SSTables y reemplazos atómicos durante flush/compaction/repair.
4. Incorporar logging de compaction, tracker de activas, pending repair awareness y manifest/generations para LCS.
5. Añadir o adaptar un journal interno si la baseline lo requiere como fundamento del storage moderno; si se decide no replicar 1:1, dejar compatibilidad operativa documentada y tests de crash safety equivalentes.
6. Integrar eventos/notificaciones de memtables/SSTables/truncation para que caches, virtual tables, metrics y tooling puedan observar cambios.
7. Conectar compaction con metrics, admin tools y virtual tables que se implementarán después.
8. Escribir tests de compaction correctness, crash/restart, pending repair interaction, anti-compaction y long-running background compaction.

Criterios de aceptación de esta fase:
- Compaction deja de ser solo estrategia y pasa a ser ejecución real.
- Lifecycle transactions existen y hacen crash-safe los cambios de SSTables.
- Hay tracker/metrics/logging de compactions activas.
- Tests de reinicio y correctness pasan.

Entregables mínimos:
- Compaction execution engine
- Lifecycle transactions
- Journal/events integration
- Tests de compaction y crash-safety

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 10 — IO util layer, compresión, mmap/rebufferers, buffer pools y primitives de acceso a disco

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Construir la capa de I/O que el gap analysis marca como enteramente ausente: readers/writers avanzados, rebuffering, mmap, checksum, buffer pools y compresión reutilizable. Sin esto, la performance y la compatibilidad de SSTables seguirán muy lejos de Cassandra.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/io/util/**`
- `src/java/org/apache/cassandra/io/compress/**`
- `src/java/org/apache/cassandra/db/compression/**`
- `src/java/org/apache/cassandra/io/sstable/format/**` para integración

Trabajo que debes ejecutar ahora:
1. Implementar una capa de `DataInputPlus`/`DataOutputPlus` o equivalente con primitives reutilizables, varints, checksums y soporte para distintos backing stores.
2. Crear `RandomAccessReader`, `SequentialWriter`, `FileHandle`, `Rebufferer` y buffer pools con abstractions limpias para usar desde SSTable readers/writers y streaming.
3. Añadir soporte de mmap o estrategia equivalente cuando aporte valor real, con fallback portable y tests de correctness.
4. Implementar compresión de SSTables y segmentos/hints cuando la baseline lo requiera: LZ4, Snappy, Zstd o las necesarias, con metadata y checksums correctos.
5. Añadir `DiskOptimizationStrategy` o criterios configurables para tamaños de buffer y patrones de acceso.
6. Conectar esta capa con commitlog, SSTables, streaming, tools offline y benchmarks.
7. Definir límites claros de memoria y pooling para no degradar bajo cargas mixtas.
8. Escribir benchmarks de lectura/escritura secuencial/aleatoria, tests de corrupción/checksum y comparaciones con la capa `std::fs` simple previa.

Criterios de aceptación de esta fase:
- Existe una capa de IO util reutilizable por todo el storage engine.
- SSTables y otros componentes pueden usar compresión real.
- Hay buffer pools/rebufferers y tests de corrupción/checksum.
- Benchmarks muestran mejora frente al acceso simplificado anterior.

Entregables mínimos:
- IO util layer completa
- Compression framework
- Buffer pools/mmap/rebufferers
- Benchmarks y tests de I/O

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 11 — SSTables completas: compatibilidad/versioning, index summary, key cache, range tombstones y herramientas offline

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la brecha de SSTable I/O más allá del lector/escritor básico: versionado, metadatos binarios, index summary, key cache, scans por rango, reverse iteration, range tombstones y herramientas offline/migración compatibles o explícitamente traducibles.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/io/sstable/**`
- `src/java/org/apache/cassandra/io/sstable/format/big/**`
- `src/java/org/apache/cassandra/io/sstable/format/bti/**`
- `src/java/org/apache/cassandra/io/sstable/indexsummary/**`
- `src/java/org/apache/cassandra/cache/**` (key cache integration)
- `src/java/org/apache/cassandra/tools/**` para scrub/verify/loader/upgrader

Trabajo que debes ejecutar ahora:
1. Implementar version handling real de SSTables en vez de un único `DATA_VERSION = 1`, con flags/capacidades por versión y validación estricta.
2. Reemplazar metadata JSON por metadatos binarios compatibles o definir formalmente un formato de traducción/migración completo y comprobable.
3. Completar lectura/escritura de index summary, key cache y row-level index cuando la variante de formato lo requiera.
4. Soportar range tombstones, scans por token range, reverse iteration y scanners filtrados sin devolver siempre la tabla completa.
5. Implementar `SSTableRewriter`, `Importer`, `Loader`, `Scrubber`, `Verifier`, `Upgrader` y herramientas offline mínimas necesarias para migración y operaciones.
6. Añadir zero-copy paths cuando el diseño lo permita y sea medible, o dejar interfaces claras para el prompt de performance final.
7. Definir y probar una estrategia oficial de compatibilidad: lectura/escritura compatible con Java si es alcanzable para la baseline, o herramienta de conversión/migración completa si no lo es.
8. Escribir un corpus de golden SSTables y pruebas diferenciales que comparen lecturas, metadata y scans con Java.

Criterios de aceptación de esta fase:
- SSTables tienen versionado y metadata serios.
- Index summary y key cache foundation existen.
- Hay scanners/iterators correctos y range tombstones soportados.
- Existe una historia de migración/compatibilidad verificable.

Entregables mínimos:
- SSTable versioning y metadata
- Index summary/key cache hooks
- Offline tools esenciales
- Corpus de golden SSTables y diff tests

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 12 — Streaming real: transporte de red, protocolo de mensajes, receiver, coordinator y zero-copy paths

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Transformar el streaming de una state machine puramente estructural a un subsistema operativo de red capaz de mover SSTables/datos para bootstrap, rebuild, replace, repair y range movement.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/db/streaming/**`
- `src/java/org/apache/cassandra/net/**` para integración de transporte
- `src/java/org/apache/cassandra/dht/RangeStreamer.java`

Trabajo que debes ejecutar ahora:
1. Implementar el protocolo de mensajes de streaming, serialización, coordinación de sesiones y transporte de red real.
2. Crear `StreamCoordinator`, `StreamResultFuture`, receiver/deserializer y write-to-disk paths robustos.
3. Integrar streaming con mensajería persistente o canales dedicados según la arquitectura elegida, con rate limiting y backpressure medibles.
4. Soportar compresión, checksums, reintentos, aborts y cleanups de sesiones fallidas.
5. Conectar streaming con bootstrap, rebuild, replace, repair sync tasks y herramientas operativas.
6. Añadir zero-copy o sendfile-like paths cuando correspondan y estén soportados por la plataforma objetivo, sin comprometer correctness.
7. Crear pruebas multi-nodo que validen transferencia real de datos y recuperación ante fallos de red o disco.
8. Documentar runbooks básicos y surfaces observables para nodetool/virtual tables/metrics.

Criterios de aceptación de esta fase:
- Streaming mueve datos reales por red y escribe en disco.
- Bootstrap/rebuild/replace pueden usarlo.
- Hay rate limits, retries y checksums reales.
- Pruebas multi-nodo y de fallo pasan.

Entregables mínimos:
- Streaming protocol/network transport
- Receiver/coordinator
- Integración con topology ops
- Tests multi-nodo de transferencia real

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 13 — Repair completo: validator, sync tasks, consistent repair, auto-repair y tablas/estado operativos

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el repair más allá de Merkle trees básicos: consistent repair, validators sobre SSTables, sync tasks reales, mensajes completos, repair virtual tables y políticas de paralelismo.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/repair/**`
- `src/java/org/apache/cassandra/service/ActiveRepairService.java`
- `src/java/org/apache/cassandra/streaming/**` para sync/repair integration

Trabajo que debes ejecutar ahora:
1. Implementar `Validator`, `RepairJob`, `SyncTask`, `RemoteSyncTask`, `StreamingRepairTask` y el flujo completo de comparación/sincronización entre réplicas.
2. Materializar consistent repair: coordinator/local sessions, persistencia del estado, reanudación y validación de repaired data.
3. Completar los mensajes y estados faltantes de repair, incluyendo auto-repair y/o asymmetric repair si existen en la baseline.
4. Soportar `RepairParallelism` y políticas configurables (sequential, parallel, dc-parallel) con tests apropiados.
5. Integrar repair con compaction, pending repair manager, anti-compaction, streaming y nodetool surfaces.
6. Crear virtual tables, history tracking y métricas útiles para observar repair sessions y repaired data divergence.
7. Añadir tests multi-nodo de full/incremental/preview repair, validación de repaired-at, fallos intermedios y reanudación.
8. Usar el flujo de repair real como parte de la historia de cambio de RF/topology y no como subsistema aislado.

Criterios de aceptación de esta fase:
- Repair puede validar, comparar y sincronizar réplicas reales.
- Consistent repair existe con estado persistente.
- Hay surfaces operativas y nodetool integration básica.
- Pruebas multi-nodo de repair pasan.

Entregables mínimos:
- Validator/sync tasks
- Consistent repair
- Repair state surfaces
- Tests multi-nodo de repair

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 14 — Auth completo: persistencia system_auth, caches, CIDR groups, mTLS auth y auth internodo/red

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Pasar de auth in-memory a un subsistema de seguridad utilizable en cluster: persistencia real en `system_auth`, caches, mTLS identities, CIDR groups y autenticación/autorización para cliente, red e internodo.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/auth/**`
- `src/java/org/apache/cassandra/security/**`
- `src/java/org/apache/cassandra/schema/system_auth*` o keyspaces equivalentes
- Crates Rust de security/auth/authz/roles/cidr

Trabajo que debes ejecutar ahora:
1. Persistir roles, permisos, credenciales y metadatos en `system_auth` o sistema equivalente durable y replicado, eliminando el estado solo en memoria.
2. Implementar `AuthCache`/`PermissionsCache`/`RolesCache` o equivalentes con invalidez correcta ante cambios.
3. Completar CIDR groups management y su integración con el authorizer.
4. Soportar autenticadores de mTLS e identidades si la baseline los incluye, con mapping a roles y validación de certificados.
5. Implementar `INetworkAuthorizer` e `IInternodeAuthenticator` o equivalentes para controlar acceso de clientes y entre nodos.
6. Conectar auth con QueryProcessor, native transport, tooling, config y system keyspaces.
7. Escribir pruebas de auth multi-nodo, invalidación de caches, failover y compatibilidad de permisos.
8. Cerrar stubs de verificación de credenciales en native protocol y server.

Criterios de aceptación de esta fase:
- Roles/permisos sobreviven reinicios y replican donde corresponde.
- Caches de auth existen e invalidan correctamente.
- mTLS/CIDR/network/internode auth están soportados según baseline.
- Los stubs de credenciales desaparecen.

Entregables mínimos:
- system_auth persistence
- Auth caches
- mTLS/CIDR/network/internode auth
- Tests de seguridad y cluster

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 15 — Security y config runtime: DatabaseDescriptor, hot reload, properties, guardrails y encryption-at-rest

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar los gaps de configuración/runtime y de seguridad de almacenamiento: `DatabaseDescriptor`, propiedades relevantes, hot reload, guardrails, crypto provider y encryption-at-rest si la baseline lo soporta.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/config/**`
- `src/java/org/apache/cassandra/db/guardrails/**`
- `src/java/org/apache/cassandra/security/**`
- `src/java/org/apache/cassandra/service/CassandraDaemon.java` para boot/runtime integration

Trabajo que debes ejecutar ahora:
1. Implementar un `DatabaseDescriptor` o runtime descriptor equivalente que unifique config efectiva, overrides, derived defaults y acceso thread-safe a parámetros globales.
2. Añadir soporte para `CassandraRelevantProperties`, environment overrides y documentación clara del precedence model.
3. Implementar hot reload de parámetros seguros, incluyendo TLS cuando ya exista soporte, y rechazar o reiniciar de forma controlada lo que no sea hot-reloadable.
4. Construir el framework de guardrails: thresholds, enable flags, password validation u otros guardrails presentes en la baseline, con surfaces de config y tests.
5. Implementar `RepairConfig`, `StorageAttachedIndexOptions`, `TransparentDataEncryptionOptions` y otras config classes faltantes relevantes a la baseline.
6. Introducir crypto provider abstraction y encryption-at-rest para commitlog/SSTables u objetos que la baseline soporte, con key management claramente documentado.
7. Conectar config/guardrails con QueryProcessor, storage, streaming, repair, tools y virtual tables de settings.
8. Escribir tests de config loading, override precedence, hot reload, guardrails enforcement y encryption smoke tests.

Criterios de aceptación de esta fase:
- Existe una capa runtime de config/descriptor seria.
- Guardrails funcionan y pueden activarse/configurarse.
- Hay hot reload donde es seguro y explícitamente documentado.
- Security at rest/crypto provider quedan implementados o formalmente gated con tests.

Entregables mínimos:
- DatabaseDescriptor equivalente
- Guardrails framework
- Hot reload/config precedence
- Crypto/encryption-at-rest integration

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 16 — Índices core: Index API, registry, planner y motor legacy 2i completo

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar el vacío total de ejecución de índices arrancando por la base común: API de índices, registro, lifecycle, planner y un motor legacy 2i funcional.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/index/**`
- `src/java/org/apache/cassandra/index/internal/**`
- `src/java/org/apache/cassandra/cql3/restrictions/**` para integración con planner
- `src/java/org/apache/cassandra/schema/IndexMetadata*`

Trabajo que debes ejecutar ahora:
1. Definir `Index` interface/trait, `IndexRegistry`, `SecondaryIndexManager` y lifecycle de build/rebuild/drop/reload.
2. Implementar el motor legacy 2i con mantenimiento en write path, consultas indexadas y rebuilds mínimos funcionales.
3. Integrar la planificación de queries con restrictions y selección de índices aplicables.
4. Conectar índices con schema metadata, system tables, rebuild tasks, compaction/flush y herramientas de administración.
5. Añadir soporte para índices en el planner CQL y errores/validaciones compatibles.
6. Implementar persistencia y recovery del estado del índice.
7. Escribir tests de insert/update/delete/query/rebuild/drop para 2i y diff tests con Java en casos básicos.
8. Preparar la base común para SAI/SASI sin mezclar todavía todos los motores en una sola clase espagueti.

Criterios de aceptación de esta fase:
- Existe una infraestructura común de índices.
- Legacy 2i funciona de punta a punta.
- El planner puede elegir índices y validar restricciones.
- Hay rebuild/recovery tests.

Entregables mínimos:
- Index API/registry/lifecycle
- Legacy 2i engine
- Planner integration
- Tests y rebuild flows

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 17 — SAI, SASI experimental, vector search y funciones asociadas

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la larga cola de indexación moderna y experimental: SAI, SASI si sigue presente en la baseline, vector search y funciones relacionadas. Debes hacerlo con clasificación explícita estable/experimental y surfaces operativas claras.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/index/sai/**`
- `src/java/org/apache/cassandra/index/sasi/**`
- `src/java/org/apache/cassandra/cql3/functions/**` (vector/masking helpers)
- `src/java/org/apache/cassandra/config/StorageAttachedIndexOptions*`
- Planner CQL, schema metadata y storage engine

Trabajo que debes ejecutar ahora:
1. Implementar SAI con su lifecycle de build/rebuild/update/delete, integración con memtables/SSTables y query planning real.
2. Clasificar SASI según la baseline: si existe como experimental, implementarla o aislarla con feature flags, documentación y tests que preserven la postura operativa del Java.
3. Integrar el tipo/vector search con el planner, funciones vectoriales, metadata de índices y surfaces de consulta compatibles.
4. Conectar SAI/SASI/vector con compaction, flush, rebuild, repair y herramientas de administración.
5. Añadir options/config/runtime validation y metrics para cada motor de índice.
6. Escribir tests de correctness y performance para búsqueda indexada, rebuild, schema changes y mixed workloads.
7. Completar las funciones CQL necesarias para operar con vector search y masking si dependen de esta fase.
8. No escondas diferencias difíciles: deja explícitos los límites si alguna feature se implementa por etapas, siempre con tests y guard-rails.

Criterios de aceptación de esta fase:
- SAI tiene motor y planner funcionales.
- SASI queda implementada o gated explícitamente según baseline.
- Vector search se integra con índices y CQL.
- Hay rebuild/config/tests operativos.

Entregables mínimos:
- SAI engine
- SASI classification/implementation
- Vector search integration
- Tests y docs operativas

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 18 — Schema completo: metadata de views/triggers/UDF/UDA, distributed schema y materialized views completas

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar los gaps del catálogo/schema y completar materialized views de verdad: metadata, distributed schema, change listeners, propagation y runtime de views/triggers/UDF/UDA dentro del alcance de la baseline.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/schema/**`
- `src/java/org/apache/cassandra/db/view/**` o equivalentes
- `src/java/org/apache/cassandra/triggers/**`
- `src/java/org/apache/cassandra/cql3/functions/**` (UDF/UDA metadata/runtime)
- `src/java/org/apache/cassandra/db/virtual/**` y system keyspaces donde aplique

Trabajo que debes ejecutar ahora:
1. Completar metadata faltante: `ViewMetadata`, triggers, UDT/UDF/UDA catalog management, caching/compaction/compression/memtable params y dropped columns/diffs.
2. Implementar `SchemaTransformation`, `SchemaChangeListener`, `SchemaChangeNotifier`, pull/push/version handlers y `DistributedSchema` o equivalente.
3. Cerrar materialized views: `ViewBuilder` backfill, evaluación real de WHERE clause, recomputación de PK, read-repair/backfill consistency y rebuild flows.
4. Integrar schema changes con gossip/TCM según baseline, QueryProcessor, prepared invalidation, auth surfaces y tooling.
5. Implementar o cerrar stubs del runtime de triggers y del módulo UDF/UDA si forman parte de la baseline; si una parte se gatea, documentar y testear el límite.
6. Añadir system tables y surfaces necesarias para observar schema agreement, versioning y cambios distribuidos.
7. Escribir tests multi-nodo de schema agreement, alter/drop/rebuild de views, invalidación de prepared statements y errores de metadata.
8. Dejar el catálogo como fuente unificada de verdad para prompts posteriores de virtual tables, tools y migration.

Criterios de aceptación de esta fase:
- Schema metadata cubre views/triggers/UDF/UDA y parámetros faltantes.
- Distributed schema/change propagation existe.
- Materialized views dejan de tener TODOs críticos y funcionan con backfill/rebuild.
- Prepared invalidation y schema agreement están probados.

Entregables mínimos:
- Schema catalog ampliado
- Distributed schema/change listeners
- Materialized views completas
- Tests multi-nodo de schema

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 19 — Virtual tables, system keyspaces, tracing persistente, audit filters, notifications y diagnostics

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar toda la superficie operativa que el gap analysis marca ausente o parcial: virtual tables, system keyspaces, tracing persistido, filtros de auditoría, notifications y diagnostics.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/virtual/**`
- `src/java/org/apache/cassandra/tracing/**`
- `src/java/org/apache/cassandra/audit/**` o equivalentes
- `src/java/org/apache/cassandra/notifications/**`
- `src/java/org/apache/cassandra/diag/**` o diagnostic event service equivalente
- `src/java/org/apache/cassandra/db/SystemKeyspace*` y `system_traces`

Trabajo que debes ejecutar ahora:
1. Construir la infraestructura de virtual tables/keyspaces y materializar las tablas operativas más importantes para settings, clients, internode messaging, compactions, repairs, caches y tracing.
2. Implementar `SystemKeyspace` y system tables mínimas necesarias para runtime, auth, tracing, schema, hints y operación.
3. Persistir tracing en `system_traces` o equivalente con TTL/expiry y manager global de tracing.
4. Completar filtros de auditoría, `AuditLogContext` por request y, si la baseline lo requiere, `BinAuditLogger` o un equivalente compatible/documentado.
5. Implementar notifications/diagnostics para eventos de SSTables, memtables, truncation, compaction y runtime operativo.
6. Conectar virtual tables y diagnostics con metrics, nodetool/tools y surfaces HTTP/JMX equivalentes si existen.
7. Escribir tests de system tables, tracing persistence, audit filters y virtual table queries a través de CQL.
8. Documentar claramente qué virtual tables quedan incluidas según baseline y cómo se actualizan.

Criterios de aceptación de esta fase:
- Existen virtual tables y system keyspaces operativas.
- Tracing se persiste y expira correctamente.
- Audit filters funcionan y existe contexto por request.
- Notifications/diagnostics son observables.

Entregables mínimos:
- Virtual tables infra + implementations
- System keyspaces/tables
- Tracing persistence
- Audit/diag/notification integration

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 20 — Caches completas: key cache, row cache, counter cache, chunk cache y autosave

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la ausencia total de cachés y el impacto severo que eso tiene en read performance. Debes diseñar e implementar caches persistibles y observables que encajen con SSTables, compaction y tooling.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/cache/**`
- `src/java/org/apache/cassandra/io/util/**` (chunk cache interaction)
- `src/java/org/apache/cassandra/io/sstable/**`
- `src/java/org/apache/cassandra/service/CacheService.java`
- `src/java/org/apache/cassandra/metrics/**` para cache metrics

Trabajo que debes ejecutar ahora:
1. Definir interfaces `ICache`/providers equivalentes y elegir implementaciones apropiadas para key/row/counter/chunk cache.
2. Implementar key cache y row cache integradas con SSTable readers, scanners y read path, incluyendo invalidación por compaction/rewrite/truncate.
3. Añadir counter cache si la baseline lo requiere y está conectada al path de counters/LWT.
4. Construir autosave/persistencia a disco de caches cuando aplique, con carga al arranque y formatos versionados.
5. Integrar chunk cache con la capa IO util y medir su efecto real.
6. Añadir metrics, virtual tables o tools para observar tamaño, hits, misses, evictions y save/load status.
7. Crear tests de correctness bajo compaction, schema changes, truncation y restart.
8. Benchmarcar lecturas calientes/frías antes y después para demostrar impacto.

Criterios de aceptación de esta fase:
- Existen key/row/chunk cache operativas y observables.
- La invalidación ante cambios de SSTables es correcta.
- Hay persistencia/autosave donde corresponde.
- Benchmarks muestran mejora medible.

Entregables mínimos:
- Cache subsystem completa
- Autosave/load
- Metrics y observabilidad de caches
- Benchmarks de read performance

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 21 — TCM/CMS completos: commit protocol, listeners, ownership, sequences, migration y snapshots

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar los gaps de Transactional Cluster Metadata: metadata linealizable, commit/processor, listeners, migration, ownership y sequences para bootstrap/leave/move, respetando la baseline moderna si apunta a TCM/CMS.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/tcm/**`
- `src/java/org/apache/cassandra/tcm/log/**`
- `src/java/org/apache/cassandra/tcm/membership/**`
- `src/java/org/apache/cassandra/tcm/ownership/**`
- `src/java/org/apache/cassandra/tcm/sequences/**`
- `src/java/org/apache/cassandra/tcm/migration/**`
- `src/java/org/apache/cassandra/tcm/listeners/**`

Trabajo que debes ejecutar ahora:
1. Implementar el commit/processor de TCM/CMS con persistencia del metadata log, snapshots y validación de epochs/versiones.
2. Completar `ClusterMetadata`/ownership/data placements/movement maps/versioned endpoints para que sirvan como fuente de verdad del cluster cuando la baseline lo requiera.
3. Implementar listeners, discovery/startup y el flujo de arranque/migración hacia CMS/TCM.
4. Completar sequences de bootstrap/leave/move/removenode/replace, con step tracking, locked ranges y recovery tras reinicio.
5. Integrar TCM con gossip, StorageService, schema propagation, topology changes, repair y tools.
6. Añadir serialization versionada/wire-compatible donde corresponda en lugar de confiar solo en `serde` trivial.
7. Escribir pruebas de metadata convergence, epoch advancement, crash/restart, migration y topology operations.
8. Documentar claramente la relación entre TCM y los modos clásicos si la baseline conserva ambos.

Criterios de aceptación de esta fase:
- TCM tiene commit protocol y metadata log persistente.
- Ownership/sequences/listeners están implementados.
- Topology ops pueden apoyarse en TCM/CMS.
- Tests de epochs/migration/topology pasan.

Entregables mínimos:
- TCM/CMS commit protocol
- Metadata log/snapshots
- Ownership/sequences/listeners
- Tests de convergencia y migration

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 22 — Paxos/LWT persistentes, Accord/consensus bridge y transaction statements

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar los gaps de consistencia fuerte y trunk-only: Paxos/LWT persistentes, routers/bridges de consenso y transaction statements si la baseline moderna incorpora Accord o rutas de migración de consenso.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/paxos/**`
- `src/java/org/apache/cassandra/service/accord/**`
- `src/java/org/apache/cassandra/service/consensus/**`
- `src/java/org/apache/cassandra/cql3/statements/Transaction*` y `Condition*` si existen
- `src/java/org/apache/cassandra/service/StorageProxy.java`

Trabajo que debes ejecutar ahora:
1. Implementar persistencia/recuperación de Paxos/LWT state, ballots, prepare/propose/commit y timeouts/result mapping completos.
2. Integrar IF conditions del prompt 03 con el runtime LWT real del coordinador y storage.
3. Completar `service/paxos` gaps: repairs, contention strategies, cleanup tras restart y métricas.
4. Si la baseline contiene `service.accord`/`service.consensus`, implementar o aislar explícitamente la ruta de Accord, el router de requests y la migración de consenso con feature gates y tests.
5. Añadir `TransactionStatement`/`ConditionStatement` y surfaces CQL necesarias si existen en la baseline congelada.
6. Conectar counters y operaciones que interaccionen con LWT/consenso si la baseline así lo hace.
7. Escribir pruebas multi-nodo de LWT, retries, ambiguous results, restart recovery y compatibilidad del router de consenso.
8. Documentar claramente cuándo se usa Paxos clásico, cuándo se enruta a Accord y cómo se migra/observa.

Criterios de aceptación de esta fase:
- LWT/Paxos sobreviven reinicios y funcionan multi-nodo.
- Los bridges de consenso están implementados o gated explícitamente con tests.
- Transaction statements de la baseline existen.
- Hay pruebas multi-nodo y documentación de modo/transición.

Entregables mínimos:
- Paxos/LWT persistence
- Accord/consensus bridge
- Transaction statements
- Tests multi-nodo de consistencia fuerte

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 23 — DHT, locator, snitches cloud, partitioners, token allocator, range streamer y transient replication

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Completar el plano de particionado y colocación: partitioner abstraction, token allocation, snitches cloud reales, replica plans y transient replication.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/dht/**`
- `src/java/org/apache/cassandra/locator/**`
- `src/java/org/apache/cassandra/dht/tokenallocator/**`
- `src/java/org/apache/cassandra/dht/RangeStreamer.java`
- `src/java/org/apache/cassandra/config/**` para snitch config

Trabajo que debes ejecutar ahora:
1. Introducir `IPartitioner` o equivalente y soportar los partitioners de la baseline, al menos con clasificación explícita si alguno queda limitado temporalmente.
2. Implementar token allocator vnode-aware, bootstrap token selection, `RangeStreamer` y replica plans/typed collections.
3. Completar snitches cloud reales (EC2/GCE/Azure/Alibaba/Cloudstack según baseline) con clientes HTTP/metadata seguros y testeables.
4. Implementar `MetaStrategy`, `RemoteStrategy`, `SystemStrategy` y `ReplicationFactor` tipado con awareness de réplicas transitorias.
5. Añadir `EndpointsForToken/Range`, replica plans, transient replication y `additional_write_policy` si la baseline lo requiere.
6. Conectar locator/placement con gossip/TCM/topology/repair/streaming y nodetool surfaces.
7. Escribir pruebas de placement por datacenter/rack/cloud, bootstrap balanceado y cambios de topología.
8. Dejar claramente documentadas las estrategias soportadas y sus limitaciones.

Criterios de aceptación de esta fase:
- Partitioners/allocator/range streaming existen.
- Cloud snitches obtienen metadata real o tienen harnesses equivalentes.
- Transient replication y replica plans están representados correctamente.
- Placement tests multi-DC pasan.

Entregables mínimos:
- DHT/locator completos
- Token allocator/range streamer
- Cloud snitches
- Tests de placement/topology

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 24 — Tooling paridad: nodetool, cqlsh/admin surfaces, offline tools, bootstrap monitor y load tools

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la enorme brecha de tooling. Debes llevar la CLI/operación a un nivel usable: nodetool commands clave, herramientas offline de SSTable, bootstrap monitor, token generator, sstableloader y compatibilidad de surfaces administrativas.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/tools/**`
- `src/java/org/apache/cassandra/tools/nodetool/**`
- `tools/bin/**` y scripts relevantes
- `src/java/org/apache/cassandra/troubleshooting/use_nodetool` docs/surfaces
- Crates Rust de tools/admin/http API

Trabajo que debes ejecutar ahora:
1. Inventariar los comandos de nodetool de la baseline y priorizar una implementación que cubra primero operaciones realmente necesarias para bootstrap, repair, topology, compaction, caches, tracing, audit y diagnostics.
2. Reemplazar stubs por implementaciones reales para los comandos ya presentes y añadir los comandos faltantes más críticos.
3. Implementar herramientas offline: scrubber, splitter, upgrader, verifier, token generator, sstable metadata/dump, repairedAt setters y equivalentes de bootstrap monitor.
4. Crear surfaces administrativas claras (HTTP/JMX-equivalent/CLI direct) y documentar cuál es la fuente de verdad para cada comando.
5. Conectar tools con virtual tables, metrics, tracing, schema/topology y storage internals.
6. Asegurar que `cqlsh` y/o herramientas de administración de cliente funcionen con el server real cuando aplique.
7. Escribir pruebas de CLI, golden output tests y smoke tests de herramientas offline sobre artefactos reales.
8. Documentar cobertura de tooling y las diferencias intencionales respecto a nodetool Java si alguna surface se rediseña.

Criterios de aceptación de esta fase:
- Los stubs principales de nodetool desaparecen.
- Existen herramientas offline críticas funcionales.
- Las surfaces administrativas están conectadas al runtime real.
- Hay tests de CLI y golden outputs.

Entregables mínimos:
- Tooling CLI/ops funcional
- Offline SSTable tools
- Nodetool command coverage priorizada
- Tests y docs de operación

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 25 — Metrics, exceptions, utils, concurrency stages, TODO/stub cleanup y unignore de gap guards

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la deuda transversal que sostiene muchos de los gaps restantes: métricas faltantes, excepciones especializadas, utils/trees/histograms/timeuuid, stages observables, limpieza de stubs/TODOs y recuperación de tests ignorados.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/metrics/**`
- `src/java/org/apache/cassandra/exceptions/**`
- `src/java/org/apache/cassandra/utils/**`
- `src/java/org/apache/cassandra/utils/concurrent/**`
- `rust/crates/**/gap_guards.rs` y tests ignorados
- Todos los crates con TODOs/stubs listados en el gap analysis

Trabajo que debes ejecutar ahora:
1. Extender la superficie de métricas para cubrir por lo menos table/keyspace/commitlog/cache/messaging/client/request/storage/repair/tracing/paxos/TCM según la baseline y la arquitectura Rust elegida.
2. Implementar/mapea excepciones especializadas faltantes y su traducción correcta al protocolo, logs y tooling.
3. Añadir utilidades faltantes realmente usadas por el diseño final: EstimatedHistogram, TimeUUID helpers, bytecomparable, vint, trees/range structures, progress/binlog/memory helpers donde apliquen.
4. Si la arquitectura async en Rust sustituye la infraestructura `Stage` Java, crear nombres/labels/metrics equivalentes para tracing y observabilidad de pools/colas/tareas.
5. Eliminar o cerrar TODOs/stubs de crates críticos; donde alguno deba sobrevivir temporalmente, conviértelo en un issue/gap trazado con tests y flags, no en comentario flotante.
6. Reactivar los `gap_guards` ignorados a medida que la funcionalidad quede implementada y convertirlos en pruebas reales de regresión.
7. Crear un resumen mecánico de coverage actualizado para comparar antes/después de esta fase.
8. Dejar el repo con deuda explícita muy acotada y auditable.

Criterios de aceptación de esta fase:
- La cobertura de métricas aumenta de forma visible y documentada.
- Excepciones y utils críticas están implementadas.
- Los TODOs/stubs peligrosos desaparecen o quedan formalizados.
- Gap guards reactivados cubren los cierres de esta serie.

Entregables mínimos:
- Metrics expansion
- Exceptions/utils improvements
- Stub/TODO cleanup
- Gap guards reactivados

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


# Prompt 26 — Validación final: cluster de 3 nodos, diff tests, chaos/soak, migración, rollback y RC gates

```text

Actúa como Principal Engineer y Staff+ maintainer de una reescritura total de Apache Cassandra de Java a Rust. No estás haciendo un prototipo ni una demo: estás cerrando gaps reales contra un análisis archivo-a-archivo entregado por el usuario.

Contexto obligatorio:
- Ya existe una reescritura parcial en Rust con múltiples crates.
- Existe un gap analysis comparando `src/java/org/apache/cassandra/` (~3168 archivos Java) contra `rust/crates/` (~209 archivos Rust, ~75k LOC).
- Ese gap analysis es tu documento de control y **no puedes ignorarlo**.
- Debes trabajar dentro del repo real, usando Java como oráculo de comportamiento y preservando compatibilidad funcional, wire, operativa y de tooling dentro de la baseline congelada.
- Este prompt pertenece a una serie ordenada. Debes asumir que los prompts anteriores de esta serie ya se ejecutaron y que debes dejar el repositorio listo para el siguiente.
- Tu trabajo debe cerrar los gaps del análisis, no esconderlos.

Reglas no negociables:
1. **Fuente de verdad**:
   - El comportamiento observable del Cassandra Java congelado en la baseline.
   - El gap analysis del usuario.
   - Los tests existentes, dtests, tooling y documentación oficial.
2. **Cobertura total**:
   - Si el gap analysis lista una clase, familia o subsistema como faltante, parcial, stub o simplificado, debes clasificarlo y abordarlo.
   - No aceptes “future work” ni “out of scope” sin dejar: feature flag o fallback seguro, tests que documenten el límite, documento de compatibilidad y criterio concreto de cierre.
3. **Incremental y verificable**:
   - El repo debe quedar compilable.
   - Cada capacidad nueva debe traer tests, docs, scripts y/o harnesses reproducibles.
   - Si tocas rutas calientes, añade benchmark/perfil o justifica por qué no toca todavía.
4. **Compatibilidad**:
   - Mantén compatibilidad con CQL, protocolo nativo, mensajería internodo, SSTables/migración, tooling, nodetool, repair, streaming, topología, seguridad y surfaces operativas según la baseline.
5. **Calidad de implementación**:
   - Minimiza `unsafe`.
   - Documenta invariantes.
   - Evita introducir deuda oculta.
   - No borres Java ni el harness diferencial.
6. **Salida esperada**:
   - Código, tests, documentación, scripts, benchmarks/harnesses cuando aplique y un reporte final breve con:
     1) qué cambiaste,
     2) archivos tocados,
     3) tests añadidos/ejecutados,
     4) gaps cerrados,
     5) gaps residuales,
     6) riesgos inmediatos,
     7) siguiente corte lógico.

Formato de entrega:
- No me devuelvas solo pseudocódigo.
- Materializa cambios reales en el repo.
- Usa ADRs o docs en `docs/rewrite/` o ruta equivalente para decisiones importantes.
- Si una pieza del Java es demasiado grande para copiar la estructura completa en una sola pasada, deja interfaces estables, coverage tests y un plan explícito de continuación sin romper build ni compatibilidad.

Objetivo específico de esta fase:
Cerrar la serie con evidencia operativa real: cluster mínimo validado, pruebas de upgrade/migration, chaos/soak/performance y gates de RC/GA alineados con el gap analysis. Aquí no se escriben features nuevas grandes salvo ajustes para que las pruebas pasen.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- Harnesses de diff tests y dtests
- Scripts de cluster local/CI
- Herramientas de migración/compatibilidad SSTable
- Docs de operación, runbooks y release readiness
- Todos los subsistemas tocados por la serie

Trabajo que debes ejecutar ahora:
1. Montar un cluster real de al menos 3 nodos con la implementación Rust y ejecutar workloads representativos de reads/writes/repair/topology/streaming/auth/tools.
2. Ejecutar diff tests contra Java donde existan harnesses comparables y documentar divergencias residuales con severidad y plan de cierre.
3. Correr chaos/soak tests: reinicios, caída de nodos, network partitions simuladas, peers lentos, compactions concurrentes, repair/streaming simultáneos y auth rotations.
4. Validar historia de migración: lectura/escritura de SSTables compatibles o uso completo de la herramienta de conversión definida, rollback documentado y probado.
5. Medir performance base del cluster (latencias, throughput, uso de memoria/disco/red, tiempos de compaction/repair/streaming) y comparar contra objetivos mínimos.
6. Crear checklist de RC/GA: gaps cerrados, gaps residuales aceptados, riesgos operativos, runbooks, observabilidad, backup/restore, security posture y tooling mínimo.
7. No dejes el proyecto en un estado ambiguo: o cumple los gates acordados o deja una lista explícita y priorizada de bloqueantes remanentes.
8. Generar un informe final legible por ingeniería y operaciones.

Criterios de aceptación de esta fase:
- Existe evidencia reproducible de un cluster Rust de 3 nodos funcionando.
- Hay diff/chaos/soak/perf results documentados.
- La historia de migración/rollback está probada.
- Se emite un juicio claro RC/No-RC con datos.

Entregables mínimos:
- Harnesses y scripts de validación final
- Informe de RC gates
- Resultados de cluster/chaos/soak/perf
- Plan de rollback/migración probado

Reporte final obligatorio:
1. resumen de cambios;
2. archivos creados/modificados;
3. tests añadidos/ejecutados;
4. gaps del análisis que quedaron cerrados;
5. gaps del análisis que siguen abiertos;
6. riesgos inmediatos;
7. siguiente corte lógico.

No me devuelvas teoría abstracta: materializa código, tests, docs, scripts y evidencia.
```


