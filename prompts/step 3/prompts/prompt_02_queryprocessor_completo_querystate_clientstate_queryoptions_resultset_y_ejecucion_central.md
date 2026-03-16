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
