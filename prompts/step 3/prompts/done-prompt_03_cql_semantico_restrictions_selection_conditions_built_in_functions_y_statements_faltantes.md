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
