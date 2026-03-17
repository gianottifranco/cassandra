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
