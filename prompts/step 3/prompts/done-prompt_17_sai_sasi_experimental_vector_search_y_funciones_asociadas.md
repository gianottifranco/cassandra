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
