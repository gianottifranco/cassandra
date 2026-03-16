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
