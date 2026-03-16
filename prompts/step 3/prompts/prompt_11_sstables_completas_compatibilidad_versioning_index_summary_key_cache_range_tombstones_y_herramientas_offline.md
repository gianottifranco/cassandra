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
