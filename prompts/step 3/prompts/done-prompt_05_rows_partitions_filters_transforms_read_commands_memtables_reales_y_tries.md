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
