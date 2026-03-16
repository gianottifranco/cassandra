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
