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
