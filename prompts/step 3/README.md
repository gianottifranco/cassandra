# Paquete de prompts Codex para cerrar el gap analysis de Cassandra Java vs Rust

Este paquete está construido **directamente** a partir del archivo `INPUT_full_gap_analysis.md` incluido dentro del zip.  
No es una lista genérica de prompts: es una serie ordenada para cerrar los gaps concretos detectados en la comparación Java↔Rust.

## Qué contiene

- `INPUT_full_gap_analysis.md`  
  Copia del gap analysis que controla la serie.

- `00_orden_de_ejecucion.md`  
  Orden recomendado y modo de uso.

- `01_prompts_codex_completos.md`  
  Todos los prompts juntos en un solo archivo.

- `02_top30_gaps_a_prompts.md`  
  Trazabilidad directa entre el Top 30 del análisis y los prompts de esta serie.

- `03_subsistemas_a_prompts.md`  
  Cobertura por subsistema grande.

- `04_checklist_de_cierre.md`  
  Checklist final para no perder gaps por el camino.

- `05_fuentes_oficiales_y_modulos.md`  
  Documentación oficial y módulos Java/Rust que sirven como oráculo durante la serie.

- `06_todos_stubs_gapguards_a_prompts.md`  
  Trazabilidad rápida de TODOs, stubs y gap guards del análisis.

- `prompts/`  
  Un archivo por prompt, listo para copiar/pegar en Codex.

## Filosofía

La serie está organizada para:
1. cerrar primero los **P0** que bloquean cualquier uso;
2. luego los **P1** que bloquean beta;
3. después los **P2** que bloquean GA;
4. y finalmente convertir el conjunto en una validación de cluster/RC con evidencia.

## Orden recomendado

1. Prompt 00 — Orquestación y freeze de baseline
2. Prompt 01 — Servidor CQL real
3. Prompt 02 — QueryProcessor y ejecución central
4. Prompt 03 — Semántica CQL
5. Prompt 04 — Marshal/type system
6. Prompt 05 — Rows/partitions/filters/transforms/read commands
7. Prompt 06 — StorageService/StorageProxy/hints/batchlog
8. Prompt 07 — Gossip y schema exchange
9. Prompt 08 — Mensajería internodo persistente
10. Prompt 09 — CompactionManager/lifecycle/journal
11. Prompt 10 — IO util/compression/mmap
12. Prompt 11 — SSTable compat/versioning/offline tools
13. Prompt 12 — Streaming real
14. Prompt 13 — Repair completo
15. Prompt 14 — Auth persistente y auth avanzada
16. Prompt 15 — Security/config/guardrails
17. Prompt 16 — Índices core y 2i
18. Prompt 17 — SAI/SASI/vector
19. Prompt 18 — Schema/views/triggers/UDF/UDA
20. Prompt 19 — Virtual tables/tracing/audit/diag
21. Prompt 20 — Cache subsystem
22. Prompt 21 — TCM/CMS
23. Prompt 22 — Paxos/LWT/Accord/consensus
24. Prompt 23 — DHT/locator/snitches/allocator
25. Prompt 24 — Tooling paridad
26. Prompt 25 — Metrics/exceptions/utils/stub cleanup
27. Prompt 26 — Cluster validation/chaos/RC gates

## Reglas prácticas para usarlo con Codex

- Un prompt por rama/PR siempre que sea posible.
- No mezclar dos workstreams P0 en el mismo PR salvo que estén fuertemente acoplados.
- Exigir siempre:
  - código,
  - tests,
  - docs,
  - scripts/harnesses,
  - y reporte final.
- No aceptar “done” si no hay evidencia (diff tests, golden tests, multi-node tests, CLI tests, etc.).
- Si un prompt descubre nuevos gaps no listados explícitamente en el analysis, deben añadirse a la matriz de backlog del Prompt 00.

## Resultado esperado

Si la serie se ejecuta con disciplina, deberías pasar de un estado “beta condicionada / no RC” a un estado donde:
- el servidor es conectable,
- las queries se ejecutan de verdad,
- el cluster converge,
- repair/streaming/topology funcionan,
- hay tooling razonable,
- y existe una respuesta clara sobre RC/GA basada en evidencia.