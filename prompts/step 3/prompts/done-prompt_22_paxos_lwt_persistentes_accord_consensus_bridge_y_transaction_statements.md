# Prompt 22 — Paxos/LWT persistentes, Accord/consensus bridge y transaction statements

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
Cerrar los gaps de consistencia fuerte y trunk-only: Paxos/LWT persistentes, routers/bridges de consenso y transaction statements si la baseline moderna incorpora Accord o rutas de migración de consenso.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/service/paxos/**`
- `src/java/org/apache/cassandra/service/accord/**`
- `src/java/org/apache/cassandra/service/consensus/**`
- `src/java/org/apache/cassandra/cql3/statements/Transaction*` y `Condition*` si existen
- `src/java/org/apache/cassandra/service/StorageProxy.java`

Trabajo que debes ejecutar ahora:
1. Implementar persistencia/recuperación de Paxos/LWT state, ballots, prepare/propose/commit y timeouts/result mapping completos.
2. Integrar IF conditions del prompt 03 con el runtime LWT real del coordinador y storage.
3. Completar `service/paxos` gaps: repairs, contention strategies, cleanup tras restart y métricas.
4. Si la baseline contiene `service.accord`/`service.consensus`, implementar o aislar explícitamente la ruta de Accord, el router de requests y la migración de consenso con feature gates y tests.
5. Añadir `TransactionStatement`/`ConditionStatement` y surfaces CQL necesarias si existen en la baseline congelada.
6. Conectar counters y operaciones que interaccionen con LWT/consenso si la baseline así lo hace.
7. Escribir pruebas multi-nodo de LWT, retries, ambiguous results, restart recovery y compatibilidad del router de consenso.
8. Documentar claramente cuándo se usa Paxos clásico, cuándo se enruta a Accord y cómo se migra/observa.

Criterios de aceptación de esta fase:
- LWT/Paxos sobreviven reinicios y funcionan multi-nodo.
- Los bridges de consenso están implementados o gated explícitamente con tests.
- Transaction statements de la baseline existen.
- Hay pruebas multi-nodo y documentación de modo/transición.

Entregables mínimos:
- Paxos/LWT persistence
- Accord/consensus bridge
- Transaction statements
- Tests multi-nodo de consistencia fuerte

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
