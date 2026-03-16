# Prompt 00 — Orquestación maestra, freeze de baseline y backlog ejecutable a partir del gap analysis

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
Convertir `INPUT_full_gap_analysis.md` en un plan ejecutable dentro del repo: congelar baseline exacta, validar que los gaps listados siguen existiendo, descomponerlos por crate/PR y dejar un tablero técnico que impida perder coverage durante el resto de la serie.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `INPUT_full_gap_analysis.md`
- `src/java/org/apache/cassandra/**`
- `rust/crates/**`
- `test/**`, dtests, diff tests, gap guards
- CI actual, scripts de build, docs de arquitectura y ADRs existentes

Trabajo que debes ejecutar ahora:
1. Leer completo el gap analysis incluido en el paquete y convertir cada gap P0/P1/P2, cada TODO/stub y cada gap guard ignorado en una matriz ejecutable dentro del repo.
2. Congelar la baseline exacta de Java (commit/tag/branch) y la baseline exacta de Rust que se usará como punto de partida para esta serie.
3. Crear una matriz `gap_id -> paquete Java -> crate Rust -> prompt dueño -> test esperado -> severidad -> estado` y hacerla parte del repositorio.
4. Revalidar automáticamente los top gaps del análisis para detectar si alguno ya fue parcialmente cerrado y ajustar el backlog sin perder trazabilidad.
5. Clasificar gaps en: bloqueo de arranque, bloqueo de beta, bloqueo de GA, deuda técnica no bloqueante y divergencia aceptada temporalmente.
6. Crear dashboards o archivos de seguimiento para: TODOs, stubs, ignored tests, gaps de wire compatibility, gaps on-disk, gaps de tooling y gaps de seguridad.
7. Añadir checks de CI que fallen si reaparecen nuevos stubs/TODOs en subsistemas críticos sin una etiqueta de seguimiento aprobada.
8. Preparar documentos de trabajo para el resto de la serie: orden de ejecución, criterios de aceptación por prompt y política de no degradación.

Criterios de aceptación de esta fase:
- Existe `docs/rewrite/gap_backlog.md` o equivalente con todos los gaps trazados.
- Existe una matriz de ownership y estado por prompt.
- Hay checks de CI o scripts que vigilan TODOs/stubs/gap guards.
- El repo queda listo para ejecutar el Prompt 01 sin ambigüedad de baseline.

Entregables mínimos:
- Matriz completa de gaps
- Freeze de baseline documentado
- Script de auditoría de TODO/stub/ignored-tests
- Docs de ejecución y criterios de cierre

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
