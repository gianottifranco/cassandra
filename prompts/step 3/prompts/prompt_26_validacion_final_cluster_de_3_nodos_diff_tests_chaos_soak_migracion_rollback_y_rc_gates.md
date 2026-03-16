# Prompt 26 — Validación final: cluster de 3 nodos, diff tests, chaos/soak, migración, rollback y RC gates

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
Cerrar la serie con evidencia operativa real: cluster mínimo validado, pruebas de upgrade/migration, chaos/soak/performance y gates de RC/GA alineados con el gap analysis. Aquí no se escriben features nuevas grandes salvo ajustes para que las pruebas pasen.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- Harnesses de diff tests y dtests
- Scripts de cluster local/CI
- Herramientas de migración/compatibilidad SSTable
- Docs de operación, runbooks y release readiness
- Todos los subsistemas tocados por la serie

Trabajo que debes ejecutar ahora:
1. Montar un cluster real de al menos 3 nodos con la implementación Rust y ejecutar workloads representativos de reads/writes/repair/topology/streaming/auth/tools.
2. Ejecutar diff tests contra Java donde existan harnesses comparables y documentar divergencias residuales con severidad y plan de cierre.
3. Correr chaos/soak tests: reinicios, caída de nodos, network partitions simuladas, peers lentos, compactions concurrentes, repair/streaming simultáneos y auth rotations.
4. Validar historia de migración: lectura/escritura de SSTables compatibles o uso completo de la herramienta de conversión definida, rollback documentado y probado.
5. Medir performance base del cluster (latencias, throughput, uso de memoria/disco/red, tiempos de compaction/repair/streaming) y comparar contra objetivos mínimos.
6. Crear checklist de RC/GA: gaps cerrados, gaps residuales aceptados, riesgos operativos, runbooks, observabilidad, backup/restore, security posture y tooling mínimo.
7. No dejes el proyecto en un estado ambiguo: o cumple los gates acordados o deja una lista explícita y priorizada de bloqueantes remanentes.
8. Generar un informe final legible por ingeniería y operaciones.

Criterios de aceptación de esta fase:
- Existe evidencia reproducible de un cluster Rust de 3 nodos funcionando.
- Hay diff/chaos/soak/perf results documentados.
- La historia de migración/rollback está probada.
- Se emite un juicio claro RC/No-RC con datos.

Entregables mínimos:
- Harnesses y scripts de validación final
- Informe de RC gates
- Resultados de cluster/chaos/soak/perf
- Plan de rollback/migración probado

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
