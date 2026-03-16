# Prompt 19 — Virtual tables, system keyspaces, tracing persistente, audit filters, notifications y diagnostics

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
Cerrar toda la superficie operativa que el gap analysis marca ausente o parcial: virtual tables, system keyspaces, tracing persistido, filtros de auditoría, notifications y diagnostics.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/db/virtual/**`
- `src/java/org/apache/cassandra/tracing/**`
- `src/java/org/apache/cassandra/audit/**` o equivalentes
- `src/java/org/apache/cassandra/notifications/**`
- `src/java/org/apache/cassandra/diag/**` o diagnostic event service equivalente
- `src/java/org/apache/cassandra/db/SystemKeyspace*` y `system_traces`

Trabajo que debes ejecutar ahora:
1. Construir la infraestructura de virtual tables/keyspaces y materializar las tablas operativas más importantes para settings, clients, internode messaging, compactions, repairs, caches y tracing.
2. Implementar `SystemKeyspace` y system tables mínimas necesarias para runtime, auth, tracing, schema, hints y operación.
3. Persistir tracing en `system_traces` o equivalente con TTL/expiry y manager global de tracing.
4. Completar filtros de auditoría, `AuditLogContext` por request y, si la baseline lo requiere, `BinAuditLogger` o un equivalente compatible/documentado.
5. Implementar notifications/diagnostics para eventos de SSTables, memtables, truncation, compaction y runtime operativo.
6. Conectar virtual tables y diagnostics con metrics, nodetool/tools y surfaces HTTP/JMX equivalentes si existen.
7. Escribir tests de system tables, tracing persistence, audit filters y virtual table queries a través de CQL.
8. Documentar claramente qué virtual tables quedan incluidas según baseline y cómo se actualizan.

Criterios de aceptación de esta fase:
- Existen virtual tables y system keyspaces operativas.
- Tracing se persiste y expira correctamente.
- Audit filters funcionan y existe contexto por request.
- Notifications/diagnostics son observables.

Entregables mínimos:
- Virtual tables infra + implementations
- System keyspaces/tables
- Tracing persistence
- Audit/diag/notification integration

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
