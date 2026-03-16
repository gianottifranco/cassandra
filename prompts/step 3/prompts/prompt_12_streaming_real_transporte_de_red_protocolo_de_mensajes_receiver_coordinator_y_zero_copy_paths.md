# Prompt 12 — Streaming real: transporte de red, protocolo de mensajes, receiver, coordinator y zero-copy paths

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
Transformar el streaming de una state machine puramente estructural a un subsistema operativo de red capaz de mover SSTables/datos para bootstrap, rebuild, replace, repair y range movement.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/streaming/**`
- `src/java/org/apache/cassandra/db/streaming/**`
- `src/java/org/apache/cassandra/net/**` para integración de transporte
- `src/java/org/apache/cassandra/dht/RangeStreamer.java`

Trabajo que debes ejecutar ahora:
1. Implementar el protocolo de mensajes de streaming, serialización, coordinación de sesiones y transporte de red real.
2. Crear `StreamCoordinator`, `StreamResultFuture`, receiver/deserializer y write-to-disk paths robustos.
3. Integrar streaming con mensajería persistente o canales dedicados según la arquitectura elegida, con rate limiting y backpressure medibles.
4. Soportar compresión, checksums, reintentos, aborts y cleanups de sesiones fallidas.
5. Conectar streaming con bootstrap, rebuild, replace, repair sync tasks y herramientas operativas.
6. Añadir zero-copy o sendfile-like paths cuando correspondan y estén soportados por la plataforma objetivo, sin comprometer correctness.
7. Crear pruebas multi-nodo que validen transferencia real de datos y recuperación ante fallos de red o disco.
8. Documentar runbooks básicos y surfaces observables para nodetool/virtual tables/metrics.

Criterios de aceptación de esta fase:
- Streaming mueve datos reales por red y escribe en disco.
- Bootstrap/rebuild/replace pueden usarlo.
- Hay rate limits, retries y checksums reales.
- Pruebas multi-nodo y de fallo pasan.

Entregables mínimos:
- Streaming protocol/network transport
- Receiver/coordinator
- Integración con topology ops
- Tests multi-nodo de transferencia real

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
