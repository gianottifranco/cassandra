# Prompt 07 — Gossip real, shadow round, schema exchange y descubrimiento de cluster

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
Cerrar el P0 de gossip: hoy existen estructuras, pero no el loop real de SYN/ACK/ACK2 ni el descubrimiento inicial. Debes construir un gossip operativo con subscribers, quarantine/shutdown y propagación de schema/topology mínima.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/gms/Gossiper.java`
- `src/java/org/apache/cassandra/gms/**`
- `src/java/org/apache/cassandra/schema/SchemaPullVerbHandler*`, `SchemaPushVerbHandler*`, `VersionVerbHandler*`
- `src/java/org/apache/cassandra/service/StorageService.java` (gossip integration)
- `src/java/org/apache/cassandra/tcm/**` para coexistencia con metadata moderna

Trabajo que debes ejecutar ahora:
1. Implementar el loop periódico de gossip con mensajes SYN/ACK/ACK2, heartbeats, selección de peers y propagation budgets.
2. Añadir verb handlers reales, estado de endpoint, subscribers, notifications y transiciones de estado (alive, dead, removed, quarantined, shutdown).
3. Soportar shadow round y descubrimiento inicial para bootstrap y arranque en clusters parcialmente conocidos.
4. Integrar failure detector, seed providers, quarantine logic y clean shutdown gossip.
5. Conectar schema/version exchange y eventos de topology/status para que el cluster pueda converger y notificar cambios.
6. Definir la convivencia entre gossip clásico y los módulos más nuevos de metadata/TCM si la baseline los contiene, evitando inconsistencia de ownership.
7. Agregar pruebas multi-nodo con pérdida/retraso de mensajes, node flaps, seed failures y schema agreement básico.
8. Exponer métricas y virtual tables/herramientas mínimas necesarias para observar el estado de gossip en prompts posteriores.

Criterios de aceptación de esta fase:
- Los nodos se descubren, intercambian estado y convergen.
- Hay loop real de gossip y verb handlers reales.
- Schema/topology notifications básicas existen.
- Pruebas multi-nodo muestran convergencia.

Entregables mínimos:
- Gossip loop operativo
- Subscribers y handlers
- Schema/version exchange base
- Tests de convergencia

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
