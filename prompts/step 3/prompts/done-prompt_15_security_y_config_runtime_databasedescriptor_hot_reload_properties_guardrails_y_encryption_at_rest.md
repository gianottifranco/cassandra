# Prompt 15 — Security y config runtime: DatabaseDescriptor, hot reload, properties, guardrails y encryption-at-rest

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
Cerrar los gaps de configuración/runtime y de seguridad de almacenamiento: `DatabaseDescriptor`, propiedades relevantes, hot reload, guardrails, crypto provider y encryption-at-rest si la baseline lo soporta.

Antes de tocar código:
- Lee `INPUT_full_gap_analysis.md` y cita en tu propia documentación interna qué gaps de ese archivo estás cerrando en esta fase.
- Revisa el trabajo de los prompts anteriores de esta serie y evita reabrir decisiones ya fijadas.
- Mantén Java como oráculo y no rompas el harness diferencial.

Paquetes/directorios/artefactos que debes inspeccionar primero:
- `src/java/org/apache/cassandra/config/**`
- `src/java/org/apache/cassandra/db/guardrails/**`
- `src/java/org/apache/cassandra/security/**`
- `src/java/org/apache/cassandra/service/CassandraDaemon.java` para boot/runtime integration

Trabajo que debes ejecutar ahora:
1. Implementar un `DatabaseDescriptor` o runtime descriptor equivalente que unifique config efectiva, overrides, derived defaults y acceso thread-safe a parámetros globales.
2. Añadir soporte para `CassandraRelevantProperties`, environment overrides y documentación clara del precedence model.
3. Implementar hot reload de parámetros seguros, incluyendo TLS cuando ya exista soporte, y rechazar o reiniciar de forma controlada lo que no sea hot-reloadable.
4. Construir el framework de guardrails: thresholds, enable flags, password validation u otros guardrails presentes en la baseline, con surfaces de config y tests.
5. Implementar `RepairConfig`, `StorageAttachedIndexOptions`, `TransparentDataEncryptionOptions` y otras config classes faltantes relevantes a la baseline.
6. Introducir crypto provider abstraction y encryption-at-rest para commitlog/SSTables u objetos que la baseline soporte, con key management claramente documentado.
7. Conectar config/guardrails con QueryProcessor, storage, streaming, repair, tools y virtual tables de settings.
8. Escribir tests de config loading, override precedence, hot reload, guardrails enforcement y encryption smoke tests.

Criterios de aceptación de esta fase:
- Existe una capa runtime de config/descriptor seria.
- Guardrails funcionan y pueden activarse/configurarse.
- Hay hot reload donde es seguro y explícitamente documentado.
- Security at rest/crypto provider quedan implementados o formalmente gated con tests.

Entregables mínimos:
- DatabaseDescriptor equivalente
- Guardrails framework
- Hot reload/config precedence
- Crypto/encryption-at-rest integration

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
