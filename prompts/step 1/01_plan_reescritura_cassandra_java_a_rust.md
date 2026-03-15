# Plan maestro paso a paso para reescribir Apache Cassandra de Java a Rust

## Resumen ejecutivo

La reescritura completa de Cassandra no debe plantearse como una traducción mecánica del árbol `src/java` a `src/rust`, sino como una **reimplementación guiada por comportamiento**. Cassandra es, al mismo tiempo, un motor LSM, un sistema distribuido con gossip/replicación/repair, una superficie pública CQL/native protocol y un producto operativo con herramientas, seguridad, observabilidad y semántica histórica. Si intentas portarlo “todo a la vez”, el resultado más probable es un sistema que compila pero no es migrable, ni operable, ni verificable.

La estrategia correcta es:

1. congelar una baseline;
2. definir el contrato observable;
3. usar Cassandra Java como oráculo diferencial;
4. portar por vertical slices;
5. llegar primero a una **paridad conservadora**;
6. introducir optimizaciones estructurales después;
7. cerrar mixed-cluster/dual-cluster, migración y hardening como parte del producto, no como postdata.

## Definición de “completo”

En este plan, “completo” significa que la versión Rust cubre, dentro del alcance fijado por la baseline:

- superficie CQL y protocolo nativo;
- esquema, tablas de sistema y prepared statements;
- write path y read path;
- SSTables, commit log, memtables, compaction, snapshots, backups y CDC;
- anillo, gossip, placement, consistency levels, hints, read repair, streaming y repair;
- Paxos/LWT y demás semánticas fuertes que entren en la baseline;
- seguridad, roles, TLS, auditoría, masking y authorizers;
- observabilidad, virtual tables, tooling SSTable y plano administrativo;
- estrategia de migración, rollback y operación real.

## Principios rectores

### 1) Baseline congelada o muerte por deriva
Si partes de `trunk`, congela un commit. Toda novedad upstream posterior entra en una cola separada.

### 2) Paridad observable > similitud estructural
El éxito se mide en queries, frames, SSTables, repairs, fallos y workflows de operador; no en cuántas clases Rust “se parecen” a Java.

### 3) Java permanece como oráculo hasta el final
No borrar el código Java ni asumir “ya estamos” hasta que la fase equivalente pase las pruebas diferenciales.

### 4) Conservador primero, sofisticado después
Primero camino compatible y entendible; luego optimizaciones estructurales. Ejemplo: formato `big` antes de `BTI`, memtable clásica antes de memtable trie, compaction base antes de sofisticación adicional.

### 5) Operabilidad es parte del producto
Sin nodetool-equivalent, métricas, troubleshooting, snapshots, repair y runbooks, no existe una reescritura “terminada”.

## Priorización recomendada de features

### P0 — Núcleo imprescindible
- protocolo nativo y CQL principal;
- esquema y system tables;
- commit log, memtable conservadora, SSTable `big`, compaction inicial;
- ring, gossip, replica placement, coordinator path, CLs, hints, read repair;
- streaming, bootstrap, decommission, repair;
- Paxos/LWT;
- TLS/auth/roles básicos;
- observabilidad mínima y tooling operativo esencial.

### P1 — Paridad moderna 5.x
- `BTI`/Trie SSTables;
- memtables trie;
- Unified Compaction Strategy (UCS);
- SAI;
- tipo vector y features asociadas;
- Dynamic Data Masking;
- authorizers avanzados (CIDR/network) según baseline.

### P2 — Experimental o trunk-only
- features todavía en evolución o con fuerte acoplamiento a trabajo reciente de trunk;
- rutas de consenso adicionales sobre Paxos/LWT;
- cualquier capacidad que aún no sea requisito de la baseline congelada.

## Estructura de ejecución

- **Oleada A**: contrato, harness diferencial, workspace y single-node completo.
- **Oleada B**: plano distribuido, streaming/repair, LWT.
- **Oleada C**: seguridad, observabilidad, tooling, features avanzadas.
- **Oleada D**: mixed-cluster/dual-cluster, migración, hardening y GA.

## Fase 0 — Congelar baseline, alcance y contrato de compatibilidad

**Objetivo**

Evitar que la reescritura persiga una rama móvil. Congelar una rama/commit de referencia, decidir qué significa “completa”, y fijar el contrato público que el runtime Rust deberá respetar.

**Entregables**

- Documento `baseline.md` con rama, commit, fecha, estado de features y clasificación: GA, avanzado 5.x, experimental, trunk-only.
- Matriz de compatibilidad con tres niveles: compatibilidad de cliente (CQL/native protocol), compatibilidad de datos (SSTables/snapshots/CDC/backups) y compatibilidad de clúster (gossip, internode, repair, bootstrap, LWT).
- Lista explícita de no-negociables: semántica CQL, consistency levels, on-disk formats prioritarios, tooling operativo, seguridad, observabilidad y resiliencia.
- Separación formal entre: ruta de paridad, ruta de features avanzadas y ruta de intake de cambios upstream.

**Gate de salida**

- Existe una baseline congelada y todo el equipo trabaja contra ella.
- Se sabe qué features entran en GA v1, qué features quedan bajo feature flags y qué features se posponen sin ambigüedad.
- La definición de done no se expresa como “el código se parece al Java”, sino como “pasa el contrato de compatibilidad”.

**Nota de diseño**

Aquí conviene declarar desde el principio que la reescritura NO será una traducción línea a línea. El objetivo es equivalencia observable, no mimetismo estructural.

## Fase 1 — Inventario profundo del repositorio y descomposición por subsistemas

**Objetivo**

Convertir el código Java actual, la documentación oficial y los tests en un mapa operativo de subsistemas, dependencias y superficies públicas.

**Entregables**

- Inventario por dominio: frontend CQL, native protocol, esquema, storage engine, gossip/ring/snitches, internode messaging, coordinator path, hints/read repair/repair/streaming, Paxos/LWT, índices, seguridad, tooling y observabilidad.
- Mapa de paquetes Java a crates Rust propuestos, con ownership claro por equipo/subsistema.
- Lista de artefactos actuales que no son Java pero forman parte del producto: `cqlsh`, `pylib`, scripts `bin/`, `conf/`, empaquetado, tests distribuidos, fixtures y herramientas SSTable.
- Árbol de dependencias y secuencia de portabilidad: qué se puede portar primero sin bloquear el resto.

**Gate de salida**

- Cada clase relevante de Cassandra cae en un dominio funcional claramente identificado.
- Hay un grafo de dependencias de porting y una priorización P0/P1/P2.
- El equipo sabe qué subsistemas pueden vivir como stubs al principio y cuáles deben ser funcionales desde el día 1.

**Nota de diseño**

El resultado útil no es una lista infinita de clases, sino una partición del producto en unidades que puedan compilar, probarse y liberarse de forma incremental.

## Fase 2 — Montar el oráculo diferencial y el harness de validación

**Objetivo**

Usar Cassandra Java como especificación ejecutable. Cada incremento en Rust debe contrastarse automáticamente contra el comportamiento de referencia.

**Entregables**

- Harness dual que levante un clúster Cassandra Java de referencia y un clúster Rust sujeto a prueba.
- Suites diferenciales para CQL, native protocol, errores, prepared statements, paging, snapshots, SSTables y operaciones de clúster.
- Integración con los tipos de tests ya presentes en Cassandra: unit, integration y dtests; además, incorporación de fuzz/model testing y replay de workloads.
- Fixtures dorados generados desde Java: SSTables, frames del protocolo, resultados de queries, topologías y escenarios de failure injection.

**Gate de salida**

- Cada feature nueva en Rust puede evaluarse contra el oráculo Java automáticamente.
- Existe un pipeline que marca drift semántico, no solo fallos de compilación.
- El equipo puede reproducir errores distribuidos con escenarios deterministas.

**Nota de diseño**

Sin un harness diferencial, el proyecto se convierte en una reinterpretación de Cassandra. Con él, se convierte en una reimplementación verificable.

## Fase 3 — Fundación de ingeniería en Rust: workspace, ADRs, CI y contratos internos

**Objetivo**

Crear la base del monorepo Rust que permita compilar rápido, aislar dominios y sostener años de evolución sin acoplamiento accidental.

**Entregables**

- Workspace Cargo con crates separados por dominio: common, config, types, schema, native-protocol, cql, storage, cluster-metadata, messaging, coordinator, repair, streaming, security, admin y binaries.
- ADRs (Architecture Decision Records) para runtime, modelo de ownership, errores, trazas, feature flags, compatibilidad on-disk y políticas de unsafe.
- Tooling base: fmt, clippy, deny, Miri donde aplique, sanitizers, cobertura, documentación interna y plantillas de PR.
- CI con targets de build, test, fuzz y benchmarks, más pipelines que comparen contra Cassandra Java.

**Gate de salida**

- El workspace compila aunque muchos crates aún tengan stubs.
- Las fronteras entre dominios están explícitas y documentadas.
- El equipo puede iterar por vertical slices sin romper medio repositorio.

**Nota de diseño**

La decisión crítica aquí es cómo aislar storage, coordinator y ops plane para que el proyecto no se convierta en un único crate monolítico.

## Fase 4 — Primitivas compartidas: tipos, serialización, clocks, partitioners y claves

**Objetivo**

Portar primero la semántica básica del sistema: valores CQL, comparadores, tokens, clocks, TTLs, tombstones, particionado y formatos binarios base.

**Entregables**

- Tipos canónicos para particiones, clustering keys, cells, rows, range tombstones, timestamps y TTL.
- Codecs binarios para los tipos CQL soportados por el protocolo nativo y los formatos internos necesarios para storage y mensajería.
- Implementaciones de partitioners y comparadores, con tests cruzados contra fixtures Java.
- Catálogo inicial de IDs y digests (table IDs, schema digests, tokens, digests de mensajes, checksums).

**Gate de salida**

- Los tipos básicos tienen equivalencia semántica con Java y golden tests cruzados.
- La serialización binaria es estable, reproducible y compatible con los fixtures.
- No quedan `String`/`Vec<u8>` ad hoc en interfaces críticas donde debería existir un tipo de dominio.

**Nota de diseño**

Todo lo que se apoye en esto —protocolo, esquema, SSTables, repair— se abarata radicalmente si aquí se evita deuda conceptual.

## Fase 5 — Configuración, arranque y catálogo de esquema

**Objetivo**

Recrear la forma en que Cassandra arranca, valida entorno, carga `cassandra.yaml`, materializa el esquema y mantiene su metadato estable.

**Entregables**

- Loader y validador de configuración con defaults, unidades, compatibilidad de nombres y validaciones de arranque.
- Modelo de keyspaces, tablas, tipos, funciones, vistas y propiedades de tabla, con persistencia en tablas de sistema equivalentes.
- Mecanismo de migraciones de esquema, schema agreement y snapshots consistentes del catálogo.
- Startup checks de discos, puertos, directorios, formatos soportados y coherencia de configuración.

**Gate de salida**

- El nodo Rust puede arrancar, validar entorno, cargar configuración y exponer catálogo de sistema sin ejecutar aún el storage distribuido completo.
- DDL básica produce el mismo esquema observable que Java.
- El esquema puede persistirse, recuperarse y compararse de manera estable.

**Nota de diseño**

Este paso desbloquea que el protocolo CQL y la ejecución local trabajen contra un catálogo real en vez de estructuras efímeras.

## Fase 6 — Native protocol v5, transporte cliente-servidor y frontend CQL

**Objetivo**

Implementar la superficie pública más importante: el protocolo binario CQL, el parser/AST/planner y la compatibilidad con drivers y `cqlsh`.

**Entregables**

- Server del protocolo nativo con handshake, auth negotiation, compresión, prepared statements, paging y manejo correcto de errores.
- Parser y AST de CQL, más planner/normalizer que convierta queries en planes ejecutables contra el catálogo.
- Mapeo completo de tipos, códigos de error, flags, streams, eventos y versiones del protocolo.
- Tests de compatibilidad con `cqlsh` y al menos un driver moderno mediante suites automatizadas.

**Gate de salida**

- Un cliente real puede conectarse, autenticar, preparar, ejecutar y paginar contra el nodo Rust.
- Los frames y errores coinciden con los dorados del oráculo Java.
- La capa de transporte queda suficientemente desacoplada del motor para permitir evolución futura.

**Nota de diseño**

Aquí aún no hace falta que todo el motor local sea perfecto, pero sí que la superficie de protocolo sea estable y testeable.

## Fase 7 — Write path local: commit log, memtables, flush y durabilidad

**Objetivo**

Construir el write path local siguiendo la semántica LSM de Cassandra: commit log primero, memtable después, flush a SSTables inmutables.

**Entregables**

- Commit log/WAL con segmentación, checksums, políticas de fsync y rotación.
- Memtable inicial conservadora para paridad (skiplist o equivalente), con interfaz que permita introducir una implementación trie después.
- Flushing, backpressure y coordinación con límites de memoria y directorios de datos.
- Integración básica con CDC, snapshots e incremental backups donde dependa del commit log/flush.

**Gate de salida**

- Las escrituras sobreviven reinicios y replays del commit log.
- La presión de memoria provoca flush de forma correcta y reproducible.
- Los golden tests prueban que timestamps, TTLs, tombstones y ordering siguen la semántica esperada.

**Nota de diseño**

Para reducir riesgo, conviene implementar primero el equivalente funcional del camino clásico antes de perseguir optimizaciones estilo TrieMemtable.

## Fase 8 — Read path local y formatos SSTable

**Objetivo**

Permitir lecturas locales correctas y compatibles, incluyendo SSTables, Bloom filters, índices, compresión, cachés y merge de múltiples SSTables.

**Entregables**

- Lectores/escritores SSTable para el formato prioritario de compatibilidad (recomendado: `big` primero).
- Índices primarios, summaries, Bloom filters, checksums, compresión por chunks y recorrido de particiones/rangos.
- Merge de memtable + múltiples SSTables, con resolución correcta por timestamp/tombstone/TTL.
- Cachés de lectura y utilidades de inspección/exportación de SSTables.

**Gate de salida**

- El motor local pasa suites diferenciales de lectura puntual, slice reads, range scans, tombstones y expiraciones.
- El nodo puede leer fixtures generados por Java para el formato soportado.
- Se puede crear, abrir, verificar y compactar SSTables de forma estable.

**Nota de diseño**

La ruta recomendada es: compatibilidad fuerte con `big` primero; BTI y estructuras trie después, cuando la semántica de lectura ya sea sólida.

## Fase 9 — Ejecución local completa: DDL/DML/SELECT, batches, paging y semántica de consulta

**Objetivo**

Cerrar el loop single-node completo: del CQL al storage, incluyendo prepared statements, batches, paging, filtros y semántica observable.

**Entregables**

- Ejecución de CREATE/ALTER/DROP, INSERT/UPDATE/DELETE/SELECT, batch log local, funciones básicas y condiciones de error correctas.
- Prepared statements con invalidación y cache coherente respecto al catálogo.
- Paging, límites, ordering por clustering, timestamps de usuario y reglas de validación/guardrails relevantes.
- Manejo correcto de counters locales cuando aplique, o stubs explícitos si quedan atados a la fase distribuida.

**Gate de salida**

- El nodo single-node es utilizable por clientes reales para workloads no distribuidos.
- Los resultados, mensajes de error y side effects coinciden con el oráculo Java dentro del alcance implementado.
- La capa de ejecución local queda lista para ser invocada por el coordinator distribuido.

**Nota de diseño**

Hasta aquí el objetivo es tener un Cassandra single-node funcional en Rust, ya útil para desarrollar el resto.

## Fase 10 — Metadato de clúster: ring, snitches, replica placement, gossip y failure detection

**Objetivo**

Reconstruir el plano de control distribuido que hace posible saber quién posee qué datos y qué nodos están vivos.

**Entregables**

- Token ring y estrategias de replicación (SimpleStrategy y NTS como mínimo inicial).
- Snitches, endpoint selection, racks/DCs y replica placement estable.
- Gossip, versionado de estado, seeds, heartbeats y failure detection compatibles a nivel de comportamiento.
- Snapshots inmutables de cluster metadata para lecturas lock-light y coordinación segura.

**Gate de salida**

- Los nodos Rust pueden descubrirse y converger en un estado compartido del clúster.
- Replica placement y rutas de lectura/escritura se calculan de manera estable.
- Escenarios de caída, reincorporación y cambios de estado tienen tests automatizados.

**Nota de diseño**

Sin snapshots consistentes de metadato de clúster, todo el coordinator path posterior se vuelve extremadamente frágil.

## Fase 11 — Mensajería internodo y coordinator path distribuido

**Objetivo**

Implementar el núcleo operacional distribuido: mensajes internodo, replicas, consistency levels, writes, reads, digest reads, hinted handoff y read repair.

**Entregables**

- Servicio de mensajería internodo con codecs, verbs, backpressure y separación clara respecto al canal de streaming.
- Coordinator para escrituras y lecturas con CLs, replica selection, speculative execution cuando corresponda y manejo de timeouts/failures.
- Digest reads, compare/repair de réplicas, hinted handoff y batch log distribuido.
- Métricas, trazas y herramientas para depurar saturation y routing.

**Gate de salida**

- Un clúster Rust de varios nodos puede servir lecturas y escrituras replicadas con semántica correcta.
- Los CLs principales pasan escenarios diferenciales y de fallo.
- La separación entre coordinator, messaging y storage sigue siendo clara y testeable.

**Nota de diseño**

Este paso es donde más se dispara la complejidad accidental si se mezcla lógica de réplica, red y storage en el mismo módulo.

## Fase 12 — Streaming, bootstrap, decommission, replace, rebuild y repair

**Objetivo**

Completar las operaciones de movimiento y reconciliación de datos que hacen a Cassandra operable en producción.

**Entregables**

- Canal de streaming separado de la mensajería general, con transferencias de SSTables/fragmentos y control de flujo.
- Bootstrap, decommission, replace, removenode, rebuild y workflows asociados de estado.
- Repair full/incremental, árboles de Merkle, anti-entropy y coordinación segura con compaction y snapshots.
- Pruebas de topología cambiante, nodos lentos, caídas en mitad de streaming y recuperación.

**Gate de salida**

- El clúster Rust soporta cambios de topología sin corrupción ni drift sostenido de datos.
- Repair y streaming tienen suites largas, reproducibles y con fallo inyectado.
- El plano operativo ya es suficiente para administración real, aunque aún falten features avanzadas.

**Nota de diseño**

El streaming no debe reusar ingenuamente el mismo contrato que mensajería internodo; Cassandra ya lo separa por razones operativas.

## Fase 13 — Transacciones ligeras y semántica fuerte: Paxos/LWT

**Objetivo**

Portar la parte más sensible de consistencia fuerte: Paxos, CAS, serial consistency y todos los edge cases temporales y de contención.

**Entregables**

- Implementación de Paxos/LWT con las tablas de sistema, ballots, prepare/propose/commit y reads seriales.
- Pruebas de linealizabilidad, contención, timeouts, reinicios y recuperación tras fallos parciales.
- Aislamiento explícito entre el camino eventual-consistent y el camino serial/consensus.
- Interfaces para futuras extensiones de consenso trunk-only si forman parte del baseline elegido.

**Gate de salida**

- Los CAS/LWT funcionan correctamente bajo contención y fallos parciales.
- La implementación pasa tests de linealizabilidad y equivalencia frente a Java.
- Las rutas seriales no contaminan innecesariamente el write path común.

**Nota de diseño**

Si la baseline incluye trabajo de consenso adicional en trunk, conviene tratarlo como épica aparte encima de un Paxos estable, no como prerrequisito para empezar.

## Fase 14 — Features avanzadas de motor y consulta: índices, SAI, vectores, vistas, triggers, UDF/UDA

**Objetivo**

Completar la cola larga funcional de Cassandra moderna sin comprometer la solidez del núcleo ya portado.

**Entregables**

- Índices secundarios legacy, materialized views y triggers con su semántica operacional.
- SAI integrado al storage engine y al read path, con pruebas de build/streaming/consulta.
- Tipo vector, funciones de similitud y vector search si forman parte del alcance target.
- UDF/UDA, o una decisión explícita y segura de encapsulación/restricción si la baseline así lo exige.

**Gate de salida**

- Las features avanzadas se habilitan detrás de una matriz clara de soporte.
- SAI y vector search no degradan silenciosamente correctness ni operabilidad.
- La plataforma ya cubre la mayor parte del surface area funcional de Cassandra 5.x.

**Nota de diseño**

Estas features deben entrar cuando storage, streaming y coordinator estén maduros; antes solo añaden ruido y retrabajo.

## Fase 15 — Seguridad y gobierno: TLS, authn/authz, roles, authorizers, masking y auditoría

**Objetivo**

Cerrar la brecha de seguridad operativa para que la reescritura sea desplegable en entornos reales.

**Entregables**

- TLS cliente-nodo e internodo, manejo de certificados, recarga y políticas seguras.
- Autenticación, autorización, roles, permisos y authorizers de red/CIDR según alcance.
- Dynamic Data Masking y reglas de presentación segura de datos.
- Audit logging y full query logging con formatos y utilidades comparables.

**Gate de salida**

- El nodo Rust puede desplegarse con endurecimiento equivalente al Java actual.
- Las tablas de sistema de seguridad y los flujos de login/permiso son consistentes.
- Existen pruebas explícitas de seguridad, no solo happy-path.

**Nota de diseño**

La seguridad no puede quedar como parche final de último momento; debe entrar cuando el plano operativo ya existe pero antes del endurecimiento final.

## Fase 16 — Observabilidad, tooling y plano administrativo

**Objetivo**

Recrear la experiencia operativa: métricas, virtual tables, herramientas SSTable, admin commands y nodetool-equivalent.

**Entregables**

- Métricas internas por subsistema, tracing distribuido, logs estructurados y paneles base.
- Virtual tables y/o superficie administrativa equivalente para inspeccionar estado interno.
- CLI administrativo compatible por intención con `nodetool`, más herramientas de SSTables, snapshots y export/import.
- Documentación de operación, troubleshooting, backup/restore y runbooks de incidentes.

**Gate de salida**

- Los operadores pueden administrar el clúster Rust sin perder visibilidad crítica.
- Las operaciones frecuentes tienen equivalentes funcionales a las herramientas actuales.
- Existe documentación suficiente para operar el sistema sin depender del equipo de desarrollo.

**Nota de diseño**

Aquí vale la pena priorizar compatibilidad operacional por intención. JMX exacto puede ser una decisión aparte; el objetivo real es paridad de capacidades.

## Fase 17 — Compatibilidad de clúster mixto, migración y rollout seguro

**Objetivo**

Transformar la reescritura en algo migrable: coexistencia controlada, dual-run, shadow traffic y procedimientos de transición seguros.

**Entregables**

- Matriz explícita de compatibilidad Java↔Rust por feature, versión, formato on-disk y protocolo internodo.
- Herramientas de shadow reads/writes, replay de FQL, verificación de divergencias y cassandra-diff equivalente o integrado.
- Procedimiento de migración y rollback: canary, dual-cluster, mixed-cluster (si se soporta) y cutover.
- Conjuntos de pruebas de upgrade/downgrade, restore y desastre.

**Gate de salida**

- Existe una ruta segura y comprobada para pasar de Java a Rust.
- Los desvíos de comportamiento se detectan antes de llegar a producción.
- Hay rollback claro y probado.

**Nota de diseño**

Muchos proyectos técnicamente correctos fracasan aquí: no por el motor, sino por no ofrecer una vía migrable y auditable.

## Fase 18 — Hardening final, performance parity, soak tests y criterio de release

**Objetivo**

Cerrar el proyecto con evidencia de estabilidad, rendimiento, recuperabilidad y operabilidad al nivel exigido por un sistema de misión crítica.

**Entregables**

- Benchmarks y cargas largas bajo compaction, repair, bootstrap, failover, LWT y workloads mixtos.
- Presupuestos de latencia/throughput, consumo de CPU/memoria/disk/network y comportamiento bajo degradación.
- Pruebas de recuperación: cortes de energía, corrupción detectada, replay WAL, restore snapshot, rebuild y pérdida de nodos.
- Checklist de release GA con gates de correctness, performance, observabilidad, seguridad y documentación.

**Gate de salida**

- La versión Rust cumple el contrato de compatibilidad y los budgets operativos definidos.
- Los soak tests y chaos tests dejan de ser exploratorios y se vuelven gates formales de release.
- La organización puede sostener mantenimiento evolutivo sin depender de heroicidades.

**Nota de diseño**

El proyecto termina cuando la plataforma es operable y migrable, no cuando el repositorio compila.


## Riesgos principales y mitigaciones

### Riesgo 1 — Deriva semántica
**Problema**: la versión Rust “parece” Cassandra pero no responde exactamente igual en edge cases.  
**Mitigación**: harness diferencial, fixtures dorados, tests de error codes, comparadores de payloads y linealizabilidad donde corresponda.

### Riesgo 2 — Reescritura monolítica
**Problema**: un solo crate enorme, acoplado, difícil de perfilar y casi imposible de mantener.  
**Mitigación**: workspace multi-crate, ADRs, ownership claro por subsistema y vertical slices con gates.

### Riesgo 3 — Mezclar correctness y optimización demasiado pronto
**Problema**: introducir BTI/trie/UCS/SAI antes de tener el camino conservador estable.  
**Mitigación**: ruta en dos etapas: paridad conservadora primero, optimización estructural después.

### Riesgo 4 — No cerrar la operación real
**Problema**: motor correcto pero sin tooling, métricas, repair operable o migración segura.  
**Mitigación**: tratar observabilidad, admin plane y migration/rollback como fases de producto, no “nice to have”.

### Riesgo 5 — Baseline de `trunk` demasiado ambiciosa
**Problema**: absorber de golpe features en evolución.  
**Mitigación**: freeze estricto + backlog separado de intake upstream + feature flags.

## Criterio de “done” global

La reescritura se considera realmente terminada cuando se cumplen a la vez estas condiciones:

1. la versión Rust pasa los gates diferenciales relevantes frente a la baseline Java;
2. existe una ruta de migración y rollback validada;
3. el clúster Rust es operable con tooling, métricas y runbooks completos;
4. los presupuestos de rendimiento y estabilidad están dentro de los límites definidos;
5. el mantenimiento futuro puede continuar sin depender del código Java como soporte permanente.

## Recomendación final de secuencia práctica

Si solo pudiera darte una secuencia ejecutiva en una línea, sería esta:

**freeze -> diff harness -> workspace -> single-node completo -> distributed core -> streaming/repair -> LWT -> seguridad/ops -> features modernas -> mixed/dual migration -> hardening/GA**.