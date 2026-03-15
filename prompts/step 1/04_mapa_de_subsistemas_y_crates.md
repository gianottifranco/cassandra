# Mapa de subsistemas actuales y propuesta de crates Rust

Esta matriz sirve para dos cosas:

1. evitar una traducción mecánica de paquetes Java a módulos Rust;
2. darle a Codex y al equipo una frontera de ownership clara.

## Principio de diseño

No intentes copiar el package layout de Java tal cual. Usa el layout actual como **mapa del dominio**, no como destino obligatorio. En Rust conviene separar mejor:

- tipos/base común,
- storage,
- coordinator,
- cluster metadata,
- red internodo,
- seguridad,
- observabilidad,
- tooling.

## Tabla de correspondencia sugerida

| Dominio | Superficie Java/Repo actual | Crate(s) Rust propuestos | Nota |
|---|---|---|---|
| Configuración y arranque | `conf/cassandra.yaml`, `Config.java`, `StartupChecks.java` | `cassandra-config`, `cassandra-bootstrap` | Parser de config, validaciones, defaults, startup checks y carga de options. |
| Frontend CQL y protocolo nativo | `transport/*`, `cql3/*`, `transport/messages/*` | `cassandra-native`, `cassandra-cql` | Frames, handshake, prepared statements, parser/AST/planner y errores de protocolo. |
| Esquema y tablas de sistema | `schema/*`, `SystemKeyspace.java` | `cassandra-schema`, `cassandra-system-tables` | Catálogo, migrations, schema agreement, snapshots inmutables. |
| Storage engine | `db/*`, `commitlog/*`, `db/memtable/*`, `io/sstable/*` | `cassandra-storage`, `cassandra-wal`, `cassandra-sstable` | Commit log, memtables, flush, SSTables, compaction y caches. |
| Mensajería internodo | `net/*`, `MessagingService.java` | `cassandra-internode` | Verbs, codecs, backpressure, request/response, métricas de red. |
| Gossip y membership | `gms/*`, `locator/*`, `tcm/*` | `cassandra-cluster-metadata`, `cassandra-gossip` | Ring, tokens, snitches, DC/rack, state convergence y snapshots de metadata. |
| Coordinator y CLs | `service/StorageProxy.java`, `service/reads/*` | `cassandra-coordinator` | Replica selection, CLs, digest reads, timeouts, batch log, hints y read repair. |
| Streaming y repair | `streaming/*`, `repair/*` | `cassandra-streaming`, `cassandra-repair` | Bootstrap, decommission, rebuild, Merkle trees, anti-entropy y transferencias. |
| Consistencia fuerte | `service/paxos/*`, `service/accord/*`, `service/consensus/*` | `cassandra-lwt`, `cassandra-consensus` | Paxos/LWT como P0 funcional; Accord/trunk consensus como épica separada si entra en baseline. |
| Seguridad | `auth/*`, `security/*` | `cassandra-auth`, `cassandra-security` | TLS, authn/authz, roles, authorizers, masking y auditoría. |
| Observabilidad y ops | `metrics/*`, `tracing/*`, `tools/*`, virtual tables | `cassandra-observability`, `cassandra-admin`, `cassandra-tools` | Métricas, tracing, virtual tables, CLI administrativa, herramientas SSTable. |
| Artefactos no Java a conservar | `bin/`, `pylib/`, `cqlsh.py`, scripts de empaquetado | Compatibilidad externa / wrappers | No reescribir por deporte. Mantenerlos si siguen sirviendo como superficie operativa. |

## Reglas prácticas para este mapa

### Regla 1 — `schema` no depende de `storage`
El catálogo debe poder vivir como snapshot estable sin arrastrar detalles del motor físico.

### Regla 2 — `coordinator` no hace I/O bruto directo
Debe orquestar; el trabajo físico queda en storage, messaging, repair o streaming.

### Regla 3 — `cluster metadata` y `gossip` merecen frontera propia
No los entierres como utilidades de red. Son parte del plano de control del clúster.

### Regla 4 — `security` y `admin` son first-class
No los metas como módulos accesorios al final. Deben tener crates y contratos claros.

### Regla 5 — features modernas detrás de interfaces
Trie memtables, BTI, UCS, SAI y vector search deben entrar sobre interfaces estables, no como invasión transversal improvisada.

## Secuencia sugerida de activación de crates

1. `common`, `types`, `config`, `schema`
2. `native`, `cql`
3. `storage`, `wal`, `sstable`
4. `cluster-metadata`, `gossip`, `messaging`
5. `coordinator`
6. `streaming`, `repair`
7. `lwt`, `consensus`
8. `security`
9. `observability`, `admin`, `tools`

## Criterio de éxito del mapa

El mapa es bueno si:

- cada crate tiene una responsabilidad comprensible;
- las dependencias van “hacia abajo” y no crean ciclos conceptuales;
- el equipo puede medir, perfilar y probar subsistemas por separado;
- y una optimización en storage o protocol no obliga a reescribir medio sistema.