# Java ↔ Rust Compatibility Matrix

**Date**: 2026-03-15
**Purpose**: Track functional parity between Java baseline and Rust implementation per subsystem.

## Legend

| Level | Meaning |
|-------|---------|
| **Full** | Byte/wire/semantically compatible, tested |
| **Partial** | Core logic implemented, some edge cases missing |
| **Stub** | API surface exists, minimal logic |
| **None** | Not started |

---

## Protocol & Wire Compatibility

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| CQL native protocol (v4/v5) | STARTUP→READY handshake, QUERY, RESULT, ERROR, AUTH, BATCH, PREPARE, EXECUTE, REGISTER, EVENT | Codec stub: frame parse/encode, all opcodes + column types defined | Stub | No TCP listener wired; no actual query execution over wire |
| Internode messaging | Large/small channels, verb dispatch, flow control | Verb enum + stub handler dispatch | Stub | No gossip wire compat; cannot form mixed cluster |
| Gossip wire format | Serialized `EndpointState`, `GossipDigestSyn/Ack/Ack2` | `GossipMessage` enum, phi-accrual failure detector | Partial | Wire format not binary-compatible with Java |

## On-Disk Formats

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| SSTable (read) | `big` format: Data, Index, Filter, Statistics, CompressionInfo, Summary, TOC | Custom format: Data + Index + Filter + Stats + TOC | Partial | Not binary-compatible with Java SSTables; reads/writes own format |
| SSTable (write) | Same components as read | SSTableWriter produces readable SSTables | Partial | Cannot read Java SSTables or vice versa |
| CommitLog | Segment-based, CRC32 per mutation, sync modes | Segment + CRC + replay | Partial | Format differs from Java; cannot replay Java logs |
| Hints | File-based hint storage | Stub only | Stub | Cannot store/deliver hints |
| Snapshots | Hard-link SSTable files | `engine.snapshot()` hard-links | Partial | Format-specific; snapshots not portable Java↔Rust |

## Query & Data Path

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| CQL parser | Full CQL3 grammar (ANTLR) | Recursive-descent, major statements covered | Partial | Missing: GRANT, REVOKE, ROLE, custom payloads |
| Query planner | Schema-validated, restriction analysis | Statement validation against catalog | Partial | No cost-based decisions |
| Prepared statements | Parse-once, execute-many, schema-invalidation | PreparedStatementCache with generation tracking | Partial | Not wired to protocol layer |
| Consistency levels | ALL, QUORUM, LOCAL_QUORUM, ONE, etc. | Enum + timeout config defined | Stub | No actual multi-replica enforcement |
| Paging | Page sizes, paging state tokens | Not implemented | None | — |
| Read path | Memtable + SSTables, merge by timestamp | `read_partition()` merges memtable + SSTables | Partial | No range scan, no predicate push-down |
| Write path | CommitLog → Memtable → async flush | `apply_mutation()` with CL + memtable + flush | Partial | No async flush, no write-back pressure to coordinator |

## Distributed Operations

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| Token ring | Murmur3 partitioner, virtual nodes | Murmur3 + vnode support | Partial | Not connected to actual routing |
| Replication | SimpleStrategy, NetworkTopologyStrategy | Both strategies implemented | Partial | No actual data placement |
| Repair | Full + incremental, Merkle tree exchange | MerkleTree + RepairCoordinator stubs | Stub | No actual data exchange |
| Streaming | SSTable streaming for bootstrap/decommission | StreamSession + transfer stubs | Stub | No actual data transfer |
| LWT (Paxos) | Single-decree Paxos with prepare/propose/commit | Full state machine, coordinator, recovery, backoff | Partial | In-memory only; not persisted to system.paxos |
| Topology ops | Bootstrap, decommission, replace, removenode | Topology manager with event stubs | Stub | No actual token range movement |

## Security

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| TLS | JDK SSL, mutual TLS, cert hot-reload | rustls + cert file watcher | Partial | Not wired to listeners |
| Authentication | PasswordAuthenticator, AllowAll, LDAP | Password + AllowAll authenticators | Partial | No LDAP/Kerberos; no persistent role store |
| Authorization | CassandraAuthorizer, role inheritance | Permission checks with role hierarchy | Partial | No persistent permission store |
| Audit | File-based audit logging | AuditLogger with serde output | Partial | No syslog sink |
| CIDR filtering | IP-based access control | CIDR matcher implementation | Partial | Not wired |
| Data masking | Dynamic data masking | DDM with mask functions | Partial | Not wired to read path |

## Observability & Tooling

| Subsystem | Java Behavior | Rust Status | Parity | Blocking Gaps |
|-----------|--------------|-------------|--------|---------------|
| Metrics | JMX + 3rd-party reporters | Prometheus-native registry | Partial | No JMX; not full metric coverage |
| Virtual tables | system_views.* | 5 built-in tables | Partial | Not wired to query path |
| nodetool CLI | 40+ subcommands via JMX | 17 subcommands (clap), most stubs | Stub | Most commands are unimplemented stubs |
| Admin HTTP | Not in standard Cassandra | HTTP API for metrics + operations | Partial | Java doesn't have this; Rust-only feature |
| Distributed tracing | RequestTracing + system_traces | Trace context propagation | Stub | No storage of traces |
| FQL | Chronicle-queue based | Not implemented | None | Deferred |

## Mixed-Cluster Feasibility

> **Decision**: Mixed-cluster deployment is **NOT feasible** in this phase.

Reasons:
1. Gossip wire format not binary-compatible
2. Internode messaging protocol differs
3. SSTable format not cross-compatible
4. Schema coordination not implemented

**Migration strategy**: Dual-cluster with shadow traffic validation.
See [ADR-014](adrs/014-migration-strategy.md).

## Data Migration Path

| Step | Method | Validated |
|------|--------|-----------|
| Schema export | `DESCRIBE KEYSPACE` from Java → apply to Rust | ❌ pending |
| Bulk data | SSTable snapshot → convert or re-ingest via CQL | ❌ pending |
| Shadow traffic | FQL capture on Java → replay on Rust → diff | ❌ pending |
| Online migration | Dual-write + read comparison | ❌ pending |
| Cutover | DNS/LB switch + monitor | ❌ pending |
| Rollback | Stop Rust, restore Java from pre-migration snapshot | ✅ runbook exists |
