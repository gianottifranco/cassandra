# Execution Order

Formalized from `prompts/step 3/00_orden_de_ejecucion.md`.

## Phase A — Startup Essential (P0)

Prompts 00–10. Goal: a single Rust node that accepts CQL connections and runs basic queries.

| Order | Prompt | Focus | Key Deliverables |
|-------|--------|-------|------------------|
| 1 | 00 | Orchestration & Baseline | Backlog, audit scripts, CI enforcement |
| 2 | 01 | Real CQL Server, TCP Listener | TCP accept, native protocol v4/v5 |
| 3 | 02 | QueryProcessor & Central Execution | Parse → plan → execute pipeline |
| 4 | 03 | CQL Semantics & Built-in Functions | Functions, restrictions, selection |
| 5 | 04 | Marshal/Type System | All CQL types, serialization |
| 6 | 05 | Rows/Partitions/Filters/Transforms | Iterator framework, filter subsystem |
| 7 | 06 | StorageService/StorageProxy/Hints/Batchlog | Coordinated read/write |
| 8 | 07 | Gossip & Schema Exchange | Gossip loop, schema propagation |
| 9 | 08 | Persistent Internode Messaging | 3-channel, CRC, LZ4 |
| 10 | 09 | CompactionManager/Lifecycle/Journal | Compaction execution |
| 11 | 10 | IO Util Layer | Compression, mmap, buffers |

## Phase B — Beta Real (P1)

Prompts 11–20. Goal: multi-node cluster running real workloads.

| Order | Prompt | Focus | Key Deliverables |
|-------|--------|-------|------------------|
| 12 | 11 | SSTable Compatibility/Versioning | Read Java SSTables, migration tool |
| 13 | 12 | Streaming Transport | SSTable transfer between nodes |
| 14 | 13 | Consistent Repair Protocol | Incremental repair |
| 15 | 14 | Persistent Auth (system_auth) | Auth persistence, CIDR, mTLS |
| 16 | 15 | Security/Config/Guardrails | Guardrails framework, config |
| 17 | 16 | Core Indexes (2i legacy) | Legacy index execution |
| 18 | 17 | SAI/SASI/Vector Search | SAI query execution |
| 19 | 18 | Schema/Views/Triggers/UDF/UDA | Schema change propagation |
| 20 | 19 | Virtual Tables/Tracing/Audit/Diag | system_views, diagnostics |
| 21 | 20 | Cache Subsystem | KeyCache, RowCache |

## Phase C — Path to GA (P2)

Prompts 21–26. Goal: production-ready, validated system.

| Order | Prompt | Focus | Key Deliverables |
|-------|--------|-------|------------------|
| 22 | 21 | TCM/CMS Complete | Linearizable metadata |
| 23 | 22 | Paxos/LWT/Accord/Consensus | Persistence, PaxosV2 |
| 24 | 23 | DHT/Locator/Snitches/Token Allocator | Balanced token distribution |
| 25 | 24 | Tooling Parity (nodetool, cqlsh, offline) | ~140 nodetool commands |
| 26 | 25 | Metrics/Exceptions/Utils/Stub Cleanup | Full observability |
| 27 | 26 | Final Cluster Validation/Chaos/RC Gates | 3-node cluster, chaos tests |

## Dependencies & Recommendations

1. Execute Prompt 00 first even though it's "just management" — it prevents traceability loss
2. If Prompts 01 or 02 find architecture blockers, resolve before continuing
3. Do not skip to SAI/TCM/Accord if server, QueryProcessor, StorageProxy, gossip, or internode messaging are broken
4. Use Prompt 26 only when most prior prompts are closed and the cluster runs real workloads
