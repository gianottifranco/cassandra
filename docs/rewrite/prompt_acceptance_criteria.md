# Prompt Acceptance Criteria

Each prompt must satisfy its preconditions, deliver its outputs, pass required tests, and meet the sign-off gate before the next prompt begins.

## Per-Prompt Matrix

| Prompt | Preconditions | Deliverables | Tests Required | Sign-off Gate |
|--------|--------------|--------------|----------------|---------------|
| 00 | Baseline frozen (ADR-016) | Backlog, audit script, CI job, tracking docs | Audit script runs, CI YAML valid | All docs reviewed, `make gap-audit` passes |
| 01 | Prompt 00 done | TCP listener, protocol negotiation | Integration: CQL driver connects | `cqlsh -e "SELECT now()"` returns |
| 02 | Prompt 01 done | QueryProcessor, config parsing | Golden: SELECT/INSERT/UPDATE/DELETE | Queries execute over wire |
| 03 | Prompt 02 done | Functions, restrictions, selection, prepared stmts | Golden: function eval, WHERE clauses | All CQL golden tests pass |
| 04 | Prompt 03 done | Type system, serializers, memtable, commitlog | Unit: all CQL types round-trip | Diff test: type serialization matches Java |
| 05 | Prompt 04 done | Row/partition iterators, filters, transforms | Unit: multi-partition scans | Read path golden tests pass |
| 06 | Prompt 05 done | StorageProxy, hints, batchlog | Integration: CL.ONE/QUORUM | Coordinated read/write with consistency |
| 07 | Prompt 06 done | Gossip loop, schema exchange, compaction strats | Integration: 2-node gossip | Nodes discover each other |
| 08 | Prompt 07 done | 3-channel internode, CRC, LZ4 | Integration: message exchange | No new TCP per send, CRC verified |
| 09 | Prompt 08 done | CompactionManager execution, lifecycle | Integration: compaction runs | STCS compaction completes on test data |
| 10 | Prompt 09 done | Compression, mmap, RandomAccessReader | Integration: compressed SSTable I/O | SSTable round-trip with compression |
| 11 | Prompt 10 done | Java SSTable reader, migration tool | Diff: read Java SSTables | Java SSTable readable by Rust |
| 12 | Prompt 11 done | Streaming transport | Integration: SSTable transfer | Data moves between nodes |
| 13 | Prompt 12 done | Consistent repair, Merkle exchange | Integration: repair session | Incremental repair completes |
| 14 | Prompt 13 done | Auth persistence, CIDR, mTLS | Integration: create user → restart → auth | Credentials survive restart |
| 15 | Prompt 14 done | Guardrails, config validation | Unit: guardrail enforcement | Config-driven limits enforced |
| 16 | Prompt 15 done | Legacy 2i index execution | Integration: CREATE INDEX + query | Index queries return correct results |
| 17 | Prompt 16 done | SAI query execution, vector kNN | Integration: SAI indexed queries | SAI queries return correct results |
| 18 | Prompt 17 done | Schema propagation, UDF/UDA, triggers | Integration: ALTER TABLE propagates | Schema changes visible cluster-wide |
| 19 | Prompt 18 done | Virtual tables, tracing, audit, diagnostics | Unit: system_views queryable | Virtual table queries return data |
| 20 | Prompt 19 done | KeyCache, RowCache | Integration: cache hit rate | Cache reduces disk I/O measurably |
| 21 | Prompt 20 done | TCM commit protocol | Integration: metadata linearizable | Metadata commits survive leader failure |
| 22 | Prompt 21 done | Paxos persistence, PaxosV2 | Integration: LWT survives restart | system.paxos data persists |
| 23 | Prompt 22 done | Token allocator, snitches | Integration: balanced joins | New node gets fair token range |
| 24 | Prompt 23 done | nodetool commands, cqlsh, offline tools | Unit: each command works | nodetool status/info/ring work |
| 25 | Prompt 24 done | Full metrics, exception hierarchy, utils | Unit: metric families registered | Prometheus endpoint returns all metrics |
| 26 | Prompts 01-25 substantially done | 3-node validation, chaos, RC gates | E2E: full workload on 3-node cluster | Chaos tests pass, performance budgets met |

## Universal Gates (every prompt)

1. `cargo check --workspace` succeeds
2. `cargo test --workspace` — no new failures
3. `cargo clippy --workspace -- -D warnings` — clean
4. `python3 rust/scripts/gap_audit.py todos` — no new untracked markers in critical subsystems
5. Gap guard count does not increase (non-degradation policy, ADR-022)
