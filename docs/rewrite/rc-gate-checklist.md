# RC Gate Checklist

**Date**: 2026-03-17
**Baseline**: Java commit `076c6f11364645bbb43360f013bee6f50a099185` (trunk, 2026-03-15)
**Workspace**: 20 Rust crates, ~89k LOC, 3,300+ test functions

## Gate Results

| # | Category | Status | Evidence | Notes |
|---|----------|--------|----------|-------|
| 1 | Build & Lint | PASS | `cargo build --workspace`, `cargo clippy` | Zero warnings |
| 2 | Unit Tests | PASS | `cargo test --workspace` | All compilation errors fixed (Units 1-3) |
| 3 | Chaos Tests (10 scenarios) | PASS | `evidence/phase-26/chaos-tests.log` | Crash recovery, corruption, concurrent snapshot |
| 4 | Soak Tests (6 scenarios) | PASS | `evidence/phase-26/soak-tests.log` | Long-running stability under load |
| 5 | Performance Budgets (ADR-015) | PASS | `evidence/phase-26/perf-budget.log` | Latency, throughput within budget |
| 6 | Fuzz Tests | PASS | `evidence/phase-26/fuzz-tests.log` | Protocol codec, CQL type round-trips |
| 7 | Security Audit | PASS | `evidence/phase-26/security-audit.log` | TLS, auth, no known CVEs |
| 8 | Golden Tests | PASS | `evidence/phase-26/golden.log` | Java oracle fixtures match Rust output |
| 9 | Migration Validation | PASS | `evidence/phase-26/migration-validation.log` | Schema, CDC continuity, rollback |
| 10 | Single-Node CQL | PASS | `cluster-validate.sh --single-node-only` | Server starts, CQL port 9042, admin API 9180 |
| 11 | **Multi-Node Cluster** | **FAIL** | N/A | No gossip loop — nodes cannot discover each other. No StorageProxy — no distributed reads/writes. Docker 3-node runs as 3x independent single-node. |
| 12 | **Java Interop** | **FAIL** | N/A | Not binary-compatible with Java Cassandra. Cannot join existing Java cluster. By-design limitation (clean-room rewrite). |
| 13 | **Gap Guards** | **FAIL** | `evidence/phase-26/gap-guards.log` | 4/21 closed: trie_index, db_filters, row_transformations, concurrency_stages. 17 remain (CQL, distributed, security, experimental). |
| 14 | No Unsafe | PASS | `workspace_tests::no_unsafe_in_phase1` | Allowlist: `cassandra-io/src/util/{mmap_rebufferer,channel_proxy,rebufferer}.rs` (memory-mapped I/O). |
| 15 | Documentation | PASS | ADRs 001-029, phase reports, charter, feature matrix | All required docs present |

## Verdict

**NOT RC.** Three blocking failures (multi-node, Java interop, gap guards).

### Conditional GO for Single-Node Beta

The Rust implementation is ready for **single-node beta** deployment with the following caveats:

1. **Single-node only**: CQL server on port 9042, admin API on port 9180, full storage engine (commitlog, memtable, SSTable, compaction).
2. **No cluster operations**: Gossip, distributed reads/writes, repair, streaming, hints — all require StorageProxy and gossip loop.
3. **No mixed-mode**: Cannot join or interact with Java Cassandra clusters.

### P0 Blockers for RC

| Blocker | What's Missing | Estimated Scope |
|---------|----------------|-----------------|
| Gossip loop | Periodic gossip round, failure detection, cluster membership | ~5k LOC |
| StorageProxy | Coordinator read/write paths, replica selection, consistency enforcement | ~10k LOC |
| Gap guards (17) | CQL functions, materialized views, SASI, CDC reader, repair protocol, etc. | Varies per gap |

### P1 for Post-Beta

- Full CQL function library (mathematical, string, aggregate)
- Materialized views
- SASI/SAI index integration
- CDC log consumer
- Repair protocol (Merkle tree exchange)
- Streaming (bulk data transfer)

## How to Reproduce

```bash
cd rust

# Full evidence capture
bash scripts/capture-evidence.sh

# Cluster validation (Tier 1 only)
bash scripts/cluster-validate.sh --single-node-only

# Individual suites
make chaos-test
make soak-test
make perf-test
make fuzz-test
make security-test
make validate-migration
```
