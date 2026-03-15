# GA Readiness Checklist

**Version**: 0.1.0
**Date**: 2026-03-15
**Target**: Production GA release of Cassandra Rust

## Legend

- ✅ Complete
- 🔶 Partial (documented gaps)
- ❌ Not started
- 🚫 Deferred (documented rationale)

---

## Functional Gates

| # | Gate | Status | Evidence | Notes |
|---|------|--------|----------|-------|
| F1 | CQL parser covers all DDL/DML | 🔶 | `cassandra-cql` tests | Missing: GRANT, REVOKE, ROLE, custom payloads |
| F2 | Native protocol v4 handshake | 🔶 | Codec tests | Frame parse/encode works; no TCP listener |
| F3 | Storage engine write path | ✅ | `engine::tests` (7 tests) | CL → memtable → flush → SSTable |
| F4 | Storage engine read path | ✅ | `engine::tests` | Merged memtable + SSTable reads |
| F5 | Compaction (STCS) | ✅ | `compaction::tests` | Size-tiered strategy with GC |
| F6 | Commit log replay | ✅ | `engine::tests::commit_log_replay` | Crash recovery validated |
| F7 | Snapshots | ✅ | `engine::tests::snapshot_creates_directory` | Hard-link based |
| F8 | LWT / Paxos | 🔶 | `paxos` module (21+ tests) | In-memory only; no persistence |
| F9 | Counters (CRDT) | 🔶 | `counter::tests` (13 tests) | Binary-compatible; no cleanup |
| F10 | Secondary indexes (legacy) | 🔶 | `index::legacy::tests` (7 tests) | In-memory BTreeMap |
| F11 | SAI indexes | 🔶 | `sai` module (15+ tests) | No disk persistence; no ANN |
| F12 | TLS encryption | 🔶 | `security::tls::tests` | rustls implemented; not wired |
| F13 | Authentication | 🔶 | `security::auth::tests` | Password auth; no LDAP |
| F14 | Authorization | 🔶 | `security::authz::tests` | Role hierarchy; no persistent store |

## Performance Gates

| # | Gate | Status | Evidence | Notes |
|---|------|--------|----------|-------|
| P1 | Memtable write < 100μs p99 | ❌ | Benchmark stub | Run `cargo bench --bench engine_bench` |
| P2 | Memtable read < 200μs p99 | ❌ | Benchmark stub | Run `cargo bench --bench engine_bench` |
| P3 | Frame parse < 5μs p99 | ❌ | Benchmark stub | Run `cargo bench --bench protocol_bench` |
| P4 | Memory budget under soak | ❌ | Soak test | Run `cargo test --test soak_tests -- --ignored` |
| P5 | No performance regression | ❌ | CI benchmark history | Requires baseline run |

## Operational Gates

| # | Gate | Status | Evidence | Notes |
|---|------|--------|----------|-------|
| O1 | Migration guide documented | ✅ | `migration-guide.md` | — |
| O2 | Rollback procedure tested | ✅ | `backup_restore_tests` | Automated drill |
| O3 | Snapshot/restore validated | ✅ | `backup_restore_tests` | Multiple snapshots tested |
| O4 | Soak test passing (1hr) | ❌ | `soak_tests` | Harness ready, not yet run |
| O5 | Chaos test passing | 🔶 | `chaos_tests` | 5 scenarios implemented |
| O6 | Prometheus metrics exposed | 🔶 | `admin::prometheus_metrics` | Registry exists; not scraped |
| O7 | SystemD service file | ✅ | `deploy/cassandra-rust.service` | — |
| O8 | Docker image builds | ❌ | `Dockerfile` | Not yet tested |
| O9 | 3-node cluster tested | ❌ | `docker-compose.prod.yml` | Config ready, not validated |
| O10 | Rollback drill script | ✅ | `scripts/rollback-drill.sh` | — |

## Documentation Gates

| # | Gate | Status | Evidence | Notes |
|---|------|--------|----------|-------|
| D1 | Compatibility matrix | ✅ | `compatibility_matrix.md` | 40+ subsystem rows |
| D2 | ADRs for all major decisions | ✅ | 15 ADRs | ADR-001 through ADR-015 |
| D3 | Feature matrix up-to-date | ✅ | `feature_matrix.yaml` | Status tracked per domain |
| D4 | Runbooks for operators | ✅ | `runbooks/` (5 runbooks) | Backup, ops, TLS, incidents, rollback |
| D5 | Performance budgets defined | ✅ | ADR-015 | 7 latency + 4 throughput budgets |
| D6 | Phase reports complete | ✅ | `reports/` | Phases 1-5 |

## Security Gates

| # | Gate | Status | Evidence | Notes |
|---|------|--------|----------|-------|
| S1 | No `unsafe` without justification | ✅ | `make unsafe-audit` | Zero unsafe blocks in Phase 1-4 |
| S2 | TLS implementation reviewed | 🔶 | ADR-012 | rustls; not wired to listeners |
| S3 | Auth bypass impossible | 🔶 | `security::auth::tests` | Tests for AllowAll and Password |
| S4 | Audit logging functional | 🔶 | `security::audit::tests` | File-based; no syslog |
| S5 | Dependency audit clean | ❌ | `cargo deny check` | Not yet run in CI |

## Release Criteria

For GA, the following must be met:

1. **All F-gates**: At least 🔶 (with documented gaps)
2. **Performance P1-P4**: Measured and within budget
3. **Operations O1-O5**: ✅
4. **Documentation D1-D6**: ✅
5. **Security S1-S3**: ✅
6. **Zero known data-loss bugs**
7. **Rollback validated by 2+ operators**

### Current Assessment

**Status**: **Not ready for GA** — achieving production readiness requires:
1. Wire native protocol listener (TCP accept → frame decode → query execute → respond)
2. Run and validate performance benchmarks
3. Build and test Docker image
4. Validate 3-node cluster operation
5. Complete soak test (1hr minimum)
