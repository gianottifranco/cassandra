# Final Sign-off — Cassandra Java→Rust Rewrite

**Date**: 2026-03-16
**Phase**: 25 — Final Validation (Soak/Chaos/Performance/Fuzzing/Security/RC)
**Baseline**: trunk `076c6f11` (2026-03-15)

---

## Release Candidate Assessment

### Overall Status: **NOT YET RC** — Conditional GO with documented gaps

The Rust implementation has achieved structural parity across all major subsystems with comprehensive test coverage. The remaining gaps are documented and tracked with feature flags, guard-rails, and actionable TODOs.

---

## Gate Status Summary

### Functional Gates

| Gate | Description | Status | Evidence |
|------|-------------|--------|----------|
| F1 | CQL parser DDL/DML | 🔶 | `cassandra-cql` tests |
| F2 | Native protocol v4 | 🔶 | Frame codec tests |
| F3 | Storage write path | ✅ | `engine::tests` (7+ tests) |
| F4 | Storage read path | ✅ | `engine::tests` |
| F5 | Compaction (STCS) | ✅ | `compaction::tests` |
| F6 | Commit log replay | ✅ | Chaos tests validate crash recovery |
| F7 | Snapshots | ✅ | `snapshot_creates_directory` + chaos tests |
| F8 | LWT / Paxos | 🔶 | 21+ tests, in-memory only |
| F9 | Counters | 🔶 | 13 tests, no cleanup |
| F10 | Secondary indexes | 🔶 | Legacy + SAI (25+ tests) |
| F11 | SAI indexes | 🔶 | Range + kNN, no disk persistence |
| F12 | TLS encryption | 🔶 | rustls, not wired to listeners |
| F13 | Authentication | 🔶 | Password auth implemented |
| F14 | Authorization | 🔶 | Role hierarchy implemented |

### Performance Gates

| Gate | Description | Status | Evidence |
|------|-------------|--------|----------|
| P1 | Memtable write < 100μs p99 | 🔶 | `perf_budget_tests` — measure via `make perf-test` |
| P2 | Memtable read < 200μs p99 | 🔶 | `perf_budget_tests` — measure via `make perf-test` |
| P3 | Frame parse < 5μs p99 | 🔶 | `perf_budget_tests` — measure via `make perf-test` |
| P4 | Memory budget under soak | 🔶 | `soak_tests` — 6 scenarios |
| P5 | No performance regression | 🔶 | Criterion benchmarks + perf report |

### Operational Gates

| Gate | Description | Status | Evidence |
|------|-------------|--------|----------|
| O1 | Migration guide | ✅ | `migration-guide.md` |
| O2 | Rollback procedure | ✅ | `rollback-drill.sh` |
| O3 | Snapshot/restore | ✅ | `backup_restore_tests` |
| O4 | Soak test (1hr) | 🔶 | 6 soak scenarios, harness ready |
| O5 | Chaos test | ✅ | 10 chaos scenarios implemented |
| O6 | Metrics | 🔶 | Prometheus registry |
| O7 | SystemD service | ✅ | `cassandra-rust.service` |
| O8 | Docker image | 🔶 | Dockerfile exists |
| O9 | 3-node cluster | ❌ | `docker-compose.prod.yml` not validated |
| O10 | Rollback drill | ✅ | `rollback-drill.sh` |

### Security Gates

| Gate | Description | Status | Evidence |
|------|-------------|--------|----------|
| S1 | No unjustified `unsafe` | ✅ | Zero unsafe blocks |
| S2 | TLS reviewed | ✅ | `security_review.md` |
| S3 | Auth bypass impossible | ✅ | `security_audit.rs` |
| S4 | Audit logging | 🔶 | File-based, no syslog |
| S5 | Dependency audit | ✅ | `cargo deny check` + `deny.toml` |
| S6 | Binary hardening | ✅ | `[profile.production]` validated |

### Documentation Gates

| Gate | Description | Status | Evidence |
|------|-------------|--------|----------|
| D1 | Compatibility matrix | ✅ | `compatibility_matrix.md` |
| D2 | ADRs (21 total) | ✅ | `docs/rewrite/adrs/` |
| D3 | Feature matrix | ✅ | `feature_matrix.yaml` (63 features) |
| D4 | Runbooks | ✅ | 7 runbooks |
| D5 | Performance budgets | ✅ | ADR-015 |
| D6 | Phase reports | ✅ | Phases 1-5 |

---

## Test Coverage Summary

| Category | Tests | Status |
|----------|-------|--------|
| Soak tests | 6 scenarios | ✅ Implemented |
| Chaos tests | 10 scenarios | ✅ Implemented |
| Performance budget tests | 6 gate checks | ✅ Implemented |
| Property-based fuzz tests | 10 proptest strategies | ✅ Implemented |
| Security audit tests | 6 checks | ✅ Implemented |
| Golden tests | Protocol + types + errors | ✅ Pre-existing |
| Diff tests (Docker) | CQL query comparison | ✅ Pre-existing |
| Unit tests | Per-crate | ✅ Pre-existing |

**Run all validation**: `make final-validate`

---

## Known Limitations

| # | Limitation | Mitigation | Priority |
|---|-----------|------------|----------|
| 1 | Native protocol: no TCP listener | Frame codec complete, listener is integration work | P0 |
| 2 | Paxos: in-memory only | Feature flag, tests document the gap | P1 |
| 3 | SAI: no disk persistence | In-memory index works, disk is optimization | P1 |
| 4 | TLS: not wired to listeners | rustls stack ready, wiring needed | P1 |
| 5 | Auth: no persistent role store | In-memory works, needs DB backing | P1 |
| 6 | nodetool: ~140 commands are stubs | Core 17 implemented, stubs return errors | P2 |
| 7 | TCM/Accord: trunk-only, deferred | Behind feature flags | P3 |
| 8 | 3-node cluster: not validated | Docker compose ready, needs execution | P1 |

---

## Java Oracle Decision

**Decision**: RETAIN as oracle branch (ADR-021)

Java remains in the repository as behavioral reference until:
- All functional gates reach ✅
- Soak test passes for ≥1hr
- 3-node cluster validated
- Community PMC vote
- ≥3 months production operation by ≥2 operators

See [ADR-021](adrs/021-java-oracle-retirement.md) for full criteria.

---

## Verification Commands

```bash
# Full validation suite
make final-validate

# Individual validation targets
make check-all           # fmt + clippy + build + test
make soak-test           # Storage engine soak (10s default)
make chaos-test          # Failure injection scenarios
make perf-test           # Performance budget gates
make fuzz-test           # Property-based tests
make security-test       # Security audit + cargo-deny

# Long-duration soak (production validation)
SOAK_DURATION_SECS=3600 make soak-test

# Sanitizers (requires nightly)
bash scripts/sanitizer_ci.sh --all

# Benchmarks
cargo bench --workspace
```

---

## Recommendation

**Conditional GO for beta testing** with the following pre-conditions:
1. Wire native protocol TCP listener (P0 — enables real CQL client testing).
2. Validate 3-node Docker cluster (P1 — proves distributed path).
3. Run 1hr soak test on dedicated hardware (P1 — proves stability).

Once these 3 items are complete, the implementation is ready for community beta.
