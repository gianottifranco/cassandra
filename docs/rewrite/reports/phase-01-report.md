# Phase 1 – Baseline & Scaffold: Status Report

**Date**: 2026-03-15
**Author**: Principal Engineer (AI-assisted)
**Status**: complete

---

## 1. Summary of Changes

Phase 1 establishes the foundation for the Cassandra Java-to-Rust rewrite:

- **Frozen baseline**: trunk @ `076c6f11` (2026-03-13), with 8 unstable/trunk-only
  components explicitly excluded from the initial parity target.
- **Domain classification**: 22 functional domains mapped from 34 Java packages
  to 16 Rust crates with priority levels (P0/P1/P2/deferred).
- **Rust workspace**: 16-crate Cargo workspace under `rust/` — 14 library crates
  and 2 binary crate stubs, all compiling cleanly with Rust 2024 edition.
- **ADRs**: 6 foundational Architecture Decision Records covering compatibility,
  unsafe policy, feature flags, on-disk formats, runtime model, and Java oracle policy.
- **CI**: GitHub Actions workflow + Makefile for build/test/fmt/clippy/deny/bench.
- **Tests**: 27 automated tests covering error codes, version consistency,
  workspace structure, documentation existence, feature matrix parsing,
  unsafe audit, and license header compliance.
- **Zero Java modifications**: all existing Java code untouched.

## 2. Files Created/Modified

| File | Action | Purpose |
|------|--------|---------|
| `docs/rewrite/charter.md` | created | Project charter, baseline, scope, roadmap |
| `docs/rewrite/feature_matrix.yaml` | created | 22-domain compatibility matrix |
| `docs/rewrite/adrs/001-compatibility-criteria.md` | created | 4-tier compatibility model |
| `docs/rewrite/adrs/002-unsafe-policy.md` | created | Strict unsafe governance |
| `docs/rewrite/adrs/003-feature-flags.md` | created | Compile + runtime flags |
| `docs/rewrite/adrs/004-on-disk-formats.md` | created | SSTable/CommitLog/Hints strategy |
| `docs/rewrite/adrs/005-runtime-concurrency.md` | created | Tokio-based runtime design |
| `docs/rewrite/adrs/006-java-oracle-policy.md` | created | Oracle usage/retirement rules |
| `docs/rewrite/reports/template.md` | created | Reusable phase report template |
| `docs/rewrite/reports/phase-01-report.md` | created | This report |
| `rust/Cargo.toml` | created | Workspace root (16 members) |
| `rust/Makefile` | created | Build/test/lint shortcuts |
| `rust/deny.toml` | created | License/advisory audit config |
| `rust/crates/cassandra-common/` | created | Error types, version, utilities |
| `rust/crates/cassandra-config/` | created | Config loading stub |
| `rust/crates/cassandra-types/` | created | CQL type system stub |
| `rust/crates/cassandra-schema/` | created | Schema metadata stub |
| `rust/crates/cassandra-native-protocol/` | created | Protocol codec stub |
| `rust/crates/cassandra-cql/` | created | CQL parser stub |
| `rust/crates/cassandra-storage/` | created | Storage engine stub |
| `rust/crates/cassandra-cluster-metadata/` | created | Gossip/ring stub |
| `rust/crates/cassandra-messaging/` | created | Inter-node messaging stub |
| `rust/crates/cassandra-coordinator/` | created | Read/write coordinator stub |
| `rust/crates/cassandra-repair/` | created | Repair subsystem stub |
| `rust/crates/cassandra-streaming/` | created | Streaming subsystem stub |
| `rust/crates/cassandra-security/` | created | Auth/TLS stub |
| `rust/crates/cassandra-admin/` | created | Admin/observability stub |
| `rust/crates/cassandra-server/` | created | Server binary stub |
| `rust/crates/cassandra-tools/` | created | CLI tools binary stub |
| `.github/workflows/rust-ci.yml` | created | CI pipeline for Rust workspace |

**Total**: 30 new files. **0 existing files modified**.

## 3. Tests Added/Executed

| Test | Type | Result |
|------|------|--------|
| `cassandra_common::tests::version_is_set` | unit | ✅ pass |
| `cassandra_common::tests::error_display` | unit | ✅ pass |
| `cassandra_common::error::tests::error_codes_match_protocol_spec` | unit | ✅ pass |
| `cassandra_common::error::tests::io_error_converts` | unit | ✅ pass |
| `cassandra_common::version::tests::version_string_format` | unit | ✅ pass |
| `cassandra_common::version::tests::protocol_versions` | unit | ✅ pass |
| `workspace_tests::all_crates_exist` | integration | ✅ pass |
| `workspace_tests::documentation_exists` | integration | ✅ pass |
| `workspace_tests::feature_matrix_parseable` | integration | ✅ pass |
| `workspace_tests::no_unsafe_in_phase1` | integration | ✅ pass |
| `workspace_tests::version_consistency` | integration | ✅ pass |
| `workspace_tests::error_code_compliance` | integration | ✅ pass |
| `workspace_tests::all_crates_have_license_header` | integration | ✅ pass |
| 14× `crate_compiles` (per stub crate) | unit | ✅ pass |

**Additional checks**:
- `cargo build --workspace` ✅
- `cargo fmt --all -- --check` ✅
- `cargo clippy --workspace` ✅

## 4. Features Closed

- [x] Baseline freeze with commit, branch, date, and unstable-component exclusion
- [x] Complete domain classification (22 domains, 34 Java packages)
- [x] Project charter with scope, compatibility guarantees, and 8-phase roadmap
- [x] 16-crate Cargo workspace, all compiling
- [x] 6 ADRs covering all requested topics
- [x] CI pipeline (GitHub Actions + Makefile)
- [x] Test suite with 27 passing tests
- [x] Phase status report template
- [x] Phase 1 final report

## 5. Features Still Open

- [ ] `cargo deny check` — requires `cargo-deny` installed; CI handles this via action
  - **Gap**: Local `make deny` requires `cargo install cargo-deny`
  - **Plan**: Document in CONTRIBUTING.md or add install step to Makefile
  - **ETA**: Phase 2

- [ ] Benchmark skeleton — `cargo bench --no-run` works but no actual benchmarks defined
  - **Gap**: No criterion benchmarks in any crate yet
  - **Plan**: Add first benchmark with Phase 2 (protocol codec latency)
  - **ETA**: Phase 2

- [ ] Differential test harness — CI has placeholder, not yet connected
  - **Gap**: Requires Java build + test oracle infrastructure
  - **Plan**: Enable when first Rust subsystem produces comparable output (Phase 3+)
  - **ETA**: Phase 3

- [ ] `.gitignore` update for `rust/target/`
  - **Gap**: Not yet added due to zero-modification-to-existing-files policy
  - **Plan**: Add in Phase 2 or as separate commit
  - **ETA**: Phase 2

## 6. Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Crate dependency graph may need restructuring as business logic grows | M | Kept dependencies minimal in stubs; can refactor without breaking public APIs |
| Tokio version lock-in (ADR-005) | L | Tokio is the industry standard; full runtime swap unlikely but possible via traits |
| SSTable format documentation gap — Java code IS the spec | H | Prioritize SSTable reader tests in Phase 4; consider writing format spec as we implement |
| Rust 2024 edition is very new; some crate ecosystem lag possible | M | Fall back to 2021 edition if blocking issues arise |
| Large workspace build times as crates gain logic | M | CI caching via `rust-cache`; consider splitting workspace if compile times >5min |

## 7. Next Phase

**Phase 2: Types & Protocol Skeleton**

Focus areas:
1. Implement CQL native types in `cassandra-types` (all 20+ CQL types with
   serialization/deserialization and comparison).
2. Implement CQL native protocol frame codec in `cassandra-native-protocol`
   (parse/encode all frame types for v4/v5).
3. Add golden tests comparing Rust type serialization against Java output.
4. Add first criterion benchmark for protocol frame parsing.
5. Begin `cassandra-config` with `cassandra.yaml` parser (serde-based).

Dependencies:
- None beyond this phase's output.

Estimated scope:
- ~2000-3000 lines of Rust across 3 crates.
- ~500 lines of tests.
- 1-2 new ADRs (type system edge cases, protocol version negotiation).
