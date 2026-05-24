# Cassandra Differential Testing Harness

Automated comparison infrastructure for verifying the Cassandra Rust
implementation against the Java oracle.

## Prerequisites

- **Always**: Rust 1.85+, Cargo
- **For golden tests**: No additional requirements (offline)
- **For live diff-tests**: Docker, Docker Compose, Python 3.10+
- **For fixture generation**: `pip install cassandra-driver`

## Quick Start

```bash
# Run offline golden tests (no Docker needed)
cd rust
make golden-test

# Run full differential test suite (requires Docker)
make diff-test

# Run Rust-only differential suite (no Java oracle / Docker)
make diff-test-rust-only

# Or using cargo xtask directly:
cargo xtask diff-test
cargo xtask diff-test-rust-only
cargo xtask golden-test
cargo xtask generate-golden

# Final validation gates (no Docker required)
cargo xtask final-validate
cargo xtask final-validate-strict

# Phase 26 evidence capture
make capture-evidence
make capture-evidence-strict
cargo xtask capture-evidence
cargo xtask capture-evidence-strict

# Phase 26 full validation
cargo xtask phase26-validate
cargo xtask phase26-validate-strict

# Java-free full validation (no Java oracle / Docker)
cargo xtask phase26-validate-rust-only
cargo xtask phase26-validate-rust-only-strict

# Java code presence audit
cargo xtask java-code-audit
cargo xtask java-code-audit-strict
```

## Architecture

```
diff-tests/
├── docker/
│   ├── Dockerfile.java-oracle    # Java Cassandra from source
│   ├── Dockerfile.rust-stub      # Rust workspace + TCP stub
│   └── stub-listener.py          # Protocol-aware TCP stub
├── docker-compose.yml            # Dual-cluster orchestration
├── golden/                       # Pre-committed golden fixtures
│   ├── errors/                   # Error code references
│   ├── types/                    # CQL type serialization reference
│   ├── stubs/                    # Manifests for future phases
│   └── metadata.json
├── golden-gen/
│   ├── generate_golden.py        # Fixture generator script
│   └── requirements.txt
├── pytest/
│   ├── conftest.py               # Session fixtures, skip handling
│   ├── test_protocol_behavior.py # Protocol-level diff tests
│   ├── test_schema_ddl.py        # DDL operation diff tests
│   ├── test_read_write_path.py   # Read/write path diff tests
│   ├── test_consistency_levels.py# CL semantics diff tests
│   └── requirements.txt
└── README.md                     # This file
```

## Test Tiers

### Tier 1: Golden Tests (Offline)
```bash
make golden-test
# or
cargo test -p cassandra-diff-tests
```
Compares Rust output against pre-committed JSON/binary fixtures. No Docker.

### Tier 2: Live Differential Tests
```bash
make diff-test
```
Starts Java oracle + Rust stub via Docker Compose, runs full test suite.
If you need to keep existing `diff-test` automation but skip Java/oracle
dependencies, set `CASSANDRA_NO_JAVA_ORACLE=1`.

### Tier 2B: Rust-Only Differential Tests
```bash
make diff-test-rust-only
# or:
cargo xtask diff-test-rust-only
# compatibility path:
CASSANDRA_NO_JAVA_ORACLE=1 cargo xtask diff-test
```
Runs offline differential checks only (golden, fuzz, protocol/error/tombstone).
No Docker or Java oracle required.

### Tier 3: Manual / Docker-only
```bash
make docker-up                    # Start services
make pytest                       # Run pytest separately
make docker-down                  # Stop services
```

### Tier 4: Final Validation Gates (No Docker)
```bash
# Advisory perf mode (budget overruns warn)
cargo xtask final-validate

# Strict perf mode (budget overruns fail)
cargo xtask final-validate-strict
# equivalent:
CASSANDRA_STRICT_PERF_BUDGET=1 cargo xtask final-validate
```

### Tier 5: Phase 26 Evidence Capture
```bash
# Capture suite logs + summary JSON
make capture-evidence
# or
cargo xtask capture-evidence

# Capture with strict perf budget enforcement
make capture-evidence-strict
# or
cargo xtask capture-evidence-strict
# equivalent:
bash scripts/capture-evidence.sh --strict-perf

# Fast command audit without executing suites
bash scripts/capture-evidence.sh --strict-perf --dry-run
```

### Tier 6: Phase 26 Full Validation
```bash
# Advisory perf mode
cargo xtask phase26-validate

# Strict perf mode
cargo xtask phase26-validate-strict
# equivalent:
CASSANDRA_STRICT_PERF_BUDGET=1 cargo xtask phase26-validate
```

### Tier 7: Java-Free Full Validation
```bash
# Advisory perf mode
cargo xtask phase26-validate-rust-only
# or
make phase26-validate-rust-only

# Strict perf mode
cargo xtask phase26-validate-rust-only-strict
# or
make phase26-validate-rust-only-strict
```
Runs Phase 26 + Rust-only differential tests + coverage audit without Java
oracle or Docker dependencies.

### Tier 8: Java Code Removal Audit
```bash
# Advisory: show remaining Java file count and path groups
cargo xtask java-code-audit
# or
make java-code-audit

# Strict: fail unless zero Java files remain
cargo xtask java-code-audit-strict
# or
make java-code-audit-strict
```

## Adding New Golden Fixtures

1. Add the query/operation to `golden-gen/generate_golden.py`
2. Regenerate: `make generate-golden`
3. Add corresponding Rust test in `cassandra-diff-tests/src/`
4. Commit the updated `golden/` directory

## Docker Compose Profiles

```bash
# Single-node (default)
cd diff-tests
docker compose up -d

# Three-node Java cluster
docker compose --profile three-node up -d

# Golden fixture generation
docker compose --profile golden run --rm golden-gen
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CASSANDRA_JAVA_HOST` | `localhost` | Java oracle hostname |
| `CASSANDRA_JAVA_PORT` | `19042` | Java oracle native port |
| `CASSANDRA_RUST_HOST` | `localhost` | Rust SUT hostname |
| `CASSANDRA_RUST_PORT` | `29042` | Rust SUT native port |
| `CASSANDRA_HOST` | `localhost` | Host for golden fixture generation |
| `CASSANDRA_PORT` | `9042` | Port for golden fixture generation |
| `CASSANDRA_STRICT_PERF_BUDGET` | `0` | Fail perf budget tests when set to `1`/`true` |
| `CASSANDRA_NO_JAVA_ORACLE` | `0` | When `1`, `cargo xtask diff-test` runs Rust-only mode |

## Understanding Test Results

- **Java tests pass, Rust tests XFAIL**: Normal during early phases.
- **XPASS (unexpected pass)**: Rust implementation has progressed! Remove the
  `@pytest.mark.xfail` marker and promote to a regular test.
- **Java test fails**: Possible oracle issue — investigate.
- **Golden test fails**: Either Rust regression or stale fixture — regenerate
  and compare.

## Related Documentation

- [ADR-007: Differential Testing Strategy](../../../docs/rewrite/adrs/007-differential-testing-strategy.md)
- [ADR-006: Java Oracle Policy](../../../docs/rewrite/adrs/006-java-oracle-policy.md)
- [Phase 2 Gap Report](../../../docs/rewrite/reports/phase-02-gap-report.md)
- [Project Charter](../../../docs/rewrite/charter.md)
