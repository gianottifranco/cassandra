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

# Or using cargo xtask directly:
cargo xtask diff-test
cargo xtask golden-test
cargo xtask generate-golden
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

### Tier 3: Manual / Docker-only
```bash
make docker-up                    # Start services
make pytest                       # Run pytest separately
make docker-down                  # Stop services
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
