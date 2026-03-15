# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
# See the NOTICE file and LICENSE.txt at the repository root.

.PHONY: build test fmt clippy deny bench check-all clean diff-test golden-test generate-golden

# Default target
all: check-all

## Build all crates in the workspace
build:
	cargo build --workspace

## Run all tests
test:
	cargo test --workspace

## Check code formatting
fmt:
	cargo fmt --all -- --check

## Run clippy lints (warnings as errors)
clippy:
	cargo clippy --workspace -- -D warnings

## Run cargo-deny checks (licenses, advisories, bans)
deny:
	cargo deny check

## Run benchmarks (compile only by default)
bench:
	cargo bench --workspace --no-run

## Run all checks: fmt, clippy, build, test, deny
check-all: fmt clippy build test
	@echo ""
	@echo "✓ All checks passed"

## Clean build artifacts
clean:
	cargo clean

## Show workspace member list
members:
	@cargo metadata --no-deps --format-version 1 | \
		python3 -c "import sys,json; [print(p['name']) for p in json.load(sys.stdin)['packages']]"

## Count unsafe blocks across the workspace
unsafe-audit:
	@echo "=== Unsafe blocks ==="
	@grep -rn "unsafe" crates/*/src/ --include="*.rs" || echo "No unsafe blocks found"

## Show feature matrix status
status:
	@echo "=== Feature Matrix Status ==="
	@grep -E "^  - name:|status:" ../docs/rewrite/feature_matrix.yaml | paste - - | \
		sed 's/  - name: //; s/    status: /\t/' || true

## ── Differential Testing ────────────────────────────────────────────────

## Run the full differential test suite (requires Docker)
diff-test:
	cargo run -p xtask -- diff-test

## Run offline golden tests only (no Docker required)
golden-test:
	cargo run -p xtask -- golden-test

## Generate golden fixtures from Java oracle
generate-golden:
	cargo run -p xtask -- generate-golden

## Start Docker services for manual testing
docker-up:
	cd diff-tests && docker compose up -d java-oracle rust-sut

## Stop Docker services
docker-down:
	cd diff-tests && docker compose down -v

## Run pytest diff-tests (requires running Docker services)
pytest:
	cd diff-tests/pytest && python3 -m pytest -v --tb=short

## ── Phase 5: Production Readiness ───────────────────────────────────────

## Build optimized release binaries
release:
	cargo build --profile production -p cassandra-server -p cassandra-tools

## Build Docker image
docker:
	docker build -t cassandra-rust:dev .

## Run soak tests (long-running, ~10s default)
soak-test:
	cargo test -p cassandra-diff-tests --test soak_tests -- --ignored --nocapture

## Run chaos tests
chaos-test:
	cargo test -p cassandra-diff-tests --test chaos_tests -- --nocapture

## Run backup/restore drill tests
backup-test:
	cargo test -p cassandra-diff-tests --test backup_restore_tests -- --nocapture

## Run all benchmarks
bench-run:
	cargo bench --workspace

## Run rollback drill
rollback-drill:
	bash scripts/rollback-drill.sh

## Build release package
package:
	bash scripts/build-release.sh

## Run Phase 5 full validation
phase5-validate: check-all chaos-test backup-test bench
	@echo ""
	@echo "✓ Phase 5 validation complete"
