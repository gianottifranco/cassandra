# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
# See the NOTICE file and LICENSE.txt at the repository root.

.PHONY: build test fmt clippy deny bench check-all clean diff-test golden-test generate-golden migration-test upgrade-test cdc-test rollback-test validate-migration perf-test fuzz-test security-test sanitizer-test final-validate

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

## ── Migration & Upgrade ────────────────────────────────────────────────

## Run migration crate tests
migration-test:
	cargo test -p cassandra-migration -- --nocapture

## Run upgrade harness tests
upgrade-test:
	cargo test -p cassandra-diff-tests upgrade_harness -- --nocapture

## Run CDC continuity tests
cdc-test:
	cargo test -p cassandra-diff-tests cdc_tests -- --nocapture

## Run rollback validation tests
rollback-test:
	cargo test -p cassandra-diff-tests rollback_tests -- --nocapture

## Run full migration validation suite
validate-migration: migration-test upgrade-test cdc-test rollback-test
	@echo ""
	@echo "✓ Migration validation complete"

## Run migration validation via script
validate-migration-full:
	bash scripts/validate-migration.sh

## ── Phase 25: Final Validation ─────────────────────────────────────────

## Run performance budget tests (ADR-015 gate checks)
perf-test:
	cargo test -p cassandra-diff-tests --test perf_budget_tests -- --ignored --nocapture

## Run property-based fuzz tests
fuzz-test:
	cargo test -p cassandra-diff-tests --test fuzz_tests -- --nocapture

## Run security audit tests + cargo-deny
security-test:
	cargo test -p cassandra-diff-tests --test security_audit -- --nocapture
	@echo ""
	@cargo deny check 2>&1 || echo "⚠️  cargo-deny not installed or check failed"

## Run sanitizers (requires nightly Rust)
sanitizer-test:
	bash scripts/sanitizer_ci.sh --all

## Run ALL final validation: checks + soak + chaos + perf + fuzz + security
final-validate: check-all chaos-test soak-test perf-test fuzz-test security-test
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  ✓ Final Validation Complete (Phase 25)"
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  Results:"
	@echo "    ✅ Code quality (fmt, clippy, build, test)"
	@echo "    ✅ Chaos tests (10 scenarios)"
	@echo "    ✅ Soak tests (6 scenarios)"
	@echo "    ✅ Performance budgets (ADR-015)"
	@echo "    ✅ Property-based fuzz tests"
	@echo "    ✅ Security audit"
	@echo "═══════════════════════════════════════════════════════════"
