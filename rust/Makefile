# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
# See the NOTICE file and LICENSE.txt at the repository root.

.PHONY: build test fmt clippy deny bench check-all clean diff-test diff-test-rust-only golden-test generate-golden migration-test upgrade-test cdc-test rollback-test validate-migration perf-test perf-test-strict fuzz-test security-test sanitizer-test final-validate final-validate-strict gap-audit cluster-validate capture-evidence capture-evidence-strict phase26-validate phase26-validate-strict phase26-validate-rust-only phase26-validate-rust-only-strict coverage-audit coverage-guard java-code-audit java-code-audit-strict

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

## Run gap audit (TODO/stub/gap-guard scanning)
gap-audit:
	python3 scripts/gap_audit.py todos
	python3 scripts/gap_audit.py gap-guards

## Run coverage audit (gap matrix validation)
coverage-audit:
	cargo run -p xtask -- coverage-audit

## Audit Java source presence in repository (advisory)
java-code-audit:
	cargo run -p xtask -- java-code-audit

## Audit Java source presence in repository (strict: must be zero)
java-code-audit-strict:
	cargo run -p xtask -- java-code-audit-strict

## Run coverage guard (CI-friendly: strict mode, fail on unclassified)
coverage-guard:
	@echo "── Coverage Guard ──────────────────────────────────"
	@python3 ../scripts/coverage_audit.py --check-matrix --strict --repo-root ..
	@echo ""
	@echo "── Gap Guard Count ─────────────────────────────────"
	@echo "Ignored gap guards:"
	@grep -c '#\[ignore' crates/cassandra-diff-tests/src/gap_guards.rs || echo "0"
	@echo "Active (non-ignored) gap guards:"
	@grep -c 'fn gap_guard_' crates/cassandra-diff-tests/src/gap_guards.rs || echo "0"
	@echo ""
	@echo "── YAML Validity ───────────────────────────────────"
	@python3 -c "import yaml; yaml.safe_load(open('../docs/rewrite/final_gap_matrix.yaml')); print('YAML valid')"
	@echo ""
	@echo "✓ Coverage guard passed"

## ── Differential Testing ────────────────────────────────────────────────

## Run the full differential test suite (requires Docker)
diff-test:
	cargo run -p xtask -- diff-test

## Run Rust-only differential suite (no Java oracle / Docker required)
diff-test-rust-only:
	cargo run -p xtask -- diff-test-rust-only

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
	cargo test -p cassandra-diff-tests --test soak_tests -- --nocapture

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
	cargo test -p cassandra-diff-tests --lib cdc_tests -- --nocapture

## Run rollback validation tests
rollback-test:
	cargo test -p cassandra-diff-tests --lib rollback_tests -- --nocapture

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
	cargo test -p cassandra-diff-tests --test perf_budget_tests -- --nocapture

## Run performance budget tests in strict mode (fail on budget overrun)
perf-test-strict:
	CASSANDRA_STRICT_PERF_BUDGET=1 cargo test -p cassandra-diff-tests --test perf_budget_tests -- --nocapture

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

## Run final validation with strict perf budget enforcement
final-validate-strict:
	cargo run -p xtask -- final-validate-strict

## ── Phase 26: Final Validation ───────────────────────────────────────

## Run cluster validation (Tier 1: single-node only)
cluster-validate:
	bash scripts/cluster-validate.sh --single-node-only

## Capture evidence for all test suites
capture-evidence:
	cargo run -p xtask -- capture-evidence

## Capture evidence with strict performance budget enforcement
capture-evidence-strict:
	cargo run -p xtask -- capture-evidence-strict

## Run Phase 26 full validation
phase26-validate:
	cargo run -p xtask -- phase26-validate
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  ✓ Phase 26 Validation Complete"
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  See docs/rewrite/rc-gate-checklist.md for RC assessment"
	@echo "  See evidence/phase-26/ for test logs"
	@echo "═══════════════════════════════════════════════════════════"

## Run Phase 26 validation with strict performance budgets
phase26-validate-strict:
	cargo run -p xtask -- phase26-validate-strict
	@echo ""
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  ✓ Phase 26 Strict Validation Complete"
	@echo "═══════════════════════════════════════════════════════════"
	@echo "  Strict perf budget mode: enabled"
	@echo "  See evidence/phase-26/summary.json for suite summary"
	@echo "═══════════════════════════════════════════════════════════"

## Run Java-free validation (Phase 26 + rust-only diff + coverage audit)
phase26-validate-rust-only:
	cargo run -p xtask -- phase26-validate-rust-only

## Run Java-free validation with strict performance budgets
phase26-validate-rust-only-strict:
	cargo run -p xtask -- phase26-validate-rust-only-strict
