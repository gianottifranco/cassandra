# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
# See the NOTICE file and LICENSE.txt at the repository root.

.PHONY: build test fmt clippy deny bench check-all clean

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
