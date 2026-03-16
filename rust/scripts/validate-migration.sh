#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# validate-migration.sh — Post-migration validation script.
#
# Runs the full suite of migration validation checks:
# 1. Schema diff
# 2. Version compatibility
# 3. CDC continuity
# 4. Migration test suite
#
# Usage:
#   ./scripts/validate-migration.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

echo "═══════════════════════════════════════════════════════"
echo "  Migration Validation Suite"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "═══════════════════════════════════════════════════════"

PASS=0
FAIL=0

run_check() {
    local name="$1"
    shift
    echo ""
    echo "── ${name} ──"
    if "$@" 2>&1; then
        echo "  ✅ ${name}: PASSED"
        PASS=$((PASS + 1))
    else
        echo "  ❌ ${name}: FAILED"
        FAIL=$((FAIL + 1))
    fi
}

# 1. Build check
run_check "Workspace Build" cargo build --workspace

# 2. Migration crate tests
run_check "Migration Crate Tests" cargo test -p cassandra-migration -- --nocapture

# 3. Upgrade harness tests
run_check "Upgrade Harness Tests" cargo test -p cassandra-diff-tests upgrade_harness -- --nocapture

# 4. CDC continuity tests
run_check "CDC Continuity Tests" cargo test -p cassandra-diff-tests cdc_tests -- --nocapture

# 5. Rollback tests
run_check "Rollback Tests" cargo test -p cassandra-diff-tests rollback_tests -- --nocapture

# 6. Existing backup/restore tests
run_check "Backup/Restore Tests" cargo test -p cassandra-storage backup -- --nocapture

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════════════════════"

if [ $FAIL -gt 0 ]; then
    echo "❌ Migration validation FAILED"
    exit 1
else
    echo "✅ Migration validation PASSED"
    exit 0
fi
