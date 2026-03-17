#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# capture-evidence.sh — Capture test evidence for Phase 26 RC gate.
#
# Runs each test suite and captures output to evidence/phase-26/.
#
# Usage:
#   ./scripts/capture-evidence.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

EVIDENCE_DIR="evidence/phase-26"
mkdir -p "$EVIDENCE_DIR"

PASS=0
FAIL=0
TOTAL=0

capture() {
    local name="$1"
    local log_file="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    printf "  %-35s " "$name"

    if "$@" > "$EVIDENCE_DIR/$log_file" 2>&1; then
        local count
        count=$(grep -c "test .* ok" "$EVIDENCE_DIR/$log_file" 2>/dev/null || echo "0")
        echo "PASS (${count} tests)"
        PASS=$((PASS + 1))
    else
        echo "FAIL (see $EVIDENCE_DIR/$log_file)"
        FAIL=$((FAIL + 1))
    fi
}

echo "═══════════════════════════════════════════════════════"
echo "  Evidence Capture — Phase 26"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "  Output: ${EVIDENCE_DIR}/"
echo "═══════════════════════════════════════════════════════"
echo ""

# Chaos tests
capture "Chaos Tests" \
    "chaos-tests.log" \
    cargo test -p cassandra-diff-tests --test chaos_tests -- --nocapture

# Soak tests (30s timeout, ignored tests)
capture "Soak Tests (30s)" \
    "soak-tests.log" \
    cargo test -p cassandra-diff-tests --test soak_tests -- --ignored --nocapture

# Performance budget tests
capture "Performance Budgets" \
    "perf-budget.log" \
    cargo test -p cassandra-diff-tests --test perf_budget_tests -- --ignored --nocapture

# Fuzz tests
capture "Fuzz Tests" \
    "fuzz-tests.log" \
    cargo test -p cassandra-diff-tests --test fuzz_tests -- --nocapture

# Security audit
capture "Security Audit" \
    "security-audit.log" \
    cargo test -p cassandra-diff-tests --test security_audit -- --nocapture

# Backup/restore tests
capture "Backup/Restore Tests" \
    "backup-restore.log" \
    cargo test -p cassandra-diff-tests --test backup_restore_tests -- --nocapture

# Tooling tests (nodetool, admin)
capture "Tooling Tests" \
    "tooling.log" \
    cargo test -p cassandra-tools -p cassandra-admin -- --nocapture

# Gap guards
capture "Gap Guards" \
    "gap-guards.log" \
    cargo test -p cassandra-diff-tests gap_guard -- --nocapture

# Golden tests
capture "Golden Tests" \
    "golden.log" \
    cargo test -p cassandra-diff-tests golden -- --nocapture

# Migration validation
capture "Migration Tests" \
    "migration-validation.log" \
    cargo test -p cassandra-migration -- --nocapture

# Rollback tests
capture "Rollback Tests" \
    "rollback.log" \
    cargo test -p cassandra-diff-tests --lib rollback_tests -- --nocapture

# CDC tests
capture "CDC Tests" \
    "cdc.log" \
    cargo test -p cassandra-diff-tests --lib cdc_tests -- --nocapture

# Workspace tests (unsafe audit, structure)
capture "Workspace Tests" \
    "workspace.log" \
    cargo test -p cassandra-common --test workspace_tests -- --nocapture

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Evidence Summary"
echo "═══════════════════════════════════════════════════════"
echo ""
printf "  %-25s %s\n" "Total suites:" "$TOTAL"
printf "  %-25s %s\n" "Passed:" "$PASS"
printf "  %-25s %s\n" "Failed:" "$FAIL"
echo ""
echo "  Evidence directory: ${EVIDENCE_DIR}/"
echo "  Files:"
ls -1 "$EVIDENCE_DIR/" | sed 's/^/    /'
echo ""

if [ $FAIL -gt 0 ]; then
    echo "  ⚠ Some suites failed — review logs before RC gate"
    exit 1
else
    echo "  All suites passed"
    exit 0
fi
