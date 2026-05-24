#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# capture-evidence.sh — Capture test evidence for Phase 26 RC gate.
#
# Runs each test suite and captures output to evidence/phase-26/.
#
# Usage:
#   ./scripts/capture-evidence.sh
#   ./scripts/capture-evidence.sh --strict-perf
#   ./scripts/capture-evidence.sh --strict-perf --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

# Ensure rustup toolchain shims are on PATH when available.
if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
fi

EVIDENCE_DIR="evidence/phase-26"
mkdir -p "$EVIDENCE_DIR"

STRICT_PERF=false
DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --strict-perf)
            STRICT_PERF=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--strict-perf] [--dry-run]"
            exit 2
            ;;
    esac
done

PASS=0
FAIL=0
DRY=0
TOTAL=0
SUITE_NAMES=()
SUITE_LOGS=()
SUITE_STATUS=()
SUITE_TEST_COUNTS=()
SUITE_COMMANDS=()
SUITE_EXIT_CODES=()
SUITE_DURATIONS=()

json_escape() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\n'/\\n}"
    printf '%s' "$value"
}

capture() {
    local name="$1"
    local log_file="$2"
    shift 2
    local cmd_str
    printf -v cmd_str '%q ' "$@"
    cmd_str="${cmd_str% }"
    TOTAL=$((TOTAL + 1))
    printf "  %-35s " "$name"

    if $DRY_RUN; then
        echo "DRY-RUN"
        SUITE_NAMES+=("$name")
        SUITE_LOGS+=("$log_file")
        SUITE_STATUS+=("dry-run")
        SUITE_TEST_COUNTS+=("0")
        SUITE_COMMANDS+=("$cmd_str")
        SUITE_EXIT_CODES+=("0")
        SUITE_DURATIONS+=("0.000")
        DRY=$((DRY + 1))
    else
        local start_ns end_ns elapsed_sec count status_text exit_code
        start_ns=$(date +%s%N)
        set +e
        "$@" > "$EVIDENCE_DIR/$log_file" 2>&1
        exit_code=$?
        set -e
        end_ns=$(date +%s%N)
        elapsed_sec=$(awk "BEGIN { printf \"%.3f\", ($end_ns - $start_ns) / 1000000000 }")
        count=$(grep -c "test .* ok" "$EVIDENCE_DIR/$log_file" 2>/dev/null || echo "0")

        if [ "$exit_code" -eq 0 ]; then
            status_text="pass"
            echo "PASS (${count} tests, ${elapsed_sec}s)"
            PASS=$((PASS + 1))
        else
            status_text="fail"
            echo "FAIL (${elapsed_sec}s, see $EVIDENCE_DIR/$log_file)"
            FAIL=$((FAIL + 1))
        fi

        SUITE_NAMES+=("$name")
        SUITE_LOGS+=("$log_file")
        SUITE_STATUS+=("$status_text")
        SUITE_TEST_COUNTS+=("$count")
        SUITE_COMMANDS+=("$cmd_str")
        SUITE_EXIT_CODES+=("$exit_code")
        SUITE_DURATIONS+=("$elapsed_sec")
    fi
}

echo "═══════════════════════════════════════════════════════"
echo "  Evidence Capture — Phase 26"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "  Output: ${EVIDENCE_DIR}/"
echo "  Perf mode: $(if $STRICT_PERF; then echo "STRICT"; else echo "ADVISORY"; fi)"
echo "  Run mode: $(if $DRY_RUN; then echo "DRY-RUN"; else echo "EXECUTE"; fi)"
echo "═══════════════════════════════════════════════════════"
echo ""

# Chaos tests
capture "Chaos Tests" \
    "chaos-tests.log" \
    cargo test -p cassandra-diff-tests --test chaos_tests -- --nocapture

# Soak tests (long-running suite)
capture "Soak Tests (30s)" \
    "soak-tests.log" \
    cargo test -p cassandra-diff-tests --test soak_tests -- --nocapture

# Performance budget tests
if $STRICT_PERF; then
    capture "Performance Budgets (strict)" \
        "perf-budget.log" \
        env CASSANDRA_STRICT_PERF_BUDGET=1 cargo test -p cassandra-diff-tests --test perf_budget_tests -- --nocapture
else
    capture "Performance Budgets" \
        "perf-budget.log" \
        cargo test -p cassandra-diff-tests --test perf_budget_tests -- --nocapture
fi

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
printf "  %-25s %s\n" "Dry-run:" "$DRY"
echo ""
echo "  Evidence directory: ${EVIDENCE_DIR}/"
echo "  Files:"
if $DRY_RUN; then
    echo "    (none; dry-run mode)"
else
    ls -1 "$EVIDENCE_DIR/" | sed 's/^/    /'
fi
echo ""

SUMMARY_PATH="$EVIDENCE_DIR/summary.json"
{
    echo "{"
    echo "  \"captured_at_utc\": \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\","
    echo "  \"strict_perf_budget\": $STRICT_PERF,"
    echo "  \"dry_run\": $DRY_RUN,"
    echo "  \"total_suites\": $TOTAL,"
    echo "  \"passed_suites\": $PASS,"
    echo "  \"failed_suites\": $FAIL,"
    echo "  \"dry_run_suites\": $DRY,"
    echo "  \"suites\": ["
    for i in "${!SUITE_NAMES[@]}"; do
        comma=","
        if [[ "$i" -eq $((${#SUITE_NAMES[@]} - 1)) ]]; then
            comma=""
        fi
        echo "    {"
        echo "      \"name\": \"$(json_escape "${SUITE_NAMES[$i]}")\","
        echo "      \"log_file\": \"$(json_escape "${SUITE_LOGS[$i]}")\","
        echo "      \"status\": \"$(json_escape "${SUITE_STATUS[$i]}")\","
        echo "      \"test_count\": ${SUITE_TEST_COUNTS[$i]},"
        echo "      \"exit_code\": ${SUITE_EXIT_CODES[$i]},"
        echo "      \"duration_seconds\": ${SUITE_DURATIONS[$i]},"
        echo "      \"command\": \"$(json_escape "${SUITE_COMMANDS[$i]}")\""
        echo "    }$comma"
    done
    echo "  ]"
    echo "}"
} >"$SUMMARY_PATH"

echo "  Summary JSON: ${SUMMARY_PATH}"
echo ""

if [ $FAIL -gt 0 ]; then
    echo "  ⚠ Some suites failed — review logs before RC gate"
    exit 1
else
    echo "  All suites passed"
    exit 0
fi
