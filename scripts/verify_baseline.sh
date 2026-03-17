#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
# See the NOTICE file and LICENSE.txt at the repository root.
#
# Verify the baseline freeze is consistent across all documents.
# Exit 0 if all checks pass, 1 if any mismatch is found.

set -euo pipefail

EXPECTED_COMMIT="076c6f11364645bbb43360f013bee6f50a099185"
EXPECTED_DATE="2026-03-15"
EXPECTED_BRANCH="trunk"
EXPECTED_TAG="cassandra-rewrite-baseline-v1"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ERRORS=0

check() {
    local desc="$1" ok="$2"
    if [ "$ok" = "true" ]; then
        echo "  PASS  $desc"
    else
        echo "  FAIL  $desc"
        ERRORS=$((ERRORS + 1))
    fi
}

echo "============================================="
echo "  Baseline Freeze Verification"
echo "============================================="
echo ""
echo "Expected:"
echo "  Commit : $EXPECTED_COMMIT"
echo "  Date   : $EXPECTED_DATE"
echo "  Branch : $EXPECTED_BRANCH"
echo "  Tag    : $EXPECTED_TAG"
echo ""

# 1. Check gap matrix YAML
echo "-- Gap Matrix (final_gap_matrix.yaml) --"
MATRIX="$REPO_ROOT/docs/rewrite/final_gap_matrix.yaml"
if [ -f "$MATRIX" ]; then
    MATRIX_COMMIT=$(python3 -c "
import yaml
with open('$MATRIX') as f:
    data = yaml.safe_load(f)
print(data['baseline']['primary']['commit'])
" 2>/dev/null || echo "PARSE_ERROR")
    MATRIX_DATE=$(python3 -c "
import yaml
with open('$MATRIX') as f:
    data = yaml.safe_load(f)
print(data['baseline']['primary']['date'])
" 2>/dev/null || echo "PARSE_ERROR")
    check "Commit in YAML" "$([ "$MATRIX_COMMIT" = "$EXPECTED_COMMIT" ] && echo true || echo false)"
    check "Date in YAML"   "$([ "$MATRIX_DATE" = "$EXPECTED_DATE" ] && echo true || echo false)"
else
    check "YAML file exists" "false"
fi
echo ""

# 2. Check baseline_freeze.md
echo "-- Baseline Freeze Doc --"
FREEZE="$REPO_ROOT/docs/rewrite/baseline_freeze.md"
if [ -f "$FREEZE" ]; then
    check "Commit in freeze doc" "$(grep -q "$EXPECTED_COMMIT" "$FREEZE" && echo true || echo false)"
    check "Date in freeze doc"   "$(grep -q "$EXPECTED_DATE" "$FREEZE" && echo true || echo false)"
    check "Tag in freeze doc"    "$(grep -q "$EXPECTED_TAG" "$FREEZE" && echo true || echo false)"
else
    check "Freeze doc exists" "false"
fi
echo ""

# 3. Check charter.md
echo "-- Charter --"
CHARTER="$REPO_ROOT/docs/rewrite/charter.md"
if [ -f "$CHARTER" ]; then
    check "Commit in charter" "$(grep -q "$EXPECTED_COMMIT" "$CHARTER" && echo true || echo false)"
else
    check "Charter exists" "false"
fi
echo ""

# 4. Check ADR-016
echo "-- ADR-016 (Baseline Freeze Policy) --"
ADR016="$REPO_ROOT/docs/rewrite/adrs/016-baseline-freeze-policy.md"
if [ -f "$ADR016" ]; then
    check "Commit in ADR-016" "$(grep -q "$EXPECTED_COMMIT" "$ADR016" && echo true || echo false)"
else
    check "ADR-016 exists" "false"
fi
echo ""

# 5. YAML validity
echo "-- YAML Validity --"
YAML_VALID=$(python3 -c "
import yaml
yaml.safe_load(open('$MATRIX'))
print('true')
" 2>/dev/null || echo "false")
check "YAML parses without error" "$YAML_VALID"
echo ""

# Summary
echo "============================================="
if [ "$ERRORS" -eq 0 ]; then
    echo "  All baseline checks PASSED"
    echo "============================================="
    exit 0
else
    echo "  $ERRORS check(s) FAILED"
    echo "============================================="
    exit 1
fi
