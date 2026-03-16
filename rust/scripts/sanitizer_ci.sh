#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
#
# Run Rust tests under sanitizers and Miri for memory safety validation.
#
# Usage:
#   bash scripts/sanitizer_ci.sh [--asan] [--miri] [--all]
#
# Requires nightly Rust toolchain.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass_count=0
fail_count=0
skip_count=0

run_asan() {
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Address Sanitizer (ASan)"
    echo "═══════════════════════════════════════════════════════════"
    echo ""

    if ! rustup run nightly rustc --version &>/dev/null; then
        echo -e "${YELLOW}⚠️  Nightly Rust not installed. Install with: rustup toolchain install nightly${NC}"
        echo "   Skipping ASan tests."
        ((skip_count++))
        return
    fi

    # ASan is only supported on x86_64-unknown-linux-gnu and aarch64-apple-darwin
    local target
    target=$(rustc -vV | grep host | awk '{print $2}')

    echo "  Target: $target"
    echo "  Testing crates with ASan enabled..."
    echo ""

    # Test a subset of crates that are most likely to have memory issues
    local crates=("cassandra-common" "cassandra-types" "cassandra-native-protocol" "cassandra-storage")

    for crate in "${crates[@]}"; do
        echo "  Testing $crate..."
        if RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -p "$crate" \
            --target "$target" \
            -- --test-threads=1 2>&1 | tail -5; then
            echo -e "  ${GREEN}✅ $crate — ASan clean${NC}"
            ((pass_count++))
        else
            echo -e "  ${RED}❌ $crate — ASan found issues${NC}"
            ((fail_count++))
        fi
        echo ""
    done
}

run_miri() {
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Miri (Undefined Behavior Detector)"
    echo "═══════════════════════════════════════════════════════════"
    echo ""

    if ! cargo +nightly miri --version &>/dev/null; then
        echo -e "${YELLOW}⚠️  Miri not installed. Install with: rustup +nightly component add miri${NC}"
        echo "   Skipping Miri tests."
        ((skip_count++))
        return
    fi

    # Miri is slow — test only core crates
    local crates=("cassandra-common" "cassandra-types")

    for crate in "${crates[@]}"; do
        echo "  Testing $crate with Miri..."
        if cargo +nightly miri test -p "$crate" -- --test-threads=1 2>&1 | tail -5; then
            echo -e "  ${GREEN}✅ $crate — Miri clean${NC}"
            ((pass_count++))
        else
            echo -e "  ${RED}❌ $crate — Miri found UB${NC}"
            ((fail_count++))
        fi
        echo ""
    done
}

print_summary() {
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Sanitizer Summary"
    echo "═══════════════════════════════════════════════════════════"
    echo -e "  ${GREEN}Passed: $pass_count${NC}"
    echo -e "  ${RED}Failed: $fail_count${NC}"
    echo -e "  ${YELLOW}Skipped: $skip_count${NC}"
    echo "═══════════════════════════════════════════════════════════"
    echo ""

    if [ "$fail_count" -gt 0 ]; then
        exit 1
    fi
}

# Parse arguments
MODE="${1:---all}"

case "$MODE" in
    --asan)
        run_asan
        ;;
    --miri)
        run_miri
        ;;
    --all)
        run_asan
        run_miri
        ;;
    *)
        echo "Usage: $0 [--asan] [--miri] [--all]"
        exit 1
        ;;
esac

print_summary
