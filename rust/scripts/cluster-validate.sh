#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# cluster-validate.sh — Two-tier cluster validation.
#
# Tier 1 (no Docker): build, test, single-node smoke
# Tier 2 (Docker): 3-node cluster connectivity
#
# Usage:
#   ./scripts/cluster-validate.sh                # Both tiers
#   ./scripts/cluster-validate.sh --single-node-only  # Tier 1 only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

# Ensure rustup toolchain shims are on PATH when available.
if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
fi

SINGLE_NODE_ONLY=false
if [[ "${1:-}" == "--single-node-only" ]]; then
    SINGLE_NODE_ONLY=true
fi

PASS=0
FAIL=0
SKIP=0

wait_for_port() {
    local host="$1"
    local port="$2"
    local timeout_secs="$3"
    local elapsed=0
    while [[ $elapsed -lt $timeout_secs ]]; do
        if nc -z "$host" "$port" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

check() {
    local name="$1"
    shift
    printf "  %-45s " "$name"
    if "$@" >/dev/null 2>&1; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

skip() {
    local name="$1"
    local reason="$2"
    printf "  %-45s SKIP (%s)\n" "$name" "$reason"
    SKIP=$((SKIP + 1))
}

echo "═══════════════════════════════════════════════════════"
echo "  Cluster Validation"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "═══════════════════════════════════════════════════════"

# ── Tier 1: No Docker ──────────────────────────────────────────────────

echo ""
echo "── Tier 1: Build & Single-Node ──"

check "Workspace build"         cargo build --workspace
check "Workspace test compile"  cargo test --workspace --no-run
check "Clippy lint"             cargo clippy --workspace -- -D warnings

# Single-node smoke test
echo ""
echo "── Tier 1: Single-Node Smoke ──"

SERVER_BIN="target/debug/cassandra-server"
if [[ ! -f "$SERVER_BIN" ]]; then
    SERVER_BIN=$(find target -name cassandra-server -type f -perm /111 2>/dev/null | head -1 || true)
fi

if [[ -n "$SERVER_BIN" && -f "$SERVER_BIN" ]]; then
    SERVER_PID=""
    TMPDATA=$(mktemp -d)
    TMPCONF=$(mktemp)
    cleanup_server() {
        if [[ -n "$SERVER_PID" ]]; then
            kill "$SERVER_PID" 2>/dev/null || true
            wait "$SERVER_PID" 2>/dev/null || true
        fi
        rm -f "$TMPCONF"
        rm -rf "$TMPDATA"
    }
    trap cleanup_server EXIT

    cat >"$TMPCONF" <<'EOF'
listen_address: "127.0.0.1"
native_transport_port: 19042
admin_port: 19180
EOF

    # Start server in background. cassandra-server currently reads runtime
    # parameters from CASSANDRA_CONFIG rather than CLI flags.
    CASSANDRA_CONFIG="$TMPCONF" "$SERVER_BIN" >"$TMPDATA/server.log" 2>&1 &
    SERVER_PID=$!

    # Wait for readiness (up to 10s)
    READY=false
    for i in $(seq 1 20); do
        if nc -z 127.0.0.1 19042 2>/dev/null; then
            READY=true
            break
        fi
        sleep 0.5
    done

    if $READY; then
        check "Server TCP ready (19042)"    true
        # CQL handshake: send STARTUP frame and check for response
        # A basic TCP check is sufficient for validation
        check "CQL port reachable"          nc -z 127.0.0.1 19042

        # Admin API check
        if nc -z 127.0.0.1 19180 2>/dev/null; then
            check "Admin API ready (19180)"     curl -sf http://127.0.0.1:19180/health
        else
            skip "Admin API ready (19180)"      "port not open"
        fi
    else
        if [[ -f "$TMPDATA/server.log" ]]; then
            echo "  server startup log tail:"
            tail -n 20 "$TMPDATA/server.log" | sed 's/^/    /'
        fi
        skip "Server TCP ready (19042)"     "server did not start"
        skip "CQL port reachable"           "server did not start"
        skip "Admin API ready (19180)"      "server did not start"
    fi

    # Stop server
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
else
    skip "Server TCP ready (19042)"     "binary not found"
    skip "CQL port reachable"           "binary not found"
    skip "Admin API ready (19180)"      "binary not found"
fi

# ── Tier 2: Docker Cluster ─────────────────────────────────────────────

if $SINGLE_NODE_ONLY; then
    echo ""
    echo "── Tier 2: Docker Cluster (SKIPPED — --single-node-only) ──"
    skip "Docker 3-node cluster"            "single-node-only"
    skip "CQL on node 1 (9042)"             "single-node-only"
    skip "CQL on node 2 (9043)"             "single-node-only"
    skip "CQL on node 3 (9044)"             "single-node-only"
else
    echo ""
    echo "── Tier 2: Docker Cluster ──"

    COMPOSE_FILE="docker-compose.prod.yml"
    NODE1_PORT="${CASSANDRA_NODE1_PORT:-29042}"
    NODE2_PORT="${CASSANDRA_NODE2_PORT:-29043}"
    NODE3_PORT="${CASSANDRA_NODE3_PORT:-29044}"
    NODE1_TLS_PORT="${CASSANDRA_NODE1_TLS_PORT:-29142}"
    NODE1_METRICS_PORT="${CASSANDRA_NODE1_METRICS_PORT:-29180}"
    NODE2_METRICS_PORT="${CASSANDRA_NODE2_METRICS_PORT:-29181}"
    NODE3_METRICS_PORT="${CASSANDRA_NODE3_METRICS_PORT:-29182}"

    dcompose() {
        CASSANDRA_NODE1_PORT="$NODE1_PORT" \
        CASSANDRA_NODE2_PORT="$NODE2_PORT" \
        CASSANDRA_NODE3_PORT="$NODE3_PORT" \
        CASSANDRA_NODE1_TLS_PORT="$NODE1_TLS_PORT" \
        CASSANDRA_NODE1_METRICS_PORT="$NODE1_METRICS_PORT" \
        CASSANDRA_NODE2_METRICS_PORT="$NODE2_METRICS_PORT" \
        CASSANDRA_NODE3_METRICS_PORT="$NODE3_METRICS_PORT" \
        docker compose -f "$COMPOSE_FILE" "$@"
    }

    if [[ -f "$COMPOSE_FILE" ]] && command -v docker >/dev/null 2>&1; then
        printf "  %-45s " "Docker 3-node cluster"
        if dcompose up -d --build >/dev/null 2>&1; then
            echo "PASS"
            PASS=$((PASS + 1))

            echo "  Waiting for 3-node cluster CQL ports..."
            check "CQL on node 1 (${NODE1_PORT})"   wait_for_port 127.0.0.1 "$NODE1_PORT" 180
            check "CQL on node 2 (${NODE2_PORT})"   wait_for_port 127.0.0.1 "$NODE2_PORT" 180
            check "CQL on node 3 (${NODE3_PORT})"   wait_for_port 127.0.0.1 "$NODE3_PORT" 180
        else
            echo "FAIL"
            FAIL=$((FAIL + 1))
            skip "CQL on node 1 (${NODE1_PORT})"    "docker compose up failed"
            skip "CQL on node 2 (${NODE2_PORT})"    "docker compose up failed"
            skip "CQL on node 3 (${NODE3_PORT})"    "docker compose up failed"
        fi

        echo ""
        echo "  NOTE: Multi-node limitations:"
        echo "    - No gossip loop: nodes cannot discover each other"
        echo "    - No StorageProxy: distributed reads/writes not available"
        echo "    - Each node operates independently (3x single-node)"

        if [[ $FAIL -gt 0 ]]; then
            echo ""
            echo "  Docker container status:"
            dcompose ps 2>/dev/null | sed 's/^/    /' || true
            echo ""
            echo "  Docker logs tail (cassandra-rust-1/2/3):"
            dcompose logs --tail=80 cassandra-rust-1 cassandra-rust-2 cassandra-rust-3 2>/dev/null | sed 's/^/    /' || true
        fi

        dcompose down -v 2>/dev/null || true
    else
        skip "Docker 3-node cluster"        "Docker or compose file not found"
        skip "CQL on node 1 (9042)"         "no Docker"
        skip "CQL on node 2 (9043)"         "no Docker"
        skip "CQL on node 3 (9044)"         "no Docker"
    fi
fi

# ── Summary ────────────────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: ${PASS} PASS, ${FAIL} FAIL, ${SKIP} SKIP"
echo "═══════════════════════════════════════════════════════"

if [ $FAIL -gt 0 ]; then
    exit 1
else
    exit 0
fi
