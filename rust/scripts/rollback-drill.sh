#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# Rollback drill: validates the snapshot-based rollback procedure.
#
# Sequence:
# 1. Write initial data → flush → snapshot
# 2. Write post-migration data
# 3. Simulate rollback: remove data, restore from snapshot
# 4. Verify only pre-snapshot data exists
#
# Usage:
#   ./scripts/rollback-drill.sh [data-dir]
#
# Example:
#   ./scripts/rollback-drill.sh /var/lib/cassandra

set -euo pipefail

DATA_DIR="${1:-/tmp/cassandra-rollback-drill}"
SNAP_NAME="rollback-drill-$(date +%s)"

echo "=== Rollback Drill ==="
echo "  Data directory: ${DATA_DIR}"
echo "  Snapshot name:  ${SNAP_NAME}"

# Cleanup previous drill
rm -rf "${DATA_DIR}"

echo ""
echo "── Step 1: Start engine and write initial data ──"
echo "  (This step uses the Rust test harness)"
echo "  Run: cargo test -p cassandra-diff-tests --test backup_restore_tests rollback_drill -- --nocapture"
echo ""

# Run the Rust rollback drill test
cd "$(dirname "$0")/.."
cargo test -p cassandra-diff-tests --test backup_restore_tests rollback_drill -- --nocapture

echo ""
echo "── Step 2: Verify results ──"
RESULT=$?
if [ $RESULT -eq 0 ]; then
    echo "✅ Rollback drill PASSED"
    echo "  - Snapshot created successfully"
    echo "  - Post-snapshot data written"
    echo "  - Rollback point preserved"
    echo ""
    echo "  For production rollback:"
    echo "  1. Stop Cassandra Rust:  systemctl stop cassandra-rust"
    echo "  2. Remove data:          rm -rf /var/lib/cassandra/data/*"
    echo "  3. Restore snapshot:     cp -a /var/lib/cassandra/data/snapshots/<name>/* /var/lib/cassandra/data/"
    echo "  4. Start Cassandra Rust: systemctl start cassandra-rust"
    echo "  5. Verify data:          cassandra-tools status"
else
    echo "❌ Rollback drill FAILED"
    echo "  Check test output above for details."
    exit 1
fi
