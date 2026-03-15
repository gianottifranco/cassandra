#!/usr/bin/env bash
# Licensed under Apache License, Version 2.0.
# Release build script for Cassandra Rust.
#
# Produces a release tarball with versioned binaries and checksums.
#
# Usage:
#   ./scripts/build-release.sh [version]
#
# Example:
#   ./scripts/build-release.sh 0.1.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(dirname "$SCRIPT_DIR")"
VERSION="${1:-$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['packages'][0]['version'])" 2>/dev/null || echo "0.1.0")}"

RELEASE_DIR="${WORKSPACE_DIR}/release"
TARBALL_NAME="cassandra-rust-${VERSION}"
STAGING_DIR="${RELEASE_DIR}/${TARBALL_NAME}"

echo "=== Building Cassandra Rust v${VERSION} ==="
echo "  Workspace: ${WORKSPACE_DIR}"
echo "  Output: ${RELEASE_DIR}/${TARBALL_NAME}.tar.gz"

# 1. Build release binaries
echo ""
echo "── Step 1: Building release binaries ──"
cd "$WORKSPACE_DIR"
cargo build --release -p cassandra-server -p cassandra-tools

# 2. Create staging directory
echo ""
echo "── Step 2: Staging release artifacts ──"
rm -rf "$STAGING_DIR"
mkdir -p "${STAGING_DIR}/bin"
mkdir -p "${STAGING_DIR}/conf"
mkdir -p "${STAGING_DIR}/deploy"
mkdir -p "${STAGING_DIR}/docs"

# 3. Copy binaries
cp target/release/cassandra-server "${STAGING_DIR}/bin/"
cp target/release/cassandra-tools "${STAGING_DIR}/bin/"

# 4. Copy deployment files
cp deploy/cassandra-rust.service "${STAGING_DIR}/deploy/" 2>/dev/null || true
cp deploy/prometheus.yml "${STAGING_DIR}/deploy/" 2>/dev/null || true
cp Dockerfile "${STAGING_DIR}/" 2>/dev/null || true
cp docker-compose.prod.yml "${STAGING_DIR}/" 2>/dev/null || true

# 5. Copy documentation
cp -r ../docs/rewrite/* "${STAGING_DIR}/docs/" 2>/dev/null || true

# 6. Write version file
cat > "${STAGING_DIR}/VERSION" <<EOF
Cassandra Rust v${VERSION}
Build date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')
Git commit: $(git rev-parse HEAD 2>/dev/null || echo "unknown")
Rust version: $(rustc --version 2>/dev/null || echo "unknown")
EOF

# 7. Create tarball
echo ""
echo "── Step 3: Creating tarball ──"
cd "$RELEASE_DIR"
tar czf "${TARBALL_NAME}.tar.gz" "${TARBALL_NAME}/"

# 8. Checksums
echo ""
echo "── Step 4: Generating checksums ──"
if command -v sha256sum &>/dev/null; then
    sha256sum "${TARBALL_NAME}.tar.gz" > "${TARBALL_NAME}.tar.gz.sha256"
elif command -v shasum &>/dev/null; then
    shasum -a 256 "${TARBALL_NAME}.tar.gz" > "${TARBALL_NAME}.tar.gz.sha256"
fi

# 9. Summary
echo ""
echo "=== Release build complete ==="
echo "  Tarball: ${RELEASE_DIR}/${TARBALL_NAME}.tar.gz"
if [ -f "${TARBALL_NAME}.tar.gz.sha256" ]; then
    echo "  SHA256:  $(cat "${TARBALL_NAME}.tar.gz.sha256")"
fi
echo "  Size:    $(du -h "${TARBALL_NAME}.tar.gz" | cut -f1)"
