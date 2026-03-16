# Runbook: Migration Procedure (Java → Rust)

**Audience**: Operations/DBA teams
**Estimated Duration**: 2-4 hours active + 1-2 weeks shadow bake

## Pre-Requisites

- [ ] Rust Cassandra binary or Docker image available
- [ ] Hardware provisioned for shadow Rust cluster (same sizing as Java)
- [ ] Network connectivity between clusters for tooling
- [ ] Java cluster running and healthy (`nodetool status` shows all UN)
- [ ] Backup storage available (S3, NFS, etc.)

## Pre-Checks

### 1. Verify Upgrade Path

```bash
cargo run -p cassandra-migration -- check-upgrade \
  --source-version $(nodetool version) \
  --target-version 0.1.0-rust
```

Expected: `FullySupported` or `RequiresConversion`.
**ABORT** if: `Unsupported`.

### 2. Schema Export and Validation

```bash
# Export Java schema
cqlsh java-host -e "DESCRIBE SCHEMA" > schema_java.cql

# Verify no unsupported features
grep -c "TRIGGER\|FUNCTION\|AGGREGATE" schema_java.cql
```

> [!WARNING]
> UDFs, UDAs, and Triggers require feature-flag validation in Rust.

### 3. Disk Space Check

Ensure target cluster has ≥ 1.5× the data size of source:

```bash
nodetool tablestats --human-readable | grep "Space used (total)"
```

### 4. Take Pre-Migration Backup

```bash
nodetool snapshot -t pre-migration-$(date +%Y%m%d)
# Copy snapshots to remote storage
```

## Migration Steps

### Step 1: Deploy Rust Cluster

```bash
cd rust
docker compose -f docker-compose.prod.yml up -d
# OR: systemd deployment
sudo cp target/release/cassandra-server /usr/local/bin/
sudo systemctl start cassandra-rust
```

### Step 2: Apply Schema

```bash
cqlsh rust-host -f schema_java.cql
```

### Step 3: Import Data

```bash
cargo run -p cassandra-migration -- sstable-import \
  --source /path/to/java/data \
  --target /path/to/rust/data \
  --validate-checksums \
  --keyspace my_keyspace
```

### Step 4: Enable Shadow Traffic

```bash
# On Java cluster
nodetool enablefullquerylog --path /var/log/cassandra/fql

# Convert and replay
cargo run -p cassandra-migration -- fql-convert \
  --source /var/log/cassandra/fql \
  --target /tmp/fql.json

cargo run -p cassandra-diff-tests -- shadow-replay \
  --fql-path /tmp/fql.json \
  --rust-host rust-host:9042
```

### Step 5: Validate

```bash
# Row count comparison
cargo run -p cassandra-migration -- validate \
  --tables ks.users,ks.orders

# CDC continuity
cargo run -p cassandra-migration -- cdc-check \
  --java-cdc-dir /var/lib/cassandra/cdc_raw \
  --rust-cdc-dir /var/lib/cassandra-rust/cdc_raw
```

### Step 6: Bake Period (1-2 weeks)

Monitor daily:
- Shadow divergence rate → should be 0%
- Rust cluster error rate → should be 0%
- Rust p99 latency → should be ≤ 1.5× Java

### Step 7: Cutover Decision

All these must be TRUE:
- [ ] ≥ 1 week shadow with 0% divergence
- [ ] Performance within SLO
- [ ] Rollback tested (run drill)
- [ ] Team sign-off

### Step 8: Switch Traffic

```bash
# Update DNS/LB to point to Rust cluster
# Keep Java cluster warm for 72 hours minimum
```

## Post-Checks

- [ ] Error rate stable for 1 hour
- [ ] Latency within budget for 1 hour
- [ ] Spot-check 10 queries manually
- [ ] CDC consumers receiving data
- [ ] Metrics flowing to monitoring

## Abort Criteria

**Immediately rollback if**:
- Error rate > 0.1% for > 5 minutes
- p99 latency > 2× Java baseline for > 10 minutes
- Any data loss or corruption detected
- CDC gap detected

See [Rollback Procedure](rollback-procedure.md).
