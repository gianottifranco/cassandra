# Rollback Procedure

**Purpose**: Step-by-step guide for reverting from Cassandra Rust to Java during or after migration.

## When to Rollback

- Error rates exceed 0.1% above Java baseline
- p99 latency exceeds 2× Java baseline
- Data divergence detected in shadow traffic
- Any data loss or corruption event
- CDC gap detected (missing segments)
- Schema divergence discovered

## Prerequisites

- Java cluster still running (or snapshot available)
- DNS/LB configuration access
- Operator access to both clusters
- CDC checkpoint file from migration (if applicable)

## Immediate Rollback (During Shadow Phase)

If the Rust cluster has not yet taken production traffic:

1. **Stop shadow traffic replay**:
   ```bash
   pkill -f "shadow-replay"
   ```

2. **No further action needed** — Java cluster is still serving all traffic.

## Rollback During Cutover

If traffic has been switched to Rust:

### Step 1: Switch Traffic Back

```bash
# Update DNS/LB to point back to Java cluster
# Method depends on infrastructure:
# - AWS Route 53: update A/CNAME record
# - HAProxy: update backend configuration
# - Kubernetes: update Service selector
```

### Step 2: Verify Java Cluster

```bash
nodetool status    # All nodes should be UN
nodetool info      # Verify cluster name/version
cqlsh java-host -e "SELECT * FROM system.local"   # Basic health
```

### Step 3: Stop Rust Cluster

```bash
# SystemD
sudo systemctl stop cassandra-rust

# Docker
docker compose -f docker-compose.prod.yml down
```

### Step 4: Restore CDC Continuity

```bash
# If CDC consumers were pointing to Rust, revert to Java
# 1. Update consumer config to Java CDC directory
# 2. Consumer handles from last-consumed offset (idempotent)

# Verify CDC state
ls /var/lib/cassandra/cdc_raw/ | wc -l   # Count Java CDC segments
```

### Step 5: Preserve Evidence

```bash
# Save Rust logs for analysis
mkdir -p /tmp/rollback-evidence
cp /var/log/cassandra-rust/*.log /tmp/rollback-evidence/

# Save validation report
cargo run -p cassandra-migration -- validate \
  --report /tmp/rollback-evidence/validation.json 2>&1 || true
```

## Rollback After Java Decommission

If Java cluster has been stopped:

### Step 1: Restore Java from Snapshot

```bash
sudo systemctl stop cassandra  # if running

# Restore data from pre-migration snapshot
sstableloader -d <java-host> /path/to/snapshot/keyspace/table/
```

### Step 2: Start Java Cluster

```bash
sudo systemctl start cassandra
nodetool status  # wait for UN on all nodes
```

### Step 3: Verify Data Integrity

```bash
nodetool verify
cqlsh java-host -e "SELECT count(*) FROM keyspace.table"
```

### Step 4: Restore CDC

```bash
# CDC segments from pre-migration are in the snapshot
# Consumers resume from their last checkpoint
```

## Automated Rollback Drill

Run before any real migration to validate the procedure:

```bash
cd rust

# Run the Rust rollback drill test
cargo test -p cassandra-diff-tests rollback_tests -- --nocapture

# Run the full rollback script
bash scripts/rollback-drill.sh

# Run full migration validation (includes rollback tests)
make validate-migration
```

## Post-Rollback Actions

1. **Incident report**: Document trigger, timeline, and resolution
2. **Root cause analysis**: Identify what caused the failure
3. **Fix**: Apply fix to Rust implementation
4. **Re-validate**: Run shadow traffic again with fix applied
5. **Retry**: Only after fix is verified and team signs off

## Contacts

| Role | Responsibility |
|------|---------------|
| On-call DBA | Execute rollback steps |
| Platform Engineer | DNS/LB changes |
| Cassandra Rust Owner | Root cause analysis |
| CDC Team | Consumer reconfiguration |
