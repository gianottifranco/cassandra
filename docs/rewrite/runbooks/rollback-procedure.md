# Rollback Procedure

**Purpose**: Step-by-step guide for reverting from Cassandra Rust to Java during or after migration.

## When to Rollback

- Error rates exceed 0.1% above Java baseline
- p99 latency exceeds 2× Java baseline
- Data divergence detected in shadow traffic
- Any data loss or corruption event

## Prerequisites

- Java cluster still running (or snapshot available)
- DNS/LB configuration access
- Operator access to both clusters

## Immediate Rollback (During Shadow Phase)

If the Rust cluster has not yet taken production traffic:

1. **Stop shadow traffic replay**:
   ```bash
   # Kill the shadow replay process
   pkill -f "shadow-replay"
   ```

2. **No further action needed** — Java cluster is still serving all traffic.

## Rollback During Cutover

If traffic has been switched to Rust:

### Step 1: Switch Traffic Back

```bash
# Update DNS/LB to point back to Java cluster
# Method depends on your infrastructure:
# - AWS Route 53: update A/CNAME record
# - HAProxy: update backend configuration
# - Kubernetes: update Service selector
```

### Step 2: Verify Java Cluster

```bash
# Check Java cluster health
nodetool status
nodetool info

# Verify a sample query
cqlsh java-host -e "SELECT * FROM system.local"
```

### Step 3: Stop Rust Cluster

```bash
# SystemD
sudo systemctl stop cassandra-rust

# Docker
docker compose -f docker-compose.prod.yml down
```

### Step 4: Preserve Evidence

```bash
# Save Rust logs for analysis
cp /var/log/cassandra/*.log /tmp/rollback-evidence/

# Save Rust data snapshot
cassandra-tools snapshot rollback-evidence
```

## Rollback After Decommission

If Java cluster has been stopped:

### Step 1: Restore Java from Snapshot

```bash
# Restore Java data from pre-migration snapshot
sudo systemctl stop cassandra  # if running

# Restore snapshot
sstableloader -d <java-host> /path/to/snapshot/keyspace/table/
```

### Step 2: Start Java Cluster

```bash
sudo systemctl start cassandra
nodetool status  # wait for UN on all nodes
```

### Step 3: Verify Data

```bash
# Run consistency checks
nodetool verify

# Spot-check data
cqlsh java-host -e "SELECT count(*) FROM keyspace.table"
```

## Post-Rollback Actions

1. **Incident report**: Document the trigger, timeline, and resolution
2. **Root cause analysis**: Identify what caused the failure
3. **Test fix**: Apply fix to Rust implementation
4. **Re-validate**: Run shadow traffic again with the fix
5. **Retry migration**: Only after the fix is verified

## Automated Rollback Drill

Run the automated rollback validation:

```bash
cd rust
bash scripts/rollback-drill.sh
# Or via cargo:
cargo test -p cassandra-diff-tests --test backup_restore_tests -- --nocapture
```

## Contacts

| Role | Responsibility |
|------|---------------|
| On-call DBA | Execute rollback steps |
| Platform Engineer | DNS/LB changes |
| Cassandra Rust Owner | Root cause analysis |
