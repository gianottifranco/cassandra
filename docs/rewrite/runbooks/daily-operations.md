# Runbook: Daily Operations

## Overview

Day-to-day operational procedures for managing a Cassandra Rust deployment.

## Monitoring Checklist

### Morning checks

```bash
# 1. Health check all nodes
for node in node1 node2 node3; do
  echo "$node: $(curl -s http://$node:9090/health | jq -r .status)"
done

# 2. Check for pending operations
curl -s http://localhost:9090/api/v1/operations | jq .

# 3. Check key metrics
curl -s http://localhost:9090/metrics | grep -E \
  "cassandra_(pending_compactions|live_sstable|connected_native|read_count|write_count)"
```

### Key Alerts to Monitor

| Metric | Warning | Critical |
|--------|---------|----------|
| Read latency p99 | > 100ms | > 500ms |
| Pending compactions | > 50 | > 200 |
| Connected clients | > 80% max | > 95% max |
| Disk usage | > 70% | > 85% |

## Common Operations

### Repair

```bash
# Full repair of a keyspace (recommended: weekly per keyspace)
cassandra-tools repair my_keyspace

# Monitor repair progress
curl http://localhost:9090/api/v1/operations | jq '.[] | select(.operation_type == "REPAIR")'
```

### Compaction

```bash
# Force compaction on a specific table
cassandra-tools compact my_keyspace my_table

# Monitor compaction progress
curl http://localhost:9090/api/v1/virtual/system_views/sstable_tasks
```

### Cleanup (after topology changes)

```bash
# Run after adding/removing nodes to remove data no longer owned
cassandra-tools cleanup my_keyspace
```

### Log Management

```bash
# Check audit log size
du -sh logs/audit/

# View recent audit events
tail -50 logs/audit/audit.log | jq .

# Check FQL status
ls -la logs/fql/
```

### Configuration Changes

1. Edit `cassandra.yaml`
2. Restart the Cassandra process (most settings require restart)
3. For TLS cert changes only: no restart needed if `hot_reload` is enabled

### Logging Levels

```bash
# View current levels
cassandra-tools getlogginglevels

# Enable debug for a specific subsystem
cassandra-tools setlogginglevel cassandra_storage DEBUG

# Reduce noise
cassandra-tools setlogginglevel cassandra_coordinator INFO
```

## Capacity Planning

### Disk

- Monitor `cassandra_storage_load_bytes` trend
- Plan for compaction overhead: 2x data size temporarily during major compaction
- Snapshot space: plan for 10-20% additional space

### Memory

- Rust Cassandra uses system allocator; monitor RSS via OS tools
- Key cache size configurable in `cassandra.yaml`

### Network

- Monitor `cassandra_connected_native_clients` for connection pool sizing
- Inter-node traffic proportional to replication factor and write volume

## Upgrade Procedure

1. Take snapshot: `cassandra-tools snapshot --name pre-upgrade`
2. Stop Cassandra process
3. Replace binary
4. Start Cassandra process
5. Verify with health check
6. Run repair to ensure consistency
7. Repeat for each node (rolling upgrade)
