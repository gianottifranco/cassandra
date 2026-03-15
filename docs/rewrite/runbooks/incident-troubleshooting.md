# Runbook: Incident Troubleshooting

## Overview

Quick reference for diagnosing and resolving common Cassandra Rust incidents.

## Diagnostic Commands

### Health Check
```bash
curl http://localhost:9090/health
```

### Metrics
```bash
# Full Prometheus scrape
curl http://localhost:9090/metrics

# Specific metric
curl -s http://localhost:9090/metrics | grep cassandra_read_count

# Connected clients
curl -s http://localhost:9090/metrics | grep cassandra_connected
```

### Active Operations
```bash
curl http://localhost:9090/api/v1/operations
```

### Virtual Tables
```bash
# Node info
curl http://localhost:9090/api/v1/virtual/system_views/local

# Thread pools
curl http://localhost:9090/api/v1/virtual/system_views/thread_pools

# Active compactions
curl http://localhost:9090/api/v1/virtual/system_views/sstable_tasks
```

## Common Incidents

### 1. High Read Latency

**Symptoms:** `cassandra_client_request_latency_seconds{operation="read"}` p99 > 100ms

**Diagnostics:**
```bash
# Check pending compactions
curl -s http://localhost:9090/metrics | grep pending_compactions

# Check tombstone count
curl -s http://localhost:9090/metrics | grep tombstone_scanned

# Check thread pools
curl http://localhost:9090/api/v1/virtual/system_views/thread_pools
```

**Resolution:**
- If high pending compactions: wait for compaction to complete or force compaction
- If high tombstones: review gc_grace_seconds and run repair
- If thread pools saturated: increase concurrent_reads in cassandra.yaml

### 2. TLS Handshake Failures

**Symptoms:** Clients cannot connect when TLS is enabled

**Diagnostics:**
```bash
# Test TLS handshake
openssl s_client -connect localhost:9042

# Check server logs for cert reload errors
grep "TLS" /var/log/cassandra/system.log
```

**Resolution:**
- Verify certificate files are readable by the cassandra process
- Check certificate chain is complete (includes intermediates)
- Verify certificate hasn't expired
- See `tls-certificate-rotation.md` for certificate migration

### 3. Authentication Failures

**Symptoms:** `AUTH_FAILURE` events in audit log

**Diagnostics:**
```bash
# Check audit log for patterns
grep AUTH_FAILURE logs/audit/audit.log | tail -20

# Check role exists
# (via CQL or admin API when available)
```

**Resolution:**
- Verify username/password in client configuration
- Check role has `LOGIN` permission (`can_login: true`)
- If CIDR restrictions active, verify client IP is in allowed range

### 4. Disk Space Exhaustion

**Symptoms:** Write failures, compaction stalls

**Diagnostics:**
```bash
# Check storage load
curl -s http://localhost:9090/metrics | grep storage_load

# Check disk usage
df -h /var/lib/cassandra/data
```

**Resolution:**
- Clear old snapshots: `cassandra-tools clearsnapshot`
- Force major compaction to reclaim tombstone space
- Add data directories or expand disk

### 5. Node Cannot Start

**Diagnostics:**
```bash
# Check for commit log corruption
ls -la data/commitlog/

# Check system log
tail -100 /var/log/cassandra/system.log
```

**Resolution:**
- If commit log replay fails, check for corrupted segments
- Ensure data directories have correct permissions
- Verify cassandra.yaml syntax

## Log Locations

| Log | Location | Format |
|-----|----------|--------|
| System log | stdout (tracing) | Structured text |
| Audit log | `logs/audit/audit.log` | JSON lines |
| FQL | `logs/fql/fql.bin` | Binary (use FqlReader) |

## Useful Metrics

| Metric | Alert Threshold | Description |
|--------|----------------|-------------|
| `cassandra_client_request_latency_seconds` p99 | > 500ms | Request latency |
| `cassandra_pending_compactions` | > 100 | Compaction backlog |
| `cassandra_live_sstable_count` | > 500 per table | SSTable sprawl |
| `cassandra_connected_native_clients` | > max configured | Connection saturation |
