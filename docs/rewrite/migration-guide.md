# Migration Guide: Java Cassandra → Rust Cassandra

**Version**: 0.1.0
**Date**: 2026-03-15

## Overview

This guide describes how to migrate from an Apache Cassandra Java cluster to the Rust implementation using a **dual-cluster with shadow traffic** approach. See [ADR-014](adrs/014-migration-strategy.md) for the design rationale.

> [!CAUTION]
> Mixed-cluster deployment (Rust nodes joining a Java ring) is **not supported** in this release. Gossip wire format and internode messaging are not yet binary-compatible.

## Prerequisites

- Rust Cassandra binary (`cassandra-server`, `cassandra-tools`) or Docker image
- Java Cassandra cluster running with Full Query Logging (FQL) enabled
- Sufficient hardware for the shadow Rust cluster (same sizing as Java)
- Network connectivity between clusters for tooling (not data path)

## Migration Steps

### Phase 1: Preparation

1. **Enable FQL on Java cluster**:
   ```bash
   nodetool enablefullquerylog --path /var/log/cassandra/fql
   ```

2. **Export schema**:
   ```bash
   cqlsh -e "DESCRIBE SCHEMA" > schema_export.cql
   ```

3. **Deploy Rust cluster** (see [Deployment](#deployment)):
   ```bash
   cd rust && docker compose -f docker-compose.prod.yml up -d
   ```

4. **Apply schema to Rust cluster**:
   ```bash
   # TODO: Apply schema via CQL when protocol listener is wired
   # cqlsh rust-host -f schema_export.cql
   ```

### Phase 2: Shadow Traffic

1. **Configure shadow replay**:
   ```bash
   # Convert FQL to JSON format for the replay tool
   # TODO: FQL converter from chronicle-queue to JSON
   ```

2. **Run shadow replay**:
   ```bash
   cargo run -p cassandra-diff-tests -- shadow-replay \
     --fql-path /path/to/fql.json \
     --rust-host localhost:9042
   ```

3. **Monitor divergence**:
   - Check shadow replay reports for `DIVERGENCE` status
   - Investigate any semantic differences
   - Target: 0% divergence for supported operations

### Phase 3: Bake Period

- Run shadow traffic for **1-2 weeks minimum**
- Monitor metrics: latency, throughput, error rate
- Compare against Java performance baselines
- Validate resource usage (memory, disk, CPU)

### Phase 4: Cutover

1. **Pre-cutover checklist**:
   - [ ] Shadow traffic running ≥ 1 week with 0% divergence
   - [ ] Performance within budget (see [ADR-015](adrs/015-performance-budgets.md))
   - [ ] Rollback procedure tested (see [Rollback Runbook](runbooks/rollback-procedure.md))
   - [ ] Backup of Java cluster data taken

2. **Switch traffic**:
   ```bash
   # Update DNS/LB to point to Rust cluster
   # Keep Java cluster running for rollback
   ```

3. **Post-cutover monitoring** (72 hours minimum):
   - Watch error rates
   - Compare latency percentiles
   - Verify data consistency via spot-checks

### Phase 5: Teardown

After successful bake period:
1. Stop Java cluster
2. Archive Java data (snapshots) for safety
3. Remove Java cluster resources

## Deployment

### Docker

```bash
cd rust
docker build -t cassandra-rust:latest .
docker compose -f docker-compose.prod.yml up -d
```

### SystemD (bare metal)

```bash
# Install binary
sudo cp target/release/cassandra-server /usr/local/bin/
sudo cp target/release/cassandra-tools /usr/local/bin/

# Create user and directories
sudo useradd -r -s /bin/false cassandra
sudo mkdir -p /var/lib/cassandra/{data,commitlog,hints}
sudo chown -R cassandra:cassandra /var/lib/cassandra

# Install service
sudo cp deploy/cassandra-rust.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cassandra-rust
sudo systemctl start cassandra-rust
```

## Rollback

If issues are detected during or after cutover:

1. **Immediate**: Switch DNS/LB back to Java cluster
2. **Verify**: Confirm Java cluster is serving traffic correctly
3. **Investigate**: Analyze Rust cluster logs and metrics
4. **Report**: Document the issue for the next migration attempt

See [Rollback Runbook](runbooks/rollback-procedure.md) for detailed steps.

## Known Limitations

| Limitation | Impact | Workaround | Tracking |
|-----------|--------|-----------|----------|
| No mixed-cluster | Cannot do rolling migration | Use dual-cluster approach | ADR-014 |
| No CQL listener wired | Cannot accept live traffic | Use test harness | Phase 6 |
| FQL format incompatible | Cannot replay Java FQL directly | JSON converter needed | TODO |
| Partial CQL support | Some queries may fail | Test coverage before migration | Feature matrix |
