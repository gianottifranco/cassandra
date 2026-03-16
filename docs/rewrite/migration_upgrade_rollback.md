# Migration, Upgrade & Rollback: Complete Reference

**Version**: 0.2.0
**Date**: 2026-03-16

## Upgrade Matrix

### Supported Migration Paths

| Source Version | Target | SSTable Format | Conversion | Status |
|---------------|--------|---------------|------------|--------|
| Java 4.0.x | Rust 0.1.0 | big-ma → rust-native | Required | ✅ Supported |
| Java 4.1.x | Rust 0.1.0 | big-nb → rust-native | Required | ✅ Supported |
| Java 5.0.x | Rust 0.1.0 | big-nb/bti → rust-native | Required | ✅ Supported |

### Unsupported Paths

| Source Version | Reason | Workaround |
|---------------|--------|-----------|
| Java 3.x | SSTable format incompatible | Upgrade to 4.0+ first |
| Java 2.x | SSTable format incompatible | Upgrade to 4.0+ first |
| Rust → Java | Not a supported direction | Keep Java cluster for rollback |

### Explicitly Prohibited

- **Mixed-cluster** (Rust nodes joining Java ring): gossip, internode, SSTable formats are not binary-compatible. See [ADR-014](adrs/014-migration-strategy.md).
- **In-place upgrade** (replacing Java binary with Rust on same node): data format incompatibility.
- **Rolling upgrade within same ring**: requires protocol-level compatibility not yet achieved.

## Migration Architecture

```
┌──────────────────┐                    ┌──────────────────┐
│  Java Cluster    │    FQL + Schema    │  Migration Tool  │
│  (production)    │───────────────────▶│  (cassandra-     │
│                  │                    │   migration)     │
│  4.0/4.1/5.0     │    Snapshots       │                  │
│                  │───────────────────▶│  ┌────────────┐  │
└──────────────────┘                    │  │ SSTable    │  │
                                        │  │ Import     │  │
                                        │  ├────────────┤  │
                                        │  │ Schema     │  │
                                        │  │ Diff       │  │
                                        │  ├────────────┤  │
                                        │  │ Data       │  │
                                        │  │ Validator  │  │
                                        │  └─────┬──────┘  │
                                        └────────┼─────────┘
                                                 │
                                                 ▼
                                        ┌──────────────────┐
                                        │  Rust Cluster    │
                                        │  (shadow)        │
                                        └──────────────────┘
```

## Migration Sequence

### Phase 1: Pre-Checks

```bash
# 1. Verify upgrade path is supported
cargo run -p cassandra-migration -- check-upgrade \
  --source-version 4.1.6 \
  --target-version 0.1.0-rust

# 2. Export and compare schema
cqlsh java-host -e "DESCRIBE SCHEMA" > schema_java.cql
cargo run -p cassandra-migration -- schema-diff \
  --source schema_java.cql \
  --target schema_rust.cql

# 3. Enable FQL on Java cluster
nodetool enablefullquerylog --path /var/log/cassandra/fql
```

### Phase 2: Data Migration

```bash
# 1. Take full snapshot on Java cluster
nodetool snapshot -t pre-migration

# 2. Import SSTables to Rust cluster
cargo run -p cassandra-migration -- sstable-import \
  --source /var/lib/cassandra/data \
  --target /var/lib/cassandra-rust/data \
  --validate-checksums

# 3. Apply schema to Rust cluster
cqlsh rust-host -f schema_java.cql
```

### Phase 3: Shadow Traffic

```bash
# 1. Convert FQL logs
cargo run -p cassandra-migration -- fql-convert \
  --source /var/log/cassandra/fql \
  --target /tmp/fql.json

# 2. Replay shadow traffic
cargo run -p cassandra-diff-tests -- shadow-replay \
  --fql-path /tmp/fql.json \
  --rust-host localhost:9042

# 3. Monitor divergence (target: 0%)
```

### Phase 4: Validation

```bash
# 1. Validate row counts
cargo run -p cassandra-migration -- validate \
  --source-host java-host \
  --target-host rust-host \
  --tables ks.users,ks.orders

# 2. Check CDC continuity
cargo run -p cassandra-migration -- cdc-check \
  --java-cdc-dir /var/lib/cassandra/cdc_raw \
  --rust-cdc-dir /var/lib/cassandra-rust/cdc_raw

# 3. Run full diff test suite
cd rust && make migration-test
```

### Phase 5: Cutover

Pre-cutover checklist:
- [ ] Shadow traffic ≥ 1 week with 0% divergence
- [ ] Row count validation passes
- [ ] Schema diff shows `Identical`
- [ ] CDC continuity verified
- [ ] Rollback procedure tested
- [ ] Performance within SLO budget

Cutover steps:
1. Stop FQL replay
2. Switch DNS/LB to Rust cluster
3. Monitor error rates, latency for 72 hours minimum

### Phase 6: Rollback (if needed)

See [Rollback Runbook](runbooks/rollback-procedure.md).

## Mixed-Format Handling

### big ↔ bti SSTable Format

Java 5.0+ supports both `big` and `bti` (trie-indexed) format. The migration tool handles both:

| Source Format | Handling |
|--------------|---------|
| big-ma (4.0) | Full conversion via `sstable-import` |
| big-nb (4.1/5.0) | Full conversion via `sstable-import` |
| bti (5.0+) | Full conversion via `sstable-import` |

Both formats share the same Data.db content structure; the difference is in the index component. The converter reads both index formats.

### Format Detection

```bash
# Automatically detect format per-SSTable
cargo run -p cassandra-migration -- sstable-import \
  --source /var/lib/cassandra/data \
  --detect-format
```

## CDC/FQL Continuity

### CDC Bridge

During migration, CDC log continuity is maintained by:

1. **Checkpoint**: Record last Java CDC segment ID at snapshot time
2. **Migration**: Copy CDC raw segments to Rust cluster
3. **Resume**: Rust starts CDC from checkpoint + 1
4. **Validation**: Run `cdc-check` to verify no gaps

```bash
# Step 1: Create checkpoint before migration
cargo run -p cassandra-migration -- cdc-checkpoint \
  --cdc-dir /var/lib/cassandra/cdc_raw \
  --output cdc-checkpoint.json

# Step 2: After migration, verify continuity
cargo run -p cassandra-migration -- cdc-check \
  --checkpoint cdc-checkpoint.json \
  --rust-cdc-dir /var/lib/cassandra-rust/cdc_raw
```

### FQL Bridge

FQL logs from Java (Chronicle-Queue format) are converted to JSON for replay:

```bash
# Option A: Use Java fqltool to dump
fqltool dump /var/log/cassandra/fql | jq > fql.json

# Option B: Use Rust converter (simplified parsing)
cargo run -p cassandra-migration -- fql-convert \
  --source /var/log/cassandra/fql \
  --target fql.json
```

## SLOs and Metrics

### Migration SLOs

| Metric | Target | Abort Trigger |
|--------|--------|---------------|
| Shadow divergence rate | 0% | > 0.01% |
| p99 latency (Rust vs Java) | ≤ 1.5× | > 2× |
| Error rate | 0% | > 0.1% |
| Row count delta | 0 | > 0 |
| CDC gaps | 0 | > 0 |

### Metrics to Monitor

- `cassandra_rust_request_latency_seconds` (Prometheus)
- `cassandra_rust_error_total`
- `cassandra_rust_migration_shadow_divergence_total`
- `cassandra_rust_cdc_segments_processed`
- `cassandra_rust_sstable_import_bytes_total`

## Related Documents

- [ADR-014: Migration Strategy](adrs/014-migration-strategy.md)
- [ADR-020: Upgrade/Migration Architecture](adrs/020-upgrade-migration-architecture.md)
- [Compatibility Matrix](compatibility_matrix.md)
- [Rollback Runbook](runbooks/rollback-procedure.md)
- [Migration Runbook](runbooks/migration-procedure.md)
- [CDC/FQL Continuity Runbook](runbooks/cdc-fql-continuity.md)
