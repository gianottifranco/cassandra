# Runbook: CDC/FQL Continuity During Migration

**Audience**: Operations/DBA teams
**Last Updated**: 2026-03-16

## Overview

CDC (Change Data Capture) and FQL (Full Query Logging) must maintain log continuity across the Java → Rust migration boundary. This runbook covers maintaining, verifying, and troubleshooting that continuity.

## CDC Continuity

### Pre-Migration: Create Checkpoint

```bash
# Record Java CDC state
cargo run -p cassandra-migration -- cdc-checkpoint \
  --cdc-dir /var/lib/cassandra/cdc_raw \
  --output /tmp/cdc-checkpoint.json

# Verify checkpoint
cat /tmp/cdc-checkpoint.json | jq '.last_written_id'
```

### During Migration: Copy Segments

```bash
# Copy unconsumed CDC segments
rsync -av /var/lib/cassandra/cdc_raw/ /var/lib/cassandra-rust/cdc_raw/
```

### Post-Migration: Verify Continuity

```bash
cargo run -p cassandra-migration -- cdc-check \
  --checkpoint /tmp/cdc-checkpoint.json \
  --rust-cdc-dir /var/lib/cassandra-rust/cdc_raw
```

Expected output:
```
CDC Continuity: PASS
  Source segments: 150
  Target segments: 153 (3 new post-migration)
  Missing: 0
  Gaps: 0
  Handoff: contiguous (150 → 151)
```

### Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|-----------|
| Missing segments | Copy interrupted | Re-run rsync |
| Gap in sequence | Segments consumed before copy | Acceptable if consumer processed them |
| Duplicate segments | Re-copy after partial | Safe; consumer handles idempotently |

## FQL Continuity

### Convert Java FQL

```bash
# Option 1: Java fqltool (recommended for production)
fqltool dump /var/log/cassandra/fql/ > fql_dump.json

# Option 2: Rust converter
cargo run -p cassandra-migration -- fql-convert \
  --source /var/log/cassandra/fql \
  --target fql_converted.json
```

### Merge Multiple FQL Files

```bash
cargo run -p cassandra-migration -- fql-merge \
  --inputs fql_day1.json fql_day2.json fql_day3.json \
  --output fql_merged.json
```

### Replay for Validation

```bash
cargo run -p cassandra-diff-tests -- shadow-replay \
  --fql-path fql_merged.json \
  --rust-host localhost:9042 \
  --report /tmp/replay-report.json
```

## CDC Consumer Migration

If using CDC consumers (Debezium, custom):

1. **Stop consumer** before migration
2. **Record last consumed offset** (segment ID + position)
3. **Migrate CDC segments** (as above)
4. **Update consumer config** to point to Rust CDC directory
5. **Resume consumer** from recorded offset
6. **Verify** no duplicates or gaps in downstream

## Monitoring

Watch these metrics during and after migration:

- `cassandra_rust_cdc_segments_total` — should increase steadily
- `cassandra_rust_cdc_bytes_used` — should stay below limit
- `cassandra_rust_fql_entries_replayed` — should match source count

## Rollback

If CDC continuity cannot be maintained:

1. **Stop Rust cluster**
2. **Restart Java cluster** (CDC segments still intact)
3. **Resume Java CDC consumers**
4. **Investigate** root cause

See [Rollback Procedure](rollback-procedure.md).
