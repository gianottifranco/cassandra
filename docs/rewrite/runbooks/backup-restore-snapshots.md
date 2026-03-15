# Runbook: Backup & Restore with Snapshots

## Overview

Snapshot-based backup and restore for Cassandra Rust.

## Taking a Snapshot

### Via CLI

```bash
# Snapshot all keyspaces
cassandra-tools snapshot --name daily-2026-03-15

# Snapshot specific keyspace
cassandra-tools snapshot --name backup-ks1 my_keyspace

# List snapshots
cassandra-tools listsnapshots
```

### Via Admin API

```bash
# POST to create snapshot
curl -X POST http://localhost:9090/api/v1/operations/snapshot \
  -H 'Content-Type: application/json' \
  -d '{"name": "daily-backup", "keyspaces": ["my_ks"]}'
```

## Snapshot Location

Snapshots are stored under:
```
data/<keyspace>/<table>/snapshots/<snapshot-name>/
```

Each snapshot contains hard-links to the SSTable files at the time of the snapshot.

## Backup Procedure

1. **Take snapshot**
   ```bash
   cassandra-tools snapshot --name pre-upgrade
   ```

2. **Copy snapshot off-node**
   ```bash
   tar czf backup-$(date +%Y%m%d).tar.gz \
     data/*/snapshots/pre-upgrade/
   ```

3. **Upload to remote storage**
   ```bash
   aws s3 cp backup-*.tar.gz s3://cassandra-backups/
   ```

4. **Clear local snapshot** (after confirming backup)
   ```bash
   cassandra-tools clearsnapshot --name pre-upgrade
   ```

## Restore Procedure

1. **Stop Cassandra**
   ```bash
   systemctl stop cassandra
   ```

2. **Clear existing data**
   ```bash
   rm -rf data/<keyspace>/<table>/*.db
   ```

3. **Copy snapshot files to data directory**
   ```bash
   cp data/<keyspace>/<table>/snapshots/<snapshot>/*.db \
      data/<keyspace>/<table>/
   ```

4. **Start Cassandra**
   ```bash
   systemctl start cassandra
   ```

5. **Run repair** (for multi-node clusters)
   ```bash
   cassandra-tools repair my_keyspace
   ```

## Incremental Backup

Enable in `cassandra.yaml`:
```yaml
incremental_backups: true
```

Incremental backups create hard-links in:
```
data/<keyspace>/<table>/backups/
```

## Status

> **Note:** Snapshot management is partially stubbed in the Rust implementation.
> File-system level snapshots work, but admin API integration is pending.
