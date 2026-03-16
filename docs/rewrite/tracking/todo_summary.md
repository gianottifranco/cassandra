# TODO Summary

42 TODOs identified across 11 Rust crates. Run `python3 rust/scripts/gap_audit.py todos` for live count.

## By Crate

| Crate | Count | Key Items | Owning Prompts |
|-------|-------|-----------|----------------|
| cassandra-tools | 15 | Admin API calls (snapshot, compact, sstableloader, audit, FQL, load gen, bootstrap) | 24, 26 |
| cassandra-storage | 11 | SASI memtable ops, SAI gaps, legacy index persistence, MV backfill, MV WHERE/PK | 05, 09, 10, 11, 18 |
| cassandra-server | 5 | AlterTable no-op, TLS config loading, schema passing, Row results mapping, role_permissions | 01, 02, 14, 24 |
| cassandra-coordinator | 4 | Read repair blocking, latency percentiles, READ_DATA replicas, repair tracking | 06, 13, 22 |
| cassandra-security | 3 | Persist permissions, persist roles, SHA-256 masking | 14, 15, 19 |
| cassandra-cql | 2 | Tuple type parsing, column validation | 02, 03, 04 |
| cassandra-native-protocol | 2 | Real credential verification | 01, 02, 14 |
| cassandra-common | 2 | Module expansion, murmur3 diff-test | 25 |
| cassandra-cluster-metadata | 1 | Transient replication | 07, 21, 23 |
| cassandra-migration | 1 | Full binary SSTable conversion | 11, 26 |
| cassandra-diff-tests | 1 | Wire to QueryExecutor | 26 |

## Resolution Timeline

- **Phase A (P0)**: cassandra-server, cassandra-cql, cassandra-native-protocol TODOs resolved by Prompt 10
- **Phase B (P1)**: cassandra-storage, cassandra-security, cassandra-coordinator TODOs resolved by Prompt 20
- **Phase C (P2)**: cassandra-tools, cassandra-common, cassandra-migration TODOs resolved by Prompt 26
