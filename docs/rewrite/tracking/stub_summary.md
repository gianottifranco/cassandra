# Stub Summary

55+ stubs identified across 7 Rust crates. Run `python3 rust/scripts/gap_audit.py todos` (stubs are included in TODO scan) for live count.

## By Crate

| Crate | Notable Stubs | Owning Prompts |
|-------|---------------|----------------|
| cassandra-tools | ~15 nodetool command stubs | 24 |
| cassandra-storage | UDF module (entire), Triggers module (entire) | 18, 19 |
| cassandra-server | Auth stubs, executor stubs, result mapping | 01, 02, 14 |
| cassandra-coordinator | Trigger augmentation, Accord routing, consensus router | 06, 13, 22 |
| cassandra-cluster-metadata | Ec2Snitch, GceSnitch, TransientReplication | 23 |
| cassandra-native-protocol | PasswordAuthenticator (accepts any credentials) | 14 |
| cassandra-accord | execute_transaction is stub | 22 |

## High-Priority Stubs (P0/P1)

These stubs directly block startup or beta milestones:

| Stub | Crate | Criticality | Owning Prompt |
|------|-------|-------------|---------------|
| Native protocol TCP listener | cassandra-server | P0 | 01 |
| QueryProcessor execution | cassandra-server | P0 | 02 |
| Config (most keys missing) | cassandra-config | P0 | 02 |
| CompactionManager execution | cassandra-storage | P0 | 09 |
| Row/Partition filters | cassandra-storage | P0 | 05 |
| Internode messaging service | cassandra-messaging | P1 | 08 |
| Hinted handoff persistence | cassandra-coordinator | P1 | 06 |
| Batchlog write/replay | cassandra-coordinator | P1 | 12 |
| SSTable lifecycle tracker | cassandra-storage | P1 | 09 |
