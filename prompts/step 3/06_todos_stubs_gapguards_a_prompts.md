# TODOs, stubs y gap guards -> prompts de cierre

## TODOs principales por crate
- `cassandra-server` -> Prompts 01, 02, 14, 24
- `cassandra-tools` -> Prompts 24, 26
- `cassandra-storage` -> Prompts 05, 09, 10, 11, 18
- `cassandra-security` -> Prompts 14, 15, 19
- `cassandra-cql` -> Prompts 02, 03, 04
- `cassandra-cluster-metadata` -> Prompts 07, 21, 23
- `cassandra-coordinator` -> Prompts 06, 13, 22
- `cassandra-native-protocol` -> Prompts 01, 02, 14
- `cassandra-migration` -> Prompts 11, 26
- `cassandra-common` -> Prompts 25
- `cassandra-diff-tests` -> Prompt 26

## Stubs notables
- Auth stubs -> 01, 02, 14
- nodetool stubs -> 24
- UDF/triggers stubs -> 18, 19
- Accord/consensus stubs -> 22
- Cloud snitch/transient replication stubs -> 23
- PasswordAuthenticator stub -> 14

## Gap guards ignorados
- CQL gaps -> 02, 03, 04
- Storage gaps -> 05, 09, 10, 11, 20
- Distributed gaps -> 06, 07, 08, 12, 13, 21, 22, 23
- Security gaps -> 14, 15, 19
- Tooling gaps -> 19, 24
- Trunk-only gaps (TCM/Accord/consensus/journal) -> 09, 21, 22
- Infrastructure gaps -> 25, 26