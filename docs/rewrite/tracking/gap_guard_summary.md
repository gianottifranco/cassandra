# Gap Guard Summary

27 ignored tests serving as gap guards. Run `python3 rust/scripts/gap_audit.py gap-guards` for live count.

## By Category

### CQL (Prompts 02, 03, 04)
- CQL built-in functions not evaluated
- Permission statements (GRANT/REVOKE) not parsed
- CHECK constraints not implemented
- Advanced restrictions (multi-column, CONTAINS, LIKE) missing
- Index diff testing incomplete

### Storage (Prompts 05, 09, 10, 11, 20)
- Java SSTable binary compatibility not verified
- Compaction execution not tested
- Trie index not implemented
- DB filters (ClusteringIndexFilter, ColumnFilter, RowFilter) missing
- Caching subsystem not implemented
- Row transformations not implemented
- Guardrails not enforced

### Distributed (Prompts 06, 07, 08, 12, 13, 21, 22, 23)
- Paging not implemented
- Query aggregation missing
- Gossip wire compatibility not verified
- Internode wire compatibility not verified

### Security (Prompts 14, 15, 19)
- Full Query Logging not implemented
- LDAP/Kerberos authentication not implemented

### Tooling (Prompts 19, 24)
- ~140 nodetool commands missing
- SSTable offline tools are stubs
- ~60 virtual tables missing

### Trunk-Only (Prompts 09, 21, 22)
- TCM commit protocol
- Accord transaction execution
- Consensus service
- Journal subsystem

### Infrastructure (Prompts 25, 26)
- Concurrency stages (Stage model) not implemented
- Tracing storage (system_traces) not implemented

## Non-Degradation Rule

Per ADR-022, the gap guard count must not increase between prompts. Each new gap guard must:
1. Reference a gap backlog entry
2. Include the owning prompt for resolution
3. Format: `// GAP-GUARD: <backlog-id>, <prompt-NN>`
