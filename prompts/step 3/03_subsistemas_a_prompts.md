# Subsistemas -> prompts de cierre

| Subsistema | Prompt(s) |
|---|---|
| CommitLog | 09,10,11 |
| Compaction | 09 |
| Marshal/Types | 04 |
| Memtable/Rows/Partitions/Filter/Transform/Tries | 05 |
| CQL parser/semantics/Query processing | 02,03,04 |
| Transport (native protocol) | 01,02 |
| Net (internode) | 08 |
| Gossip / Cluster discovery | 07 |
| DHT / Locator / Snitches / Token allocator | 23 |
| Service layer / coordination | 06,22 |
| TCM/CMS | 21 |
| SSTable / IO util / compat | 10,11 |
| Streaming | 12 |
| Repair | 13 |
| Auth/Security/Config/Guardrails | 14,15 |
| Index / SAI / SASI / Vector | 16,17 |
| Schema / Views / Triggers / UDF/UDA | 18 |
| Virtual tables / Tracing / Audit / Diag | 19 |
| Cache | 20 |
| Tools / nodetool / offline utilities | 24 |
| Metrics / Exceptions / Utils / Gap guards | 25 |
| Final cluster validation / RC gates | 26 |
