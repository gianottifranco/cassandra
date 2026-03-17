# Java Package Inventory

**Generated**: auto by `scripts/coverage_audit.py`
**Total packages**: 187
**Total Java files**: 3168
**Classified**: 187/187

## Packages by Subsystem

| Top-Level | Packages | Files | Classified |
|-----------|----------|-------|------------|
| `audit` | 1 | 15 | 1 |
| `auth` | 2 | 69 | 2 |
| `batchlog` | 1 | 5 | 1 |
| `cache` | 1 | 20 | 1 |
| `concurrent` | 1 | 38 | 1 |
| `config` | 1 | 35 | 1 |
| `cql3` | 14 | 285 | 14 |
| `db` | 24 | 523 | 24 |
| `dht` | 2 | 39 | 2 |
| `diag` | 2 | 8 | 2 |
| `exceptions` | 1 | 47 | 1 |
| `fql` | 1 | 3 | 1 |
| `gms` | 1 | 27 | 1 |
| `hints` | 1 | 31 | 1 |
| `index` | 40 | 277 | 40 |
| `io` | 12 | 244 | 12 |
| `journal` | 1 | 25 | 1 |
| `locator` | 1 | 73 | 1 |
| `metrics` | 1 | 86 | 1 |
| `net` | 1 | 75 | 1 |
| `notifications` | 1 | 14 | 1 |
| `profiler` | 1 | 1 | 1 |
| `repair` | 7 | 100 | 7 |
| `schema` | 1 | 48 | 1 |
| `security` | 1 | 18 | 1 |
| `serializers` | 1 | 31 | 1 |
| `service` | 26 | 332 | 26 |
| `streaming` | 5 | 62 | 5 |
| `tcm` | 12 | 133 | 12 |
| `tools` | 5 | 214 | 5 |
| `tracing` | 1 | 6 | 1 |
| `transport` | 2 | 47 | 2 |
| `triggers` | 1 | 4 | 1 |
| `utils` | 14 | 233 | 14 |

## All Packages (187)

| Package | Files | Classified |
|---------|-------|------------|
| `org.apache.cassandra.audit` | 15 | yes |
| `org.apache.cassandra.auth` | 67 | yes |
| `org.apache.cassandra.auth.jmx` | 2 | yes |
| `org.apache.cassandra.batchlog` | 5 | yes |
| `org.apache.cassandra.cache` | 20 | yes |
| `org.apache.cassandra.concurrent` | 38 | yes |
| `org.apache.cassandra.config` | 35 | yes |
| `org.apache.cassandra.cql3` | 41 | yes |
| `org.apache.cassandra.cql3.conditions` | 6 | yes |
| `org.apache.cassandra.cql3.constraints` | 19 | yes |
| `org.apache.cassandra.cql3.functions` | 44 | yes |
| `org.apache.cassandra.cql3.functions.masking` | 8 | yes |
| `org.apache.cassandra.cql3.functions.types` | 27 | yes |
| `org.apache.cassandra.cql3.functions.types.exceptions` | 4 | yes |
| `org.apache.cassandra.cql3.functions.types.utils` | 1 | yes |
| `org.apache.cassandra.cql3.restrictions` | 14 | yes |
| `org.apache.cassandra.cql3.selection` | 28 | yes |
| `org.apache.cassandra.cql3.statements` | 38 | yes |
| `org.apache.cassandra.cql3.statements.schema` | 37 | yes |
| `org.apache.cassandra.cql3.terms` | 13 | yes |
| `org.apache.cassandra.cql3.transactions` | 5 | yes |
| `org.apache.cassandra.db` | 100 | yes |
| `org.apache.cassandra.db.aggregation` | 3 | yes |
| `org.apache.cassandra.db.commitlog` | 24 | yes |
| `org.apache.cassandra.db.compaction` | 46 | yes |
| `org.apache.cassandra.db.compaction.unified` | 4 | yes |
| `org.apache.cassandra.db.compaction.writers` | 5 | yes |
| `org.apache.cassandra.db.compression` | 18 | yes |
| `org.apache.cassandra.db.context` | 1 | yes |
| `org.apache.cassandra.db.filter` | 11 | yes |
| `org.apache.cassandra.db.guardrails` | 30 | yes |
| `org.apache.cassandra.db.lifecycle` | 18 | yes |
| `org.apache.cassandra.db.marshal` | 58 | yes |
| `org.apache.cassandra.db.memtable` | 12 | yes |
| `org.apache.cassandra.db.monitoring` | 4 | yes |
| `org.apache.cassandra.db.partitions` | 19 | yes |
| `org.apache.cassandra.db.repair` | 4 | yes |
| `org.apache.cassandra.db.rows` | 41 | yes |
| `org.apache.cassandra.db.streaming` | 17 | yes |
| `org.apache.cassandra.db.transform` | 18 | yes |
| `org.apache.cassandra.db.tries` | 13 | yes |
| `org.apache.cassandra.db.view` | 7 | yes |
| `org.apache.cassandra.db.virtual` | 52 | yes |
| `org.apache.cassandra.db.virtual.model` | 9 | yes |
| `org.apache.cassandra.db.virtual.walker` | 9 | yes |
| `org.apache.cassandra.dht` | 29 | yes |
| `org.apache.cassandra.dht.tokenallocator` | 10 | yes |
| `org.apache.cassandra.diag` | 6 | yes |
| `org.apache.cassandra.diag.store` | 2 | yes |
| `org.apache.cassandra.exceptions` | 47 | yes |
| `org.apache.cassandra.fql` | 3 | yes |
| `org.apache.cassandra.gms` | 27 | yes |
| `org.apache.cassandra.hints` | 31 | yes |
| `org.apache.cassandra.index` | 10 | yes |
| `org.apache.cassandra.index.accord` | 16 | yes |
| `org.apache.cassandra.index.internal` | 5 | yes |
| `org.apache.cassandra.index.internal.composites` | 8 | yes |
| `org.apache.cassandra.index.internal.keys` | 2 | yes |
| `org.apache.cassandra.index.sai` | 9 | yes |
| `org.apache.cassandra.index.sai.analyzer` | 3 | yes |
| `org.apache.cassandra.index.sai.analyzer.filter` | 3 | yes |
| `org.apache.cassandra.index.sai.disk` | 8 | yes |
| `org.apache.cassandra.index.sai.disk.format` | 4 | yes |
| `org.apache.cassandra.index.sai.disk.io` | 5 | yes |
| `org.apache.cassandra.index.sai.disk.v1` | 15 | yes |
| `org.apache.cassandra.index.sai.disk.v1.bbtree` | 8 | yes |
| `org.apache.cassandra.index.sai.disk.v1.bitpack` | 8 | yes |
| `org.apache.cassandra.index.sai.disk.v1.keystore` | 3 | yes |
| `org.apache.cassandra.index.sai.disk.v1.postings` | 7 | yes |
| `org.apache.cassandra.index.sai.disk.v1.segment` | 12 | yes |
| `org.apache.cassandra.index.sai.disk.v1.trie` | 3 | yes |
| `org.apache.cassandra.index.sai.disk.v1.vector` | 13 | yes |
| `org.apache.cassandra.index.sai.iterators` | 6 | yes |
| `org.apache.cassandra.index.sai.memory` | 9 | yes |
| `org.apache.cassandra.index.sai.metrics` | 8 | yes |
| `org.apache.cassandra.index.sai.plan` | 9 | yes |
| `org.apache.cassandra.index.sai.postings` | 4 | yes |
| `org.apache.cassandra.index.sai.utils` | 12 | yes |
| `org.apache.cassandra.index.sai.view` | 3 | yes |
| `org.apache.cassandra.index.sai.virtual` | 4 | yes |
| `org.apache.cassandra.index.sasi` | 6 | yes |
| `org.apache.cassandra.index.sasi.analyzer` | 9 | yes |
| `org.apache.cassandra.index.sasi.analyzer.filter` | 8 | yes |
| `org.apache.cassandra.index.sasi.conf` | 3 | yes |
| `org.apache.cassandra.index.sasi.conf.view` | 4 | yes |
| `org.apache.cassandra.index.sasi.disk` | 11 | yes |
| `org.apache.cassandra.index.sasi.exceptions` | 1 | yes |
| `org.apache.cassandra.index.sasi.memory` | 5 | yes |
| `org.apache.cassandra.index.sasi.plan` | 5 | yes |
| `org.apache.cassandra.index.sasi.sa` | 8 | yes |
| `org.apache.cassandra.index.sasi.utils` | 9 | yes |
| `org.apache.cassandra.index.sasi.utils.trie` | 7 | yes |
| `org.apache.cassandra.index.transactions` | 4 | yes |
| `org.apache.cassandra.io` | 19 | yes |
| `org.apache.cassandra.io.compress` | 13 | yes |
| `org.apache.cassandra.io.sstable` | 46 | yes |
| `org.apache.cassandra.io.sstable.filter` | 2 | yes |
| `org.apache.cassandra.io.sstable.format` | 20 | yes |
| `org.apache.cassandra.io.sstable.format.big` | 14 | yes |
| `org.apache.cassandra.io.sstable.format.bti` | 20 | yes |
| `org.apache.cassandra.io.sstable.indexsummary` | 7 | yes |
| `org.apache.cassandra.io.sstable.keycache` | 3 | yes |
| `org.apache.cassandra.io.sstable.metadata` | 9 | yes |
| `org.apache.cassandra.io.tries` | 11 | yes |
| `org.apache.cassandra.io.util` | 80 | yes |
| `org.apache.cassandra.journal` | 25 | yes |
| `org.apache.cassandra.locator` | 73 | yes |
| `org.apache.cassandra.metrics` | 86 | yes |
| `org.apache.cassandra.net` | 75 | yes |
| `org.apache.cassandra.notifications` | 14 | yes |
| `org.apache.cassandra.profiler` | 1 | yes |
| `org.apache.cassandra.repair` | 40 | yes |
| `org.apache.cassandra.repair.asymmetric` | 8 | yes |
| `org.apache.cassandra.repair.autorepair` | 12 | yes |
| `org.apache.cassandra.repair.consistent` | 8 | yes |
| `org.apache.cassandra.repair.consistent.admin` | 5 | yes |
| `org.apache.cassandra.repair.messages` | 17 | yes |
| `org.apache.cassandra.repair.state` | 10 | yes |
| `org.apache.cassandra.schema` | 48 | yes |
| `org.apache.cassandra.security` | 18 | yes |
| `org.apache.cassandra.serializers` | 31 | yes |
| `org.apache.cassandra.service` | 48 | yes |
| `org.apache.cassandra.service.accord` | 61 | yes |
| `org.apache.cassandra.service.accord.api` | 10 | yes |
| `org.apache.cassandra.service.accord.events` | 1 | yes |
| `org.apache.cassandra.service.accord.exceptions` | 4 | yes |
| `org.apache.cassandra.service.accord.fastpath` | 4 | yes |
| `org.apache.cassandra.service.accord.interop` | 8 | yes |
| `org.apache.cassandra.service.accord.journal` | 1 | yes |
| `org.apache.cassandra.service.accord.repair` | 2 | yes |
| `org.apache.cassandra.service.accord.serializers` | 35 | yes |
| `org.apache.cassandra.service.accord.txn` | 25 | yes |
| `org.apache.cassandra.service.consensus` | 2 | yes |
| `org.apache.cassandra.service.consensus.migration` | 11 | yes |
| `org.apache.cassandra.service.disk.usage` | 3 | yes |
| `org.apache.cassandra.service.pager` | 7 | yes |
| `org.apache.cassandra.service.paxos` | 18 | yes |
| `org.apache.cassandra.service.paxos.cleanup` | 13 | yes |
| `org.apache.cassandra.service.paxos.uncommitted` | 9 | yes |
| `org.apache.cassandra.service.paxos.v1` | 6 | yes |
| `org.apache.cassandra.service.reads` | 16 | yes |
| `org.apache.cassandra.service.reads.range` | 8 | yes |
| `org.apache.cassandra.service.reads.repair` | 15 | yes |
| `org.apache.cassandra.service.reads.thresholds` | 4 | yes |
| `org.apache.cassandra.service.snapshot` | 14 | yes |
| `org.apache.cassandra.service.thresholds` | 1 | yes |
| `org.apache.cassandra.service.writes.thresholds` | 6 | yes |
| `org.apache.cassandra.streaming` | 38 | yes |
| `org.apache.cassandra.streaming.async` | 5 | yes |
| `org.apache.cassandra.streaming.compress` | 1 | yes |
| `org.apache.cassandra.streaming.management` | 6 | yes |
| `org.apache.cassandra.streaming.messages` | 12 | yes |
| `org.apache.cassandra.tcm` | 28 | yes |
| `org.apache.cassandra.tcm.compatibility` | 2 | yes |
| `org.apache.cassandra.tcm.extensions` | 6 | yes |
| `org.apache.cassandra.tcm.listeners` | 9 | yes |
| `org.apache.cassandra.tcm.log` | 6 | yes |
| `org.apache.cassandra.tcm.membership` | 6 | yes |
| `org.apache.cassandra.tcm.migration` | 5 | yes |
| `org.apache.cassandra.tcm.ownership` | 13 | yes |
| `org.apache.cassandra.tcm.sequences` | 18 | yes |
| `org.apache.cassandra.tcm.serialization` | 8 | yes |
| `org.apache.cassandra.tcm.transformations` | 24 | yes |
| `org.apache.cassandra.tcm.transformations.cms` | 8 | yes |
| `org.apache.cassandra.tools` | 31 | yes |
| `org.apache.cassandra.tools.nodetool` | 165 | yes |
| `org.apache.cassandra.tools.nodetool.formatter` | 1 | yes |
| `org.apache.cassandra.tools.nodetool.layout` | 2 | yes |
| `org.apache.cassandra.tools.nodetool.stats` | 15 | yes |
| `org.apache.cassandra.tracing` | 6 | yes |
| `org.apache.cassandra.transport` | 29 | yes |
| `org.apache.cassandra.transport.messages` | 18 | yes |
| `org.apache.cassandra.triggers` | 4 | yes |
| `org.apache.cassandra.utils` | 124 | yes |
| `org.apache.cassandra.utils.binlog` | 5 | yes |
| `org.apache.cassandra.utils.btree` | 15 | yes |
| `org.apache.cassandra.utils.bytecomparable` | 3 | yes |
| `org.apache.cassandra.utils.caching` | 1 | yes |
| `org.apache.cassandra.utils.concurrent` | 39 | yes |
| `org.apache.cassandra.utils.jmx` | 2 | yes |
| `org.apache.cassandra.utils.logging` | 8 | yes |
| `org.apache.cassandra.utils.memory` | 21 | yes |
| `org.apache.cassandra.utils.obs` | 3 | yes |
| `org.apache.cassandra.utils.progress` | 5 | yes |
| `org.apache.cassandra.utils.progress.jmx` | 3 | yes |
| `org.apache.cassandra.utils.streamhist` | 3 | yes |
| `org.apache.cassandra.utils.vint` | 1 | yes |

## Nodetool Commands (154)

1. `AbortBootstrap`
2. `AccordAdmin`
3. `AlterTopology`
4. `Assassinate`
5. `AsyncProfileCommandGroup`
6. `AutoRepairStatus`
7. `Bootstrap`
8. `BootstrapResume`
9. `CIDRFilteringStats`
10. `CMSAdmin`
11. `Cleanup`
12. `ClearSnapshot`
13. `ClientStats`
14. `Compact`
15. `CompactionHistory`
16. `CompactionStats`
17. `CompressionDictionaryCommandGroup`
18. `ConsensusMigrationAdmin`
19. `DataPaths`
20. `Decommission`
21. `DescribeCluster`
22. `DescribeRing`
23. `DisableAuditLog`
24. `DisableAutoCompaction`
25. `DisableBackup`
26. `DisableBinary`
27. `DisableFullQueryLog`
28. `DisableGossip`
29. `DisableHandoff`
30. `DisableHintsForDC`
31. `DisableOldProtocolVersions`
32. `Drain`
33. `DropCIDRGroup`
34. `EnableAuditLog`
35. `EnableAutoCompaction`
36. `EnableBackup`
37. `EnableBinary`
38. `EnableFullQueryLog`
39. `EnableGossip`
40. `EnableHandoff`
41. `EnableHintsForDC`
42. `EnableOldProtocolVersions`
43. `FailureDetectorInfo`
44. `Flush`
45. `ForceCompact`
46. `GarbageCollect`
47. `GcStats`
48. `GetAuditLog`
49. `GetAuthCacheConfig`
50. `GetAutoRepairConfig`
51. `GetBatchlogReplayTrottle`
52. `GetCIDRGroupsOfIP`
53. `GetColumnIndexSize`
54. `GetCompactionThreshold`
55. `GetCompactionThroughput`
56. `GetConcurrency`
57. `GetConcurrentCompactors`
58. `GetConcurrentViewBuilders`
59. `GetDefaultKeyspaceRF`
60. `GetEndpoints`
61. `GetFullQueryLog`
62. `GetInterDCStreamThroughput`
63. `GetLoggingLevels`
64. `GetMaxHintWindow`
65. `GetSSTables`
66. `GetSeeds`
67. `GetSnapshotThrottle`
68. `GetStreamThroughput`
69. `GetTimeout`
70. `GetTraceProbability`
71. `GossipInfo`
72. `GuardrailsConfigCommand`
73. `Import`
74. `Info`
75. `InvalidateCIDRPermissionsCache`
76. `InvalidateCounterCache`
77. `InvalidateCredentialsCache`
78. `InvalidateJmxPermissionsCache`
79. `InvalidateKeyCache`
80. `InvalidateNetworkPermissionsCache`
81. `InvalidatePermissionsCache`
82. `InvalidateRolesCache`
83. `InvalidateRowCache`
84. `Join`
85. `ListCIDRGroups`
86. `ListPendingHints`
87. `ListSnapshots`
88. `Move`
89. `NetStats`
90. `PauseHandoff`
91. `ProfileLoad`
92. `ProxyHistograms`
93. `RangeKeySample`
94. `Rebuild`
95. `RebuildIndex`
96. `RecompressSSTables`
97. `Refresh`
98. `RefreshSizeEstimates`
99. `ReloadCIDRGroupsCache`
100. `ReloadLocalSchema`
101. `ReloadSeeds`
102. `ReloadSslCertificates`
103. `ReloadTriggers`
104. `RelocateSSTables`
105. `RemoveNode`
106. `Repair`
107. `RepairAdmin`
108. `ReplayBatchlog`
109. `ResetFullQueryLog`
110. `ResetLocalSchema`
111. `ResumeHandoff`
112. `Ring`
113. `SSTableRepairedSet`
114. `Scrub`
115. `SetAuthCacheConfig`
116. `SetAutoRepairConfig`
117. `SetBatchlogReplayThrottle`
118. `SetCacheCapacity`
119. `SetCacheKeysToSave`
120. `SetColumnIndexSize`
121. `SetCompactionThreshold`
122. `SetCompactionThroughput`
123. `SetConcurrency`
124. `SetConcurrentCompactors`
125. `SetConcurrentViewBuilders`
126. `SetDefaultKeyspaceRF`
127. `SetHintedHandoffThrottleInKB`
128. `SetInterDCStreamThroughput`
129. `SetLoggingLevel`
130. `SetMaxHintWindow`
131. `SetSnapshotThrottle`
132. `SetStreamThroughput`
133. `SetTimeout`
134. `SetTraceProbability`
135. `Sjk`
136. `Snapshot`
137. `Status`
138. `StatusAutoCompaction`
139. `StatusBackup`
140. `StatusBinary`
141. `StatusGossip`
142. `StatusHandoff`
143. `Stop`
144. `StopDaemon`
145. `TableHistograms`
146. `TableStats`
147. `TopPartitions`
148. `TpStats`
149. `TruncateHints`
150. `UpdateCIDRGroup`
151. `UpgradeSSTable`
152. `Verify`
153. `Version`
154. `ViewBuildStatus`

## Virtual Tables (34)

- `AccordVirtualTables`
- `BatchMetricsTable`
- `CIDRFilteringMetricsTable`
- `CIDRFilteringMetricsTableMBean`
- `CQLMetricsTable`
- `CachesTable`
- `ClientsTable`
- `ClusterMetadataDirectoryTable`
- `ClusterMetadataLogTable`
- `CredentialsCacheKeysTable`
- `ExceptionsTable`
- `GossipInfoTable`
- `InternodeInboundTable`
- `InternodeOutboundTable`
- `JmxPermissionsCacheKeysTable`
- `LocalRepairTables`
- `LocalTable`
- `LogMessagesTable`
- `NetworkPermissionsCacheKeysTable`
- `PartitionKeyStatsTable`
- `PeersTable`
- `PendingHintsTable`
- `PermissionsCacheKeysTable`
- `QueriesTable`
- `RolesCacheKeysTable`
- `SSTableTasksTable`
- `SchemaCommentsTable`
- `SchemaSecurityLabelsTable`
- `SettingsTable`
- `SlowQueriesTable`
- `SnapshotsTable`
- `StreamingVirtualTable`
- `SystemPropertiesTable`
- `TableMetricTables`

## SSTable Tools (13)

- `SSTableExpiredBlockers`
- `SSTableExport`
- `SSTableLevelResetter`
- `SSTableMetadataViewer`
- `SSTableOfflineRelevel`
- `SSTablePartitions`
- `SSTableRepairedAtSetter`
- `StandaloneJournalUtil`
- `StandaloneSSTableUtil`
- `StandaloneScrubber`
- `StandaloneSplitter`
- `StandaloneUpgrader`
- `StandaloneVerifier`

## Configuration Files (20)

- `README.txt`
- `cassandra-env.sh`
- `cassandra-jaas.config`
- `cassandra-rackdc.properties`
- `cassandra-topology.properties.example`
- `cassandra.yaml`
- `cassandra_latest.yaml`
- `commitlog_archiving.properties`
- `cqlshrc.sample`
- `credentials.sample`
- `jvm-clients.options`
- `jvm-server.options`
- `jvm11-clients.options`
- `jvm11-server.options`
- `jvm17-clients.options`
- `jvm17-server.options`
- `jvm21-clients.options`
- `jvm21-server.options`
- `logback-tools.xml`
- `logback.xml`

## Test Statistics

| Category | Java Test Files |
|----------|-----------------|
| distributed | 577 |
| long | 22 |
| other | 0 |
| unit | 1872 |
