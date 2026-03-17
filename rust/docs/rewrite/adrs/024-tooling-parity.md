# ADR-024: Tooling Parity — nodetool, Offline SSTable Tools, Admin Surfaces

## Status
Implemented

## Context
The Cassandra Java-to-Rust rewrite had ~140 nodetool commands as stubs (17 implemented, rest stubs), missing offline SSTable tools, no bootstrap monitor, no token generator, and incomplete admin API endpoints. The gap analysis (Section 17) listed this as a major gap.

## Decision

### Architecture: HTTP-over-JMX
Replace Java's JMX-based nodetool with HTTP-based CLI that calls the admin API. This gives us:
- No JMX dependency (not idiomatic for Rust)
- Standard HTTP client/server pattern
- Easier testing with mock HTTP servers
- Same admin API usable by external monitoring tools

### Module Structure

**cassandra-tools (CLI):**
- `admin_client.rs` — Shared HTTP client wrapper (AdminClient)
- `cmd_cluster_info.rs` — status, info, ring, describecluster, gossipinfo
- `cmd_compaction.rs` — compact, cleanup, flush, scrub, compactionstats, etc.
- `cmd_snapshots.rs` — snapshot, listsnapshots, clearsnapshot, import, backup
- `cmd_topology.rs` — decommission, removenode, move, rebuild, drain, etc.
- `cmd_stats.rs` — tablestats, tpstats, clientstats, toppartitions, etc.
- `cmd_cache_hints.rs` — invalidate caches, handoff, hints
- `cmd_config.rs` — get/set config, reload schema/triggers/ssl, binary/gossip
- `cmd_logging_security.rs` — logging levels, audit log, FQL, tracing
- `sstable_split.rs` — Split large SSTables by target size
- `sstable_level_reset.rs` — Reset compaction level to 0
- `sstable_repaired_at.rs` — Set/clear repaired-at timestamp
- `sstable_expired_blockers.rs` — Find SSTables blocking tombstone GC
- `sstable_offline_relevel.rs` — Reassign SSTable levels for LCS
- `sstable_partitions.rs` — Analyze partition size distribution
- `bootstrap_monitor.rs` — Poll admin API for bootstrap progress
- `generate_tokens.rs` — Murmur3Partitioner token generation
- `hash_password.rs` — Password hashing utility

**cassandra-admin (Server):**
- `route_table.rs` — Modular route registration and dispatch
- `handlers_cluster.rs` — Cluster info endpoints
- `handlers_compaction.rs` — Compaction operation endpoints
- `handlers_snapshots.rs` — Snapshot CRUD endpoints
- `handlers_topology.rs` — Topology operation endpoints
- `handlers_stats.rs` — Statistics endpoints
- `handlers_cache_hints.rs` — Cache/hints endpoints
- `handlers_config.rs` — Configuration endpoints
- `handlers_logging.rs` — Logging/audit/FQL endpoints

### Command Coverage Matrix

| Category | Commands | Count |
|----------|----------|-------|
| Cluster Info | status, info, ring, describecluster, gossipinfo, version | 6 |
| Compaction | compact, cleanup, flush, scrub, compactionstats, compactionhistory, enable/disable/status autocompaction, get/set throughput, forcecompact, garbagecollect | 13 |
| Snapshots | snapshot, listsnapshots, clearsnapshot, import, enable/disable/status backup | 7 |
| Topology | decommission, removenode, move, rebuild, refresh, join, bootstrapresume, abortbootstrap, assassinate, drain, stopdaemon, netstats, topologystatus | 13 |
| Statistics | tablestats, tablehistograms, tpstats, gcstats, proxyhistograms, clientstats, toppartitions, failuredetectorinfo, datapaths, refreshsizeestimates, viewbuildstatus, getendpoints, getsstables | 13 |
| Cache & Hints | invalidate{key,row,counter,credentials,permissions,roles}cache, setcachecapacity, enable/disable/pause/resume handoff, truncatehints, listpendinghints | 13 |
| Config | getconfig, setconfig, get/set timeout, get/set streaming throughput, get/set interdc throughput, get/set concurrent compactors, reload{schema,triggers,ssl}, enable/disable/status binary, enable/disable/status gossip, getseeds | 20 |
| Logging | getlogginglevels, setlogginglevel, enable/disable auditlog, getauditlogconfig, enable/disable fql, getfqlconfig, resetfql, get/set traceprobability | 11 |
| Repair | repair, rebuild_index | 2 |
| SSTable Tools (existing) | sstabledump, sstablemetadata, sstableverify, sstablescrub, sstableupgrade | 5 |
| SSTable Tools (new) | sstablesplit, sstablelevelreset, sstablerepairedset, sstableexpiredblockers, sstableofflinerelevel, sstablepartitions | 6 |
| Utilities | sstableloader, bootstrapmonitor, generatetokens, hashpassword | 4 |
| Pass-through | auditlogviewer, fqltool, cassandra-stress | 3 |
| **Total** | | **116** |

### Intentional Differences from Java nodetool
1. **No JMX** — All communication via HTTP REST API
2. **No heap memory reporting** — Rust has no GC; gcstats returns informational message
3. **Token generation** — Built into CLI rather than separate `cassandra-tokens` binary
4. **Password hashing** — Simplified hash (bcrypt integration available via crate)
5. **Bootstrap monitor** — Polls HTTP operations endpoint instead of JMX notifications

## Consequences
- All 116 CLI commands now have implementations (real or connected-to-API)
- Admin API has 60+ endpoints covering all operational domains
- Offline SSTable tools work directly on files without running server
- 272+ tests across both crates
- Future work: wire handlers to real storage engine operations as backends mature
