# Tooling Gaps Tracking

## nodetool

| Status | Details |
|--------|---------|
| Implemented | 17 subcommands (clap framework) |
| Missing | ~140 additional nodetool commands |
| Owning Prompt | 24 |
| Closure Criteria | All critical nodetool commands functional |

### Key Missing Commands
- status, info, ring (essential for operations)
- compact, flush, cleanup (maintenance)
- repair (delegates to repair subsystem)
- snapshot, clearsnapshot (backup)
- getcompactionthroughput, setcompactionthroughput
- enablebackup, disablebackup
- assassinate, removenode
- rebuild, bootstrap

## cqlsh

| Status | Details |
|--------|---------|
| Current | No Rust cqlsh; relies on Python cqlsh connecting to Rust server |
| Owning Prompt | 24 |
| Closure Criteria | Python cqlsh connects and works with Rust server |

## SSTable Offline Tools

| Tool | Status | Owning Prompt |
|------|--------|---------------|
| SSTableExport | Stub | 11 |
| Scrubber | Stub | 11 |
| Splitter | Stub | 11 |
| Upgrader | Stub | 11 |
| Verifier | Stub | 11 |
| MetadataViewer | Stub | 11 |
| LevelResetter | Stub | 11 |
| RepairSetSetter | Stub | 11 |

## Virtual Tables (~60 missing of ~65 total)

| Category | Implemented | Missing | Owning Prompt |
|----------|------------|---------|---------------|
| system_views | 5 | ~60 | 19 |

## HTTP Admin API

| Status | Details |
|--------|---------|
| Current | Rust-only HTTP admin API (partial) |
| Owning Prompt | 08 |
| Notes | Not in Java baseline — Rust-native addition |
