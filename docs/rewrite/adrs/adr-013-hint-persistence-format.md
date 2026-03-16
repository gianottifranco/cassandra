# ADR-013: Hint Persistence Format

## Status

**Accepted** — Phase 2 write path

## Context

Java's `HintsWriter`/`HintsReader` use append-only segment files per target 
host, each entry checksummed with CRC32. When a replica is unreachable during 
a write, the coordinaten stores a hint (the mutation) persistently. When the 
target recovers, hints are replayed from these segments.

The Rust rewrite needs durable hint persistence matching Java's semantics:
- Survive coordinator restart
- Per-endpoint isolation
- Checksum integrity
- Rotation and cleanup

## Decision

We implement append-only hint segment files with the following format:

```text
Per-entry:  [CRC32: 4 bytes LE][length: 4 bytes LE][JSON-serialized Hint: length bytes]
Filename:   {target_id}-{timestamp_ms}.hints
Directory:  {data_dir}/hints/
```

### Key design choices:

1. **JSON serialization** (not Java-compatible binary) — simpler, debuggable; 
   we don't need wire-compatible hint files since hints are local-only.

2. **CRC32 per entry** — matches Java's approach; corrupted entries are 
   skipped during replay without losing the entire segment.

3. **One segment per writer session** — `HintSegmentWriter` creates a new 
   segment file per open. Rotation at configurable size (default: 128 MiB).

4. **`HintSegmentManager`** — manages listing, deletion, and disk usage 
   tracking for all segments in the hints directory.

5. **In-memory + disk dual write** — `HintStore` (in-memory) remains primary 
   for fast access; `HintSegmentWriter` provides durability. Future work 
   will add recovery-from-disk on startup.

## Consequences

- Hint data survives coordinator crashes
- Segment cleanup is explicit (delete after delivery or expiration)
- JSON format is larger than binary but avoids custom codec complexity
- Future optimization: switch to bincode/postcard for production with 
  backward-compatible version field in `HintSegmentDescriptor`

## Alternatives Considered

1. **Binary format matching Java** — Rejected: unnecessary complexity since 
   hints never cross node boundaries.
2. **SQLite per endpoint** — Rejected: over-engineered for append-only workload.
3. **WAL-style single file** — Rejected: harder to cleanup per-endpoint.
