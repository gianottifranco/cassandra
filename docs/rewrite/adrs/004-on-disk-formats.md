# ADR-004: On-Disk Format Treatment

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

Cassandra's on-disk formats are critical for data durability and cluster
interoperability. The main formats are:

1. **SSTables** (mc/nb/big format families) – data, index, filter, stats,
   compression info, digest files.
2. **CommitLog segments** – WAL for durability.
3. **Hints files** – hinted handoff data.
4. **Schema tables** – system keyspace SSTables.
5. **Snapshot manifests** – JSON metadata for snapshots.

Cassandra Java nodes in a cluster must be able to read data written by Rust
nodes and vice versa during rolling upgrades and mixed-cluster operation.

## Decision

### Read Compatibility (MUST – Phase 4+)

The Rust implementation **must** read all SSTable formats produced by the
baseline Java version:
- `big` format (legacy, still common).
- `nb` format (post-3.0).
- SSTables with all supported compression algorithms (LZ4, Snappy, Zstd,
  Deflate, no-op).

Implementation approach:
- Port the SSTable reader logic faithfully.
- Golden test: Java writes SSTable → Rust reads → compare cell-by-cell.
- Use `memmap2` for I/O where beneficial; fallback to buffered I/O.

### Write Format (MAY evolve)

The Rust implementation **may** write a new SSTable format (`rs` format) if:
- The new format is documented.
- A Java reader for the `rs` format is provided OR the Rust writer can also
  produce `nb` format for mixed-cluster compatibility.
- Conversion tools exist in both directions.

**Initial strategy**: write `nb`-compatible format to avoid mixed-cluster
issues. Introduce `rs` format only when performance data justifies it.

### CommitLog (MUST read, MAY write differently)

- Must read existing CommitLog segments for crash recovery.
- May use a different WAL format internally (e.g., append-only log with
  CRC32C per entry, similar structure but Rust-native serialization).
- If the format changes, provide a migration tool.

### Hints (MUST read)

- Must read existing hints files to replay them.
- May write hints in a new format, with a version header for detection.

### Byte-Order and Encoding

- Maintain big-endian encoding for wire and disk formats (matching Java's
  `DataOutput`/`DataInput`).
- Use `byteorder` crate for explicit endianness.
- Never rely on platform endianness.

## Consequences

- Need SSTable format documentation (the Java code IS the documentation today).
- Must write comprehensive SSTable round-trip tests.
- May need a `sstable-convert` tool for format migration.
- Performance optimization of write path is gated on `nb` format stability.
