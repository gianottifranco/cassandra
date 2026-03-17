# ADR-017: SAI, SASI, and Vector Search Architecture

## Status

Accepted

## Context

The Cassandra Java→Rust rewrite requires secondary indexing support covering
three index types: Storage Attached Indexes (SAI), SASI (experimental), and
vector search. The gap analysis identified ~5% coverage for the index
execution engine, 0% for SASI, and missing CQL vector functions.

## Decision

### SAI Architecture

SAI maintains per-SSTable index segments that co-locate with data:

- **Posting lists**: Sorted `RowLocation` vectors mapping terms to base-table
  rows. Supports merge (union) and intersect operations.
- **Segments**: Built during memtable flush via `SaiSegmentBuilder`, merged
  during compaction via `merge_segments`.
- **On-disk format**: Binary format (`SAI1` magic, version, term dictionary
  with posting lists). Read/write via `segment_format.rs`.
- **Bloom filters**: Per-segment bloom filter for fast term non-existence
  checks, using double-hashing with FNV-1a.
- **Vector index**: Dual implementation — brute-force NSW graph (existing)
  plus multi-layer HNSW graph (new). HNSW provides logarithmic search time
  with `ml * ln(random)` level selection.

### SASI Classification

SASI is classified as experimental and gated behind the `sasi` feature flag:

- **Analyzers**: Pluggable `Analyzer` trait with `StandardAnalyzer`
  (word-boundary tokenization + lowercasing) and `NonTokenizingAnalyzer`
  (whole-value, optional lowercasing).
- **In-memory index**: BTreeMap-based with analyzer-driven term indexing.
  Supports exact match and range search.
- **Feature gating**: `#[cfg(feature = "sasi")]` on the module. Executor
  rejects SASI index creation when the feature is disabled.

### Vector Search Design

- **CQL functions**: `similarity_cosine`, `similarity_euclidean`,
  `similarity_dot_product` registered in `FunctionRegistry::with_builtins()`.
  Each deserializes big-endian f32 arrays and delegates to
  `cassandra_types::vector::*`.
- **ANN planner**: `SelectPlan` carries an optional `AnnClause` with column,
  vector literal, and top_k. Requires LIMIT clause.
- **HNSW upgrade**: Multi-layer graph with configurable M, M_max0,
  ef_construction. Layer traversal from top to bottom, proper bidirectional
  edge management.

### Operational Infrastructure

- **Index metrics**: `IndexMetrics` with AtomicU64 counters (inserts, deletes,
  searches, range_searches, vector_searches, latency, build time).
- **Admin endpoints**: `GET /admin/indexes` (list all), `GET /admin/indexes/{name}`
  (detail with type, column, status, options).
- **Config validation**: `StorageAttachedIndexOptions::validate()` enforces
  ranges (segment_buffer_size 1MiB-1GiB, max_terms_per_segment 1024-1M,
  max_vector_dimensions ≤ 8192). `validate_index_definition()` checks CREATE
  INDEX option maps.
- **Compaction wiring**: `CompactionResult` carries `sai_segments` for SAI
  index data produced during compaction.

## Known Limitations

- HNSW graph is in-memory only (no disk persistence yet).
- ANN clause detection requires AST-level parser support (placeholder today).
- SASI does not yet support prefix/contains search modes.
- Bloom filter uses FNV-1a, not murmur3 (Java compatibility deferred).
- No integration with streaming for SAI segments yet.

## Future Work

- Trie-based term dictionary for memory efficiency.
- Disk-based HNSW persistence and mmap access.
- Full ANN OF syntax in the CQL parser.
- SASI prefix and contains search modes.
- SAI segment streaming during bootstrap/repair.
