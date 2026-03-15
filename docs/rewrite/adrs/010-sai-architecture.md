# ADR-010: SAI (Storage Attached Indexing) Architecture

**Status**: Accepted
**Date**: 2026-03-15
**Context**: Cassandra Rust Rewrite Phase 3

## Decision

Implement Storage Attached Indexes matching Java's `org.apache.cassandra.index.sai`,
with deep integration into the storage engine write, read, and compaction paths.

### Architecture

```
Write Path: mutation → memtable + SAI memory index
Flush:      memtable → SSTable + SAI segment (per-SSTable)
Read:       SAI query → posting lists → base table lookup
Compaction:  merge SSTables → merge SAI segments
Streaming:  SSTable + SAI segment transferred as unit
```

### Index Abstraction

- `SecondaryIndex` trait with insert/delete/search/range_search/truncate.
- `IndexManager` routes write-path notifications to relevant indexes.
- Two backends: `LegacyIndex` (inverted table) and `SaiIndex` (posting lists).

### SAI Internals

- **PostingList**: sorted `(partition_key, clustering_key)` for a term.
  Supports merge (union) and intersect operations.
- **SaiSegment**: per-SSTable index data (term → PostingList mapping).
- **SaiSegmentBuilder**: accumulates entries during flush/compaction.
- **Query engine**: supports Eq and Range predicates with AND/OR composition.

### Vector Index

- Vectors stored as `CqlType::Vector(inner, dims)`.
- SAI supports vector indexing via same posting-list infrastructure.
- Current search: brute-force kNN (scan all vectors, compute similarity).
- Similarity functions: cosine, euclidean, dot product.

## Known Limitations (TODO)

1. **On-disk persistence**: SAI segments are in-memory only. Need to persist alongside SSTables.
2. **Trie-based term dictionary**: Using BTreeMap; need compressed trie for memory efficiency.
3. **Bloom filter**: No fast non-match elimination yet.
4. **Compaction integration**: `merge_segments()` exists but not wired to compaction lifecycle.
5. **Streaming integration**: SAI segments not included in stream plans yet.
6. **ANN for vectors**: Brute-force only; need HNSW or IVF for production scale.

## Consequences

- Index-filtered queries are functional for equality and range predicates.
- SAI compaction merging is available via `merge_segments()`.
- Vector search works but is O(n) — acceptable for correctness validation, not production ANN.

## Alternatives Considered

- **External index (Lucene-like)**: Adds external dependency; SAI's storage-attached model is more Cassandra-native.
- **LSM-based index**: Could work but adds complexity; posting lists are simpler for initial implementation.
