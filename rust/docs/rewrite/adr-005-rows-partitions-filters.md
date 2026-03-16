# ADR-005: Rows, Partitions, Filters, Transforms, Tries

## Status

Accepted (Prompt 05)

## Context

The storage crate had simplified `Cell`/`Row`/`PartitionData` structs with flat `Vec<Cell>`, raw bytes for clustering keys, basic tombstones, and no transformation pipeline. The read path lacked rich filtering (column selection, clustering slices, row expressions) and the trie implementation was a single custom `TrieNode` with no generic cursor interface.

Three gaps from `gap_guards.rs` needed closing:
- Trie index (`db.tries`)
- Row/partition filters (`db.filter`)
- Row transformations (`db.transform`)

## Decision

### 1. Coexistence over replacement

New rich types (`RowData`, `CellData`, `PartitionUpdate`, etc.) coexist with existing simplified types. `From`/`Into` conversions bridge the two. This avoids breaking existing code while enabling incremental migration.

### 2. Module structure

Five new top-level modules in `cassandra-storage`:
- `rows/` — LivenessInfo, CellData, ComplexColumnData, Unfiltered, RowData, RangeTombstoneMarker, MutableDeletionInfo
- `partitions/` — DecoratedKey, PartitionUpdate, iterator traits, FilteredPartition
- `filter/` — ColumnFilter, ClusteringIndexFilter, RowFilter, DataLimits
- `transform/` — Transformation trait, FilteredRows/FilteredPartitions, PurgeTransform, LimitsTransform, FilterTransform, DuplicateRowChecker, RTBoundCloser
- `tries/` — Generic Trie trait with Cursor, InMemoryTrie, MergeTrie, MemtableTrie

### 3. Iterator abstraction

The `UnfilteredRowIterator` trait yields `Unfiltered` items (rows + range tombstone markers) in clustering order. This matches Java's `db.rows.UnfilteredRowIterator` and is the fundamental unit of the read path. Transformations compose as wrappers over this iterator.

### 4. ShardedMemtable

Token space is divided into N shards, each with its own `MemtableBackend` and lock. This eliminates the single coarse `RwLock` bottleneck.

### 5. Generic trie

`InMemoryTrie<T>` uses `BTreeMap` children for simplicity and sorted iteration. The cursor-based `Trie<T>` trait allows future optimization (inline small children, arena allocation) without API changes.

## Consequences

- Existing `memtable::partition` code remains untouched — zero regression risk
- New modules compile and test independently
- Read path can now express: "fetch columns A,B,C; filter rows where X > 5; limit to 100 rows; purge old tombstones" as a composable pipeline
- `ShardedTrie` variant registered in `MemtableType` enum
- Three gap guards closed in `cassandra-diff-tests`
