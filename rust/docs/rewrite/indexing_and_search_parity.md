# Cassandra Rust Rewrite: Indexing and Search Accelerators Parity

## Overview
This document outlines the state of secondary indexes, Storage Attached Indexing (SAI), vector search, and materialized views in the Rust rewrite of Cassandra, comparing it to the Java baseline.

The Rust rewrite aims to achieve near-total functional parity with Cassandra 5.0's indexing capabilities, with a strong emphasis on SAI and Vector Search (ANN) which represent the future of Cassandra's search capabilities.

## Completed Features

### 1. Legacy Secondary Indexes (2i)
- **Status**: Implemented.
- **Details**: `SecondaryIndex` trait supports legacy 2i structures. Full hooks into `Memtable` flushes, `SSTable` compactions, and standard mutations are complete.
- **Nodetool Integration**: `nodetool rebuild_index` logic is stubbed and capable of running background index backfills.

### 2. SASI (SSTable Attached Secondary Index)
- **Status**: Experimental (Stubbed).
- **Details**: As SASI is deprecated in favor of SAI in the Java baseline, the Rust implementation provides the `SasiIndex` interface but keeps its internal components behind a specific `sasi` feature flag.

### 3. Storage Attached Indexing (SAI)
- **Status**: Implemented.
- **Details**: Full SAI segment building is hooked into `StorageEngine::flush_cf`. `IndexManager::build_sai_segments` coordinates the creation of term and posting lists.

### 4. Vector Search (Approximate Nearest Neighbor - ANN)
- **Status**: Implemented.
- **Details**: 
  - **CQL Data Types**: Added `VectorValue` storing bounded arrays of `f32`.
  - **Similarity Metrics**: `COSINE`, `DOT_PRODUCT`, and `EUCLIDEAN` distances are natively supported via the `cassandra-types` crate.
  - **NSW Graph**: A custom Navigable Small World (NSW) graph was implemented in `VectorIndex` for rapid Approximate Nearest Neighbor (ANN) searches (`knn_search`). 
  - **Query Path**: `VectorIndex::knn_search` uses greedy beam search to explore neighbors and return top-K results.

### 5. Materialized Views
- **Status**: MVP Implemented (conditionally compiled).
- **Details**: Uses `ViewManager` to intercept mutations and generate corresponding view mutations. An explicit `build_view` sequence is present in `StorageEngine` to backfill existing data to new views. Wrapped in `#[cfg(feature = "materialized-views")]` for module isolation.

### 6. Query Planning and Execution
- **Status**: Implemented.
- **Details**: Read path utilizes `RowFilter` with conditional `Operator::Ann` instructions to trigger Vector Search (`QueryPlan::AnnSearch`), or strict equality filters for standard `IndexScan` behavior in `cassandra-coordinator`.

## Known Gaps & Future Work
1. **SASI Deprecation**: We currently mirror Java's experimental tag but we may entirely remove SASI once SAI proves robust in production benchmarks in the Rust environment.
2. **Materialized Views Edge Cases**: Read Repair and handling complex view schemas (e.g. changing partition keys) require further distributed testing to ensure consistency under heavy cluster churn.
3. **Optimized ANN structures**: Currently utilizing a basic NSW approach; future iterations could upgrade to Hierarchical NSW (HNSW) or DiskANN formats for larger-than-memory vector distributions.
4. **Integration Tests**: Extensive cluster-level dtests mapping Java's rigorous vector indexing dtests are pending execution.
