// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! # SAI Vector Index — brute-force kNN search for vector columns.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph`
//!
//! ## Design
//!
//! This module provides brute-force k-nearest-neighbor (kNN) search
//! over vectors stored in SAI posting lists.  Each vector is associated
//! with a base-table row location.
//!
//! ## Current Limitations
//!
//! - **Brute-force only**: O(n) scan per query.  No approximate NN
//!   structures (HNSW, IVF) yet.
//! - **In-memory**: Vectors are stored in RAM alongside posting lists.
//! - **Float32 only**: The CQL `vector<float, n>` type maps to f32.
//!
//! ## TODO
//!
//! - [ ] Implement HNSW graph for sub-linear ANN queries
//! - [ ] On-disk vector segment format co-located with SSTables
//! - [ ] Quantization (PQ/SQ) for memory reduction
//! - [ ] Integrate with compaction: rebuild vector index on merge

use std::collections::BTreeMap;
use parking_lot::RwLock;

use cassandra_types::vector::{VectorValue, SimilarityMetric, compute_similarity};
use super::posting::RowLocation;

/// A vector stored alongside its row location.
#[derive(Debug, Clone)]
struct StoredVector {
    vector: VectorValue,
    location: RowLocation,
}

/// A brute-force vector index for SAI.
///
/// Maps serialized vector bytes → (VectorValue, RowLocation).
/// Supports kNN queries with configurable similarity metric.
#[derive(Debug)]
pub struct VectorIndex {
    /// All stored vectors, keyed by their serialized form for dedup.
    vectors: RwLock<Vec<StoredVector>>,
    /// Number of dimensions (fixed for a column).
    dimensions: u32,
    /// Similarity metric to use for queries.
    metric: SimilarityMetric,
}

/// A kNN search result.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// Base-table row location.
    pub location: RowLocation,
    /// Similarity/distance score.
    pub score: f32,
}

impl VectorIndex {
    /// Create a new vector index for vectors of the given dimensionality.
    pub fn new(dimensions: u32, metric: SimilarityMetric) -> Self {
        Self {
            vectors: RwLock::new(Vec::new()),
            dimensions,
            metric,
        }
    }

    /// Insert a vector with its base-table location.
    pub fn insert(
        &self,
        vector: VectorValue,
        partition_key: Vec<u8>,
        clustering_key: Vec<u8>,
    ) -> Result<(), String> {
        if vector.dimensions() != self.dimensions {
            return Err(format!(
                "Dimension mismatch: index expects {}, got {}",
                self.dimensions,
                vector.dimensions()
            ));
        }

        let location = RowLocation { partition_key, clustering_key };
        self.vectors.write().push(StoredVector { vector, location });
        Ok(())
    }

    /// Remove all vectors for a given row location.
    pub fn delete(&self, partition_key: &[u8], clustering_key: &[u8]) {
        self.vectors.write().retain(|sv| {
            sv.location.partition_key != partition_key
                || sv.location.clustering_key != clustering_key
        });
    }

    /// Perform brute-force k-nearest-neighbor search.
    ///
    /// Returns the `k` closest vectors sorted by relevance:
    /// - For cosine/dot-product: highest score first (most similar).
    /// - For euclidean: lowest score first (closest).
    pub fn knn_search(&self, query: &VectorValue, k: usize) -> Vec<VectorSearchResult> {
        if k == 0 {
            return Vec::new();
        }

        let vectors = self.vectors.read();
        let mut results: Vec<VectorSearchResult> = vectors
            .iter()
            .map(|sv| {
                let score = compute_similarity(&sv.vector, query, self.metric);
                VectorSearchResult {
                    location: sv.location.clone(),
                    score,
                }
            })
            .collect();

        // Sort by relevance
        match self.metric {
            SimilarityMetric::Cosine | SimilarityMetric::DotProduct => {
                // Higher is better — sort descending
                results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            }
            SimilarityMetric::Euclidean => {
                // Lower is better — sort ascending
                results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal));
            }
        }

        results.truncate(k);
        results
    }

    /// Number of indexed vectors.
    pub fn count(&self) -> usize {
        self.vectors.read().len()
    }

    /// Clear all indexed vectors.
    pub fn truncate(&self) {
        self.vectors.write().clear();
    }

    /// Get the configured dimensionality.
    pub fn dimensions(&self) -> u32 {
        self.dimensions
    }

    /// Get the configured similarity metric.
    pub fn metric(&self) -> SimilarityMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> VectorIndex {
        VectorIndex::new(3, SimilarityMetric::Euclidean)
    }

    #[test]
    fn insert_and_count() {
        let idx = make_index();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        ).unwrap();
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn insert_wrong_dimensions_fails() {
        let idx = make_index();
        let result = idx.insert(
            VectorValue::new(vec![1.0, 2.0]), // 2D, not 3D
            b"pk1".to_vec(),
            b"".to_vec(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn knn_euclidean_basic() {
        let idx = make_index();

        // Insert 4 vectors
        idx.insert(VectorValue::new(vec![0.0, 0.0, 0.0]), b"origin".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![1.0, 0.0, 0.0]), b"x_axis".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![10.0, 10.0, 10.0]), b"far".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![0.5, 0.5, 0.0]), b"near".to_vec(), b"".to_vec()).unwrap();

        // Query near origin — should return origin, then near, then x_axis
        let query = VectorValue::new(vec![0.0, 0.0, 0.0]);
        let results = idx.knn_search(&query, 3);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].location.partition_key, b"origin");
        assert!(results[0].score < 0.001); // distance ~0

        // "far" should NOT be in top 3 when we only ask for 3
        // (it's the 4th closest)
        let far_in_results = results.iter().any(|r| r.location.partition_key == b"far");
        assert!(!far_in_results);
    }

    #[test]
    fn knn_cosine() {
        let idx = VectorIndex::new(2, SimilarityMetric::Cosine);

        idx.insert(VectorValue::new(vec![1.0, 0.0]), b"east".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![0.0, 1.0]), b"north".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![-1.0, 0.0]), b"west".to_vec(), b"".to_vec()).unwrap();

        // Query: [1, 0] — most similar should be "east" (cos=1.0)
        let results = idx.knn_search(&VectorValue::new(vec![1.0, 0.0]), 2);
        assert_eq!(results[0].location.partition_key, b"east");
        assert!((results[0].score - 1.0).abs() < 0.001);
    }

    #[test]
    fn knn_dot_product() {
        let idx = VectorIndex::new(2, SimilarityMetric::DotProduct);

        idx.insert(VectorValue::new(vec![10.0, 0.0]), b"big".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![1.0, 0.0]), b"small".to_vec(), b"".to_vec()).unwrap();

        let results = idx.knn_search(&VectorValue::new(vec![1.0, 0.0]), 2);
        // Higher dot product first
        assert_eq!(results[0].location.partition_key, b"big");
    }

    #[test]
    fn knn_empty_index() {
        let idx = make_index();
        let results = idx.knn_search(&VectorValue::new(vec![1.0, 2.0, 3.0]), 5);
        assert!(results.is_empty());
    }

    #[test]
    fn knn_k_zero() {
        let idx = make_index();
        idx.insert(VectorValue::new(vec![1.0, 0.0, 0.0]), b"pk".to_vec(), b"".to_vec()).unwrap();
        let results = idx.knn_search(&VectorValue::new(vec![1.0, 0.0, 0.0]), 0);
        assert!(results.is_empty());
    }

    #[test]
    fn delete_removes_vector() {
        let idx = make_index();
        idx.insert(VectorValue::new(vec![1.0, 0.0, 0.0]), b"pk1".to_vec(), b"".to_vec()).unwrap();
        idx.insert(VectorValue::new(vec![0.0, 1.0, 0.0]), b"pk2".to_vec(), b"".to_vec()).unwrap();

        idx.delete(b"pk1", b"");
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn truncate_clears_all() {
        let idx = make_index();
        idx.insert(VectorValue::new(vec![1.0, 0.0, 0.0]), b"pk1".to_vec(), b"".to_vec()).unwrap();
        idx.truncate();
        assert_eq!(idx.count(), 0);
    }

    #[test]
    fn knn_k_larger_than_index() {
        let idx = make_index();
        idx.insert(VectorValue::new(vec![1.0, 0.0, 0.0]), b"pk1".to_vec(), b"".to_vec()).unwrap();

        let results = idx.knn_search(&VectorValue::new(vec![0.0, 0.0, 0.0]), 100);
        assert_eq!(results.len(), 1); // Only 1 vector in index
    }
}
