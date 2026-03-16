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
//! - [ ] Integrate with compaction: rebuild vector index on merge

use parking_lot::RwLock;
use std::collections::HashSet;

use super::posting::RowLocation;
use cassandra_types::vector::{SimilarityMetric, VectorValue, compute_similarity};

/// A vector stored alongside its row location.
#[derive(Debug, Clone)]
struct StoredVector {
    vector: VectorValue,
    location: RowLocation,
    edges: Vec<usize>,
}

/// A Navigable Small World (NSW) graph-based vector index for SAI.
///
/// Maps serialized vector bytes → (VectorValue, RowLocation).
/// Uses a simple nearest-neighbor graph for approximate searching.
#[derive(Debug)]
pub struct VectorIndex {
    /// All stored vectors and their edges.
    vectors: RwLock<Vec<StoredVector>>,
    /// Number of dimensions (fixed for a column).
    dimensions: u32,
    /// Similarity metric to use for queries.
    metric: SimilarityMetric,
    /// Max connections per node (M).
    m: usize,
    /// Search beam width during construction.
    ef_construction: usize,
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
            m: 16,
            ef_construction: 100,
        }
    }

    /// Number of dimensions for vectors in this index.
    pub fn dimensions(&self) -> u32 {
        self.dimensions
    }

    /// Insert a vector with its base-table location into the NSW graph.
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

        let location = RowLocation {
            partition_key,
            clustering_key,
        };
        let mut vectors = self.vectors.write();
        let new_id = vectors.len();

        let mut node = StoredVector {
            vector: vector.clone(),
            location,
            edges: Vec::new(),
        };

        if new_id == 0 {
            // First node is the entry point
            vectors.push(node);
            return Ok(());
        }

        // Search for nearest neighbors to connect to
        let neighbors = self.search_layer(&vectors, &vector, self.ef_construction);

        // Take top M neighbors
        let mut top_m = neighbors;
        top_m.truncate(self.m);

        for &(_score, neighbor_id) in &top_m {
            node.edges.push(neighbor_id);
        }

        vectors.push(node);

        // Add bidirectional edges
        for &(_, neighbor_id) in &top_m {
            if vectors[neighbor_id].edges.len() < self.m {
                vectors[neighbor_id].edges.push(new_id);
            } else {
                // Simplified: just push it instead of pruning, or skip
                // For an MVP, we just allow slightly larger edge lists.
                vectors[neighbor_id].edges.push(new_id);
            }
        }

        Ok(())
    }

    /// Remove all vectors for a given row location.
    pub fn delete(&self, partition_key: &[u8], clustering_key: &[u8]) {
        // Deleting from an NSW graph requires rebuilding or tombstoning.
        // For this MVP, we tombstone by clearing edges and locations,
        // but removing nodes shifts indices which breaks edges.
        let mut vectors = self.vectors.write();
        for node in vectors.iter_mut() {
            if node.location.partition_key == partition_key
                && node.location.clustering_key == clustering_key
            {
                node.edges.clear();
                node.location.partition_key.clear();
                node.location.clustering_key.clear();
            }
        }
        // In a real implementation, we would maintain a freelist or rebuild.
    }

    /// Perform Approximate k-nearest-neighbor search (NSW greedy beam search).
    pub fn knn_search(&self, query: &VectorValue, k: usize) -> Vec<VectorSearchResult> {
        if k == 0 {
            return Vec::new();
        }

        let vectors = self.vectors.read();
        if vectors.is_empty() {
            return Vec::new();
        }

        // Use a beam search with width max(k, ef)
        let ef = k.max(50);
        let top_k = self.search_layer(&vectors, query, ef);

        let mut results = Vec::new();
        for (score, id) in top_k.into_iter().take(k) {
            let sv = &vectors[id];
            // Skip tombstoned nodes
            if sv.location.partition_key.is_empty() {
                continue;
            }
            results.push(VectorSearchResult {
                location: sv.location.clone(),
                score,
            });
        }

        results
    }

    /// Helper for NSW greedy beam search. Returns sorted list of (score, id)
    /// where index 0 is the best score according to `self.metric`.
    fn search_layer(
        &self,
        vectors: &[StoredVector],
        query: &VectorValue,
        ef: usize,
    ) -> Vec<(f32, usize)> {
        let entry_point = 0; // always start at 0
        let mut visited = HashSet::new();
        visited.insert(entry_point);

        let ep_score = compute_similarity(&vectors[entry_point].vector, query, self.metric);
        let mut candidates = vec![(ep_score, entry_point)];
        let mut best_results = vec![(ep_score, entry_point)];

        while !candidates.is_empty() {
            // Extract best candidate to explore
            let (c_score, c_id) = candidates.remove(0);

            // If the best candidate is worse than the worst in our best_results (and we have ef results), stop
            let worst_best = best_results.last().unwrap().0;
            if best_results.len() == ef && !self.is_better(c_score, worst_best) {
                break;
            }

            for &neighbor_id in &vectors[c_id].edges {
                if visited.insert(neighbor_id) {
                    let n_score =
                        compute_similarity(&vectors[neighbor_id].vector, query, self.metric);

                    let is_better_than_worst =
                        self.is_better(n_score, best_results.last().unwrap().0);
                    if best_results.len() < ef || is_better_than_worst {
                        // Insert into candidates and best_results, maintaining sort order
                        self.insert_sorted(&mut candidates, (n_score, neighbor_id));
                        self.insert_sorted(&mut best_results, (n_score, neighbor_id));

                        if best_results.len() > ef {
                            best_results.pop(); // Remove worst
                        }
                    }
                }
            }
        }

        best_results
    }

    fn is_better(&self, a: f32, b: f32) -> bool {
        match self.metric {
            SimilarityMetric::Cosine | SimilarityMetric::DotProduct => a > b,
            SimilarityMetric::Euclidean => a < b,
        }
    }

    fn insert_sorted(&self, list: &mut Vec<(f32, usize)>, item: (f32, usize)) {
        let pos = list.partition_point(|x| self.is_better(x.0, item.0));
        list.insert(pos, item);
    }

    /// Number of live indexed vectors.
    pub fn count(&self) -> usize {
        self.vectors
            .read()
            .iter()
            .filter(|v| !v.location.partition_key.is_empty())
            .count()
    }

    /// Clear all indexed vectors.
    pub fn truncate(&self) {
        self.vectors.write().clear();
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
        )
        .unwrap();
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
        idx.insert(
            VectorValue::new(vec![0.0, 0.0, 0.0]),
            b"origin".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"x_axis".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![10.0, 10.0, 10.0]),
            b"far".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![0.5, 0.5, 0.0]),
            b"near".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

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

        idx.insert(
            VectorValue::new(vec![1.0, 0.0]),
            b"east".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![0.0, 1.0]),
            b"north".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![-1.0, 0.0]),
            b"west".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        // Query: [1, 0] — most similar should be "east" (cos=1.0)
        let results = idx.knn_search(&VectorValue::new(vec![1.0, 0.0]), 2);
        assert_eq!(results[0].location.partition_key, b"east");
        assert!((results[0].score - 1.0).abs() < 0.001);
    }

    #[test]
    fn knn_dot_product() {
        let idx = VectorIndex::new(2, SimilarityMetric::DotProduct);

        idx.insert(
            VectorValue::new(vec![10.0, 0.0]),
            b"big".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0]),
            b"small".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

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
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        let results = idx.knn_search(&VectorValue::new(vec![1.0, 0.0, 0.0]), 0);
        assert!(results.is_empty());
    }

    #[test]
    fn delete_removes_vector() {
        let idx = make_index();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.insert(
            VectorValue::new(vec![0.0, 1.0, 0.0]),
            b"pk2".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        idx.delete(b"pk1", b"");
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn truncate_clears_all() {
        let idx = make_index();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        idx.truncate();
        assert_eq!(idx.count(), 0);
    }

    #[test]
    fn knn_k_larger_than_index() {
        let idx = make_index();
        idx.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        let results = idx.knn_search(&VectorValue::new(vec![0.0, 0.0, 0.0]), 100);
        assert_eq!(results.len(), 1); // Only 1 vector in index
    }
}
