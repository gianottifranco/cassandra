// Licensed under Apache License, Version 2.0.

//! HNSW (Hierarchical Navigable Small World) graph for approximate nearest
//! neighbor search on vector columns.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph`
//!
//! Multi-layer graph where upper layers provide long-range jumps and the
//! bottom layer provides fine-grained neighborhoods. Level assignment uses
//! `ml * ln(random)` as in the HNSW paper.

use parking_lot::RwLock;
use std::collections::HashSet;

use super::posting::RowLocation;
use cassandra_types::vector::{SimilarityMetric, VectorValue, compute_similarity};

/// A node in the HNSW graph.
#[derive(Debug, Clone)]
struct HnswNode {
    vector: VectorValue,
    location: RowLocation,
    /// Per-layer neighbor lists: `neighbors[layer]` is the set of neighbor
    /// node IDs at that layer.
    neighbors: Vec<Vec<usize>>,
    /// Whether this node is tombstoned (logically deleted).
    deleted: bool,
}

/// Multi-layer HNSW graph for approximate kNN.
#[derive(Debug)]
pub struct HnswGraph {
    nodes: RwLock<Vec<HnswNode>>,
    dimensions: u32,
    metric: SimilarityMetric,
    /// Maximum connections per node per layer (M).
    m: usize,
    /// Maximum connections at layer 0 (M_max0).
    m_max0: usize,
    /// Level multiplier.
    ml: f64,
    /// Search beam width during construction.
    ef_construction: usize,
    /// Entry point node ID (into highest layer).
    entry_point: RwLock<Option<usize>>,
    /// Current maximum layer in the graph.
    max_layer: RwLock<usize>,
}

/// A kNN search result from the HNSW graph.
#[derive(Debug, Clone)]
pub struct HnswSearchResult {
    pub location: RowLocation,
    pub score: f32,
}

impl HnswGraph {
    /// Create a new HNSW graph.
    pub fn new(dimensions: u32, metric: SimilarityMetric) -> Self {
        Self {
            nodes: RwLock::new(Vec::new()),
            dimensions,
            metric,
            m: 16,
            m_max0: 32,
            ml: 1.0 / (16.0f64).ln(),
            ef_construction: 100,
            entry_point: RwLock::new(None),
            max_layer: RwLock::new(0),
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
                "dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.dimensions()
            ));
        }

        let node_level = self.random_level();
        let location = RowLocation {
            partition_key,
            clustering_key,
        };

        let mut nodes = self.nodes.write();
        let new_id = nodes.len();

        let node = HnswNode {
            vector: vector.clone(),
            location,
            neighbors: vec![Vec::new(); node_level + 1],
            deleted: false,
        };
        nodes.push(node);

        let entry = *self.entry_point.read();
        if entry.is_none() {
            *self.entry_point.write() = Some(new_id);
            *self.max_layer.write() = node_level;
            return Ok(());
        }
        let mut ep = entry.unwrap();

        let current_max = *self.max_layer.read();

        // Traverse from top layer to node_level+1 (greedy closest)
        for layer in (node_level + 1..=current_max).rev() {
            ep = self.greedy_closest(&nodes, &vector, ep, layer);
        }

        // For layers node_level down to 0, search and connect
        for layer in (0..=node_level.min(current_max)).rev() {
            let neighbors =
                self.search_layer_inner(&nodes, &vector, ep, self.ef_construction, layer);

            let max_conn = if layer == 0 { self.m_max0 } else { self.m };
            let selected: Vec<usize> = neighbors.iter().take(max_conn).map(|&(_, id)| id).collect();

            // Connect new node to selected neighbors
            nodes[new_id].neighbors[layer] = selected.clone();

            // Add bidirectional edges
            for &neighbor_id in &selected {
                if nodes[neighbor_id].neighbors.len() > layer {
                    nodes[neighbor_id].neighbors[layer].push(new_id);
                    // Prune if over limit
                    let limit = if layer == 0 { self.m_max0 } else { self.m };
                    if nodes[neighbor_id].neighbors[layer].len() > limit {
                        nodes[neighbor_id].neighbors[layer].truncate(limit);
                    }
                }
            }

            if let Some(&(_, first)) = neighbors.first() {
                ep = first;
            }
        }

        // Update entry point if new node has higher level
        if node_level > current_max {
            *self.entry_point.write() = Some(new_id);
            *self.max_layer.write() = node_level;
        }

        Ok(())
    }

    /// Perform approximate kNN search.
    pub fn search(&self, query: &VectorValue, ef: usize, k: usize) -> Vec<HnswSearchResult> {
        if k == 0 {
            return Vec::new();
        }

        let nodes = self.nodes.read();
        if nodes.is_empty() {
            return Vec::new();
        }

        let entry = match *self.entry_point.read() {
            Some(ep) => ep,
            None => return Vec::new(),
        };

        let max_layer = *self.max_layer.read();
        let mut ep = entry;

        // Traverse from top to layer 1
        for layer in (1..=max_layer).rev() {
            ep = self.greedy_closest(&nodes, query, ep, layer);
        }

        // Search at layer 0 with ef
        let candidates = self.search_layer_inner(&nodes, query, ep, ef.max(k), 0);

        candidates
            .into_iter()
            .filter(|&(_, id)| !nodes[id].deleted)
            .take(k)
            .map(|(score, id)| HnswSearchResult {
                location: nodes[id].location.clone(),
                score,
            })
            .collect()
    }

    /// Mark all nodes for a given row location as deleted (tombstone).
    pub fn delete(&self, partition_key: &[u8], clustering_key: &[u8]) {
        let mut nodes = self.nodes.write();
        for node in nodes.iter_mut() {
            if node.location.partition_key == partition_key
                && node.location.clustering_key == clustering_key
            {
                node.deleted = true;
            }
        }
    }

    /// Number of live (non-deleted) nodes.
    pub fn count(&self) -> usize {
        self.nodes.read().iter().filter(|n| !n.deleted).count()
    }

    // ─── Internal ───────────────────────────────────────────────────────

    fn random_level(&self) -> usize {
        let r: f64 = rand_f64();
        let level = (-r.ln() * self.ml).floor() as usize;
        level.min(10) // cap max level
    }

    fn greedy_closest(
        &self,
        nodes: &[HnswNode],
        query: &VectorValue,
        mut ep: usize,
        layer: usize,
    ) -> usize {
        let mut best_score = compute_similarity(&nodes[ep].vector, query, self.metric);
        let mut improved = true;

        while improved {
            improved = false;
            if layer < nodes[ep].neighbors.len() {
                for &neighbor in &nodes[ep].neighbors[layer] {
                    let score = compute_similarity(&nodes[neighbor].vector, query, self.metric);
                    if self.is_better(score, best_score) {
                        best_score = score;
                        ep = neighbor;
                        improved = true;
                    }
                }
            }
        }

        ep
    }

    fn search_layer_inner(
        &self,
        nodes: &[HnswNode],
        query: &VectorValue,
        entry: usize,
        ef: usize,
        layer: usize,
    ) -> Vec<(f32, usize)> {
        let mut visited = HashSet::new();
        visited.insert(entry);

        let ep_score = compute_similarity(&nodes[entry].vector, query, self.metric);
        let mut candidates = vec![(ep_score, entry)];
        let mut best_results = vec![(ep_score, entry)];

        while !candidates.is_empty() {
            let (c_score, c_id) = candidates.remove(0);

            let worst_best = best_results.last().unwrap().0;
            if best_results.len() == ef && !self.is_better(c_score, worst_best) {
                break;
            }

            if layer < nodes[c_id].neighbors.len() {
                for &neighbor in &nodes[c_id].neighbors[layer] {
                    if visited.insert(neighbor) {
                        let n_score =
                            compute_similarity(&nodes[neighbor].vector, query, self.metric);

                        let is_better_than_worst =
                            self.is_better(n_score, best_results.last().unwrap().0);
                        if best_results.len() < ef || is_better_than_worst {
                            self.insert_sorted(&mut candidates, (n_score, neighbor));
                            self.insert_sorted(&mut best_results, (n_score, neighbor));

                            if best_results.len() > ef {
                                best_results.pop();
                            }
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
}

/// Simple pseudo-random f64 in (0, 1) using thread-local state.
fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0x12345678_9abcdef0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x as f64) / (u64::MAX as f64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_graph() -> HnswGraph {
        HnswGraph::new(3, SimilarityMetric::Euclidean)
    }

    #[test]
    fn insert_and_count() {
        let g = make_graph();
        g.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![0.0, 1.0, 0.0]),
            b"pk2".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        assert_eq!(g.count(), 2);
    }

    #[test]
    fn dimension_mismatch() {
        let g = make_graph();
        let result = g.insert(
            VectorValue::new(vec![1.0, 2.0]),
            b"pk".to_vec(),
            b"".to_vec(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn search_empty_graph() {
        let g = make_graph();
        let results = g.search(&VectorValue::new(vec![1.0, 0.0, 0.0]), 50, 5);
        assert!(results.is_empty());
    }

    #[test]
    fn knn_basic() {
        let g = make_graph();
        g.insert(
            VectorValue::new(vec![0.0, 0.0, 0.0]),
            b"origin".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"x".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![10.0, 10.0, 10.0]),
            b"far".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        let query = VectorValue::new(vec![0.0, 0.0, 0.0]);
        let results = g.search(&query, 50, 2);

        assert_eq!(results.len(), 2);
        // Origin should be closest (euclidean distance = 0)
        assert_eq!(results[0].location.partition_key, b"origin");
        assert!(results[0].score < 0.001);
    }

    #[test]
    fn delete_tombstones_node() {
        let g = make_graph();
        g.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk1".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![0.0, 1.0, 0.0]),
            b"pk2".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        g.delete(b"pk1", b"");
        assert_eq!(g.count(), 1);
    }

    #[test]
    fn search_k_zero() {
        let g = make_graph();
        g.insert(
            VectorValue::new(vec![1.0, 0.0, 0.0]),
            b"pk".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        let results = g.search(&VectorValue::new(vec![1.0, 0.0, 0.0]), 50, 0);
        assert!(results.is_empty());
    }

    #[test]
    fn cosine_search() {
        let g = HnswGraph::new(2, SimilarityMetric::Cosine);
        g.insert(
            VectorValue::new(vec![1.0, 0.0]),
            b"east".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![0.0, 1.0]),
            b"north".to_vec(),
            b"".to_vec(),
        )
        .unwrap();
        g.insert(
            VectorValue::new(vec![-1.0, 0.0]),
            b"west".to_vec(),
            b"".to_vec(),
        )
        .unwrap();

        let results = g.search(&VectorValue::new(vec![1.0, 0.0]), 50, 1);
        assert_eq!(results[0].location.partition_key, b"east");
    }
}
