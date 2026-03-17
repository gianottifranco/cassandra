// Licensed under Apache License, Version 2.0.

//! RepairJob: per-table multi-replica comparison and sync plan generation.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.RepairJob`
//!
//! ## Design
//!
//! A `RepairJob` takes validated Merkle trees from all replicas for a single
//! table, performs all-pairs diff, and produces `SyncTaskDescriptor`s that
//! describe what data needs to be streamed between which endpoints.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::merkle::MerkleTree;

/// Result of diffing two replicas' trees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffResult {
    /// First endpoint in the comparison.
    pub endpoint_a: Endpoint,
    /// Second endpoint in the comparison.
    pub endpoint_b: Endpoint,
    /// Ranges that differ between the two replicas.
    pub mismatched_ranges: Vec<(Token, Token)>,
}

/// Descriptor for a sync task that needs to be executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTaskDescriptor {
    /// Source endpoint to stream from.
    pub source: Endpoint,
    /// Target endpoint to stream to.
    pub target: Endpoint,
    /// Ranges that need to be synced.
    pub ranges: Vec<(Token, Token)>,
    /// Keyspace being repaired.
    pub keyspace: String,
    /// Table being repaired.
    pub table: String,
}

/// Aggregated result of a repair job.
#[derive(Debug, Clone)]
pub struct RepairJobResult {
    /// Number of trees received and compared.
    pub trees_received: usize,
    /// Pairwise diff results.
    pub diffs: Vec<DiffResult>,
    /// Sync tasks to execute.
    pub sync_tasks: Vec<SyncTaskDescriptor>,
}

/// Compute all-pairs diffs of Merkle trees from replicas.
///
/// Each pair of endpoints is compared exactly once.
pub fn compute_diffs(trees: &HashMap<Endpoint, MerkleTree>) -> Vec<DiffResult> {
    let endpoints: Vec<&Endpoint> = trees.keys().collect();
    let mut diffs = Vec::new();

    for i in 0..endpoints.len() {
        for j in (i + 1)..endpoints.len() {
            let a = endpoints[i];
            let b = endpoints[j];
            let tree_a = &trees[a];
            let tree_b = &trees[b];
            let mismatched = tree_a.diff(tree_b);

            if !mismatched.is_empty() {
                diffs.push(DiffResult {
                    endpoint_a: *a,
                    endpoint_b: *b,
                    mismatched_ranges: mismatched,
                });
            }
        }
    }

    diffs
}

/// Generate sync task descriptors from diff results.
///
/// For each pair with mismatches, creates a bidirectional sync task
/// (each side streams to the other to converge).
pub fn create_sync_plan(
    diffs: Vec<DiffResult>,
    keyspace: &str,
    table: &str,
) -> Vec<SyncTaskDescriptor> {
    let mut tasks = Vec::new();

    for diff in diffs {
        if diff.mismatched_ranges.is_empty() {
            continue;
        }

        // For symmetric repair, each side streams to the other.
        tasks.push(SyncTaskDescriptor {
            source: diff.endpoint_a,
            target: diff.endpoint_b,
            ranges: diff.mismatched_ranges.clone(),
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
        tasks.push(SyncTaskDescriptor {
            source: diff.endpoint_b,
            target: diff.endpoint_a,
            ranges: diff.mismatched_ranges,
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
    }

    tasks
}

/// Run a full repair job: diff all trees and produce sync descriptors.
pub fn run_repair_job(
    trees: HashMap<Endpoint, MerkleTree>,
    keyspace: &str,
    table: &str,
) -> RepairJobResult {
    let trees_received = trees.len();
    let diffs = compute_diffs(&trees);
    let sync_tasks = create_sync_plan(diffs.clone(), keyspace, table);

    RepairJobResult {
        trees_received,
        diffs,
        sync_tasks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merkle::hash_partition;

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    fn ep(port: u16) -> Endpoint {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn build_tree(partitions: &[(Token, [u8; 16])]) -> MerkleTree {
        MerkleTree::build(tok(0), tok(1000), 2, partitions)
    }

    #[test]
    fn identical_trees_no_sync() {
        let parts = vec![
            (tok(100), hash_partition(b"k1", b"d1")),
            (tok(500), hash_partition(b"k2", b"d2")),
        ];
        let mut trees = HashMap::new();
        trees.insert(ep(7001), build_tree(&parts));
        trees.insert(ep(7002), build_tree(&parts));

        let result = run_repair_job(trees, "ks", "t1");
        assert_eq!(result.trees_received, 2);
        assert!(result.diffs.is_empty());
        assert!(result.sync_tasks.is_empty());
    }

    #[test]
    fn two_different_trees_produce_sync() {
        let p1 = vec![(tok(100), hash_partition(b"k1", b"data_a"))];
        let p2 = vec![(tok(100), hash_partition(b"k1", b"data_b"))];

        let mut trees = HashMap::new();
        trees.insert(ep(7001), build_tree(&p1));
        trees.insert(ep(7002), build_tree(&p2));

        let result = run_repair_job(trees, "ks", "t1");
        assert_eq!(result.diffs.len(), 1);
        // Bidirectional: 2 sync tasks for 1 diff
        assert_eq!(result.sync_tasks.len(), 2);
        assert_eq!(result.sync_tasks[0].keyspace, "ks");
        assert_eq!(result.sync_tasks[0].table, "t1");
    }

    #[test]
    fn three_replicas_asymmetric_diffs() {
        let common = (tok(100), hash_partition(b"k1", b"same"));
        let diff_a = (tok(500), hash_partition(b"k2", b"version_a"));
        let diff_b = (tok(500), hash_partition(b"k2", b"version_b"));

        let mut trees = HashMap::new();
        trees.insert(ep(7001), build_tree(&[common, diff_a]));
        trees.insert(ep(7002), build_tree(&[common, diff_b]));
        trees.insert(ep(7003), build_tree(&[common, diff_a]));

        let result = run_repair_job(trees, "ks", "t1");
        // 7001 vs 7002 = diff, 7001 vs 7003 = same, 7002 vs 7003 = diff
        assert_eq!(result.diffs.len(), 2);
        // 2 diffs * 2 bidirectional = 4 sync tasks
        assert_eq!(result.sync_tasks.len(), 4);
    }

    #[test]
    fn empty_trees_no_diffs() {
        let mut trees = HashMap::new();
        trees.insert(ep(7001), build_tree(&[]));
        trees.insert(ep(7002), build_tree(&[]));

        let result = run_repair_job(trees, "ks", "t1");
        assert!(result.diffs.is_empty());
        assert!(result.sync_tasks.is_empty());
    }

    #[test]
    fn single_replica_no_diffs() {
        let parts = vec![(tok(100), hash_partition(b"k", b"d"))];
        let mut trees = HashMap::new();
        trees.insert(ep(7001), build_tree(&parts));

        let result = run_repair_job(trees, "ks", "t1");
        assert_eq!(result.trees_received, 1);
        assert!(result.diffs.is_empty());
    }

    #[test]
    fn diff_result_serde() {
        let dr = DiffResult {
            endpoint_a: ep(7001),
            endpoint_b: ep(7002),
            mismatched_ranges: vec![(tok(0), tok(250))],
        };
        let json = serde_json::to_string(&dr).unwrap();
        let deser: DiffResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.mismatched_ranges.len(), 1);
    }
}
