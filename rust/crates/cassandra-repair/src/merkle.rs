// Licensed under Apache License, Version 2.0.

//! Merkle tree for anti-entropy: detects mismatched data ranges.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.utils.MerkleTree`
//! - `org.apache.cassandra.repair.Validator`
//!
//! ## Design
//!
//! The tree is a binary hash tree over a token range.
//! - Each leaf covers a sub-range and holds MD5(sorted partition hashes)
//! - Internal nodes hold MD5(left_hash || right_hash)
//! - `diff(local, remote)` returns mismatched leaf ranges

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use cassandra_common::Token;

/// Default tree depth. 15 levels → 2^15 = 32768 leaves.
pub const DEFAULT_DEPTH: u32 = 15;

/// Hash type: 16-byte MD5.
pub type TreeHash = [u8; 16];

/// A single leaf in the Merkle tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleLeaf {
    /// Start token (inclusive).
    pub range_start: Token,
    /// End token (exclusive).
    pub range_end: Token,
    /// Hash of all partition data in this range.
    pub hash: TreeHash,
    /// Number of partitions hashed.
    pub partition_count: u64,
}

/// A Merkle tree over a token range.
///
/// Leaves cover equal subdivisions of the full token range.
/// Internal nodes are computed bottom-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTree {
    /// Depth of the tree (number of levels below root).
    pub depth: u32,
    /// Start of the full range (inclusive).
    pub range_start: Token,
    /// End of the full range (exclusive).
    pub range_end: Token,
    /// Leaf hashes (2^depth entries).
    pub leaves: Vec<MerkleLeaf>,
    /// Internal node hashes, stored level-by-level bottom-up.
    /// Level 0 = leaves, Level depth = root.
    /// We only store internal nodes (levels 1..=depth).
    pub internal: Vec<Vec<TreeHash>>,
    /// Root hash.
    pub root_hash: TreeHash,
}

impl MerkleTree {
    /// Build a new Merkle tree from partition hashes.
    ///
    /// `partition_hashes` must be sorted by token.
    pub fn build(
        range_start: Token,
        range_end: Token,
        depth: u32,
        partition_hashes: &[(Token, TreeHash)],
    ) -> Self {
        let leaf_count = 1u64 << depth;
        let start_raw = range_start.value();
        let end_raw = range_end.value();

        // Handle wrapping ranges
        let total_width = if end_raw > start_raw {
            (end_raw - start_raw) as u128
        } else {
            // Wrapping range: from start to i64::MAX, then i64::MIN to end
            ((i64::MAX as i128 - start_raw as i128) + (end_raw as i128 - i64::MIN as i128) + 1)
                as u128
        };

        let leaf_width = total_width / leaf_count as u128;

        // Build leaves
        let mut leaves = Vec::with_capacity(leaf_count as usize);
        let mut ph_idx = 0;

        for i in 0..leaf_count {
            let leaf_start_raw = start_raw.wrapping_add((i as i128 * leaf_width as i128) as i64);
            let leaf_end_raw = if i == leaf_count - 1 {
                end_raw
            } else {
                start_raw.wrapping_add(((i + 1) as i128 * leaf_width as i128) as i64)
            };

            let leaf_start = Token::from_raw(leaf_start_raw);
            let leaf_end = Token::from_raw(leaf_end_raw);

            // Hash all partitions in this leaf's range
            let mut hasher = Md5::new();
            let mut count = 0u64;

            while ph_idx < partition_hashes.len() {
                let (tok, hash) = &partition_hashes[ph_idx];
                let tok_raw = tok.value();
                // Check if token is in [leaf_start, leaf_end)
                let in_range = if leaf_start_raw <= leaf_end_raw {
                    tok_raw >= leaf_start_raw && tok_raw < leaf_end_raw
                } else {
                    tok_raw >= leaf_start_raw || tok_raw < leaf_end_raw
                };

                if in_range {
                    hasher.update(hash);
                    count += 1;
                    ph_idx += 1;
                } else {
                    break;
                }
            }

            let mut hash = [0u8; 16];
            hash.copy_from_slice(&hasher.finalize());

            leaves.push(MerkleLeaf {
                range_start: leaf_start,
                range_end: leaf_end,
                hash,
                partition_count: count,
            });
        }

        // Build internal levels bottom-up
        let mut internal = Vec::new();
        let mut current_level: Vec<TreeHash> = leaves.iter().map(|l| l.hash).collect();

        while current_level.len() > 1 {
            let mut parent_level = Vec::with_capacity(current_level.len() / 2);
            for pair in current_level.chunks(2) {
                let mut hasher = Md5::new();
                hasher.update(pair[0]);
                if pair.len() > 1 {
                    hasher.update(pair[1]);
                }
                let mut hash = [0u8; 16];
                hash.copy_from_slice(&hasher.finalize());
                parent_level.push(hash);
            }
            internal.push(current_level);
            current_level = parent_level;
        }

        let root_hash = if current_level.len() == 1 {
            current_level[0]
        } else {
            [0u8; 16]
        };

        Self {
            depth,
            range_start,
            range_end,
            leaves,
            internal,
            root_hash,
        }
    }

    /// Diff two Merkle trees, returning leaf ranges that differ.
    ///
    /// Both trees must cover the same range and have the same depth.
    pub fn diff(&self, other: &MerkleTree) -> Vec<(Token, Token)> {
        assert_eq!(self.depth, other.depth);
        assert_eq!(self.leaves.len(), other.leaves.len());

        let mut diffs = Vec::new();

        if self.root_hash == other.root_hash {
            return diffs; // Trees are identical
        }

        // Walk leaves and find mismatches
        for (local, remote) in self.leaves.iter().zip(other.leaves.iter()) {
            if local.hash != remote.hash {
                diffs.push((local.range_start, local.range_end));
            }
        }

        diffs
    }

    /// Number of leaves in the tree.
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Total partitions across all leaves.
    pub fn total_partitions(&self) -> u64 {
        self.leaves.iter().map(|l| l.partition_count).sum()
    }
}

/// Compute a hash of a single partition's data for Merkle input.
///
/// In practice, this would hash the actual partition rows/cells.
/// This helper hashes the partition key + a data hash.
pub fn hash_partition(partition_key: &[u8], data_hash: &[u8]) -> TreeHash {
    let mut hasher = Md5::new();
    hasher.update(partition_key);
    hasher.update(data_hash);
    let mut hash = [0u8; 16];
    hash.copy_from_slice(&hasher.finalize());
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    #[test]
    fn empty_tree() {
        let tree = MerkleTree::build(tok(0), tok(1000), 2, &[]);
        assert_eq!(tree.leaf_count(), 4); // 2^2
        assert_eq!(tree.total_partitions(), 0);
    }

    #[test]
    fn identical_trees_no_diff() {
        let partitions = vec![
            (tok(100), hash_partition(b"pk1", b"data1")),
            (tok(200), hash_partition(b"pk2", b"data2")),
            (tok(300), hash_partition(b"pk3", b"data3")),
        ];

        let t1 = MerkleTree::build(tok(0), tok(1000), 2, &partitions);
        let t2 = MerkleTree::build(tok(0), tok(1000), 2, &partitions);

        let diffs = t1.diff(&t2);
        assert!(diffs.is_empty(), "Identical trees should have no diffs");
    }

    #[test]
    fn different_trees_find_diff() {
        let p1 = vec![
            (tok(100), hash_partition(b"pk1", b"data1")),
            (tok(400), hash_partition(b"pk2", b"data2")),
        ];
        let p2 = vec![
            (tok(100), hash_partition(b"pk1", b"data1")),
            (tok(400), hash_partition(b"pk2", b"DIFFERENT")),
        ];

        let t1 = MerkleTree::build(tok(0), tok(1000), 2, &p1);
        let t2 = MerkleTree::build(tok(0), tok(1000), 2, &p2);

        let diffs = t1.diff(&t2);
        assert!(!diffs.is_empty(), "Should detect difference");
    }

    #[test]
    fn deterministic_build() {
        let partitions = vec![
            (tok(50), hash_partition(b"a", b"1")),
            (tok(150), hash_partition(b"b", b"2")),
        ];

        let t1 = MerkleTree::build(tok(0), tok(500), 3, &partitions);
        let t2 = MerkleTree::build(tok(0), tok(500), 3, &partitions);

        assert_eq!(t1.root_hash, t2.root_hash);
        assert_eq!(t1.leaf_count(), t2.leaf_count());
    }

    #[test]
    fn hash_partition_deterministic() {
        let h1 = hash_partition(b"key", b"data");
        let h2 = hash_partition(b"key", b"data");
        assert_eq!(h1, h2);

        let h3 = hash_partition(b"key", b"other");
        assert_ne!(h1, h3);
    }

    #[test]
    fn total_partitions_counted() {
        let partitions = vec![
            (tok(100), hash_partition(b"a", b"1")),
            (tok(200), hash_partition(b"b", b"2")),
            (tok(800), hash_partition(b"c", b"3")),
        ];

        let tree = MerkleTree::build(tok(0), tok(1000), 2, &partitions);
        assert_eq!(tree.total_partitions(), 3);
    }
}
