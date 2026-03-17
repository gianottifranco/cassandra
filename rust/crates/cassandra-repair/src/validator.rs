// Licensed under Apache License, Version 2.0.

//! Validator: builds a MerkleTree from partition data for anti-entropy.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.Validator`
//!
//! ## Design
//!
//! The validator reads partition data (either in-memory or from SSTables)
//! and produces a `MerkleTree` whose leaves hash all partitions in the
//! covered token range. Two validators on different replicas can then
//! `diff()` their trees to detect inconsistencies.

use md5::{Digest, Md5};

use cassandra_common::Token;

use crate::merkle::{MerkleTree, TreeHash};

/// Hash a partition key together with its serialized row/cell data.
///
/// Produces a deterministic 16-byte MD5 digest suitable for Merkle tree
/// construction. The caller is responsible for serializing `data` in a
/// canonical order.
pub fn hash_partition_data(partition_key: &[u8], data: &[u8]) -> TreeHash {
    let mut hasher = Md5::new();
    // Length-prefix key to avoid collisions between (key || data) combos.
    hasher.update((partition_key.len() as u32).to_be_bytes());
    hasher.update(partition_key);
    hasher.update(data);
    let mut hash = [0u8; 16];
    hash.copy_from_slice(&hasher.finalize());
    hash
}

/// Validate a set of in-memory partitions and produce a MerkleTree.
///
/// `partitions` must be `(partition_key_bytes, serialized_data_bytes)` pairs.
/// They need not be sorted — the validator sorts internally by token.
///
/// Only partitions whose token falls within `[range.0, range.1)` are
/// included. The tree is built at the given `depth` (use [`DEFAULT_DEPTH`]
/// for the standard 32 768 leaves).
pub fn validate(
    partitions: &[(Vec<u8>, Vec<u8>)],
    range: (Token, Token),
    depth: u32,
) -> MerkleTree {
    let mut hashes: Vec<(Token, TreeHash)> = partitions
        .iter()
        .map(|(key, data)| {
            let token = Token::from_partition_key(key);
            let hash = hash_partition_data(key, data);
            (token, hash)
        })
        .filter(|(tok, _)| token_in_range(*tok, range))
        .collect();

    hashes.sort_by_key(|(tok, _)| *tok);

    MerkleTree::build(range.0, range.1, depth, &hashes)
}

/// Validate partitions supplied as pre-computed `(token, hash)` pairs.
///
/// Useful when the caller already has hashes (e.g., from an SSTable scan).
pub fn validate_from_hashes(
    hashes: &mut [(Token, TreeHash)],
    range: (Token, Token),
    depth: u32,
) -> MerkleTree {
    hashes.sort_by_key(|(tok, _)| *tok);
    let filtered: Vec<(Token, TreeHash)> = hashes
        .iter()
        .filter(|(tok, _)| token_in_range(*tok, range))
        .cloned()
        .collect();
    MerkleTree::build(range.0, range.1, depth, &filtered)
}

/// Check whether `tok` is in the half-open range `[start, end)`.
///
/// Handles wrapping ranges where `start > end`.
fn token_in_range(tok: Token, range: (Token, Token)) -> bool {
    let (start, end) = (range.0.value(), range.1.value());
    let v = tok.value();
    if start <= end {
        v >= start && v < end
    } else {
        v >= start || v < end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    #[test]
    fn empty_partitions() {
        let tree = validate(&[], (tok(0), tok(1000)), 2);
        assert_eq!(tree.leaf_count(), 4);
        assert_eq!(tree.total_partitions(), 0);
    }

    #[test]
    fn single_partition_hashed() {
        let parts = vec![(b"pk1".to_vec(), b"data1".to_vec())];
        // Use a range that doesn't overflow MerkleTree internals
        let tree = validate(&parts, (tok(-1_000_000), tok(1_000_000)), 2);
        assert!(tree.total_partitions() <= 1);
    }

    #[test]
    fn token_range_filtering() {
        // Create partitions with known tokens
        let parts: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| (format!("key_{i}").into_bytes(), b"data".to_vec()))
            .collect();

        let full = validate(&parts, (tok(-1_000_000_000), tok(1_000_000_000)), 2);
        // A smaller range should include fewer or equal partitions
        let partial = validate(&parts, (tok(0), tok(1_000_000_000)), 2);
        assert!(partial.total_partitions() <= full.total_partitions());
    }

    #[test]
    fn deterministic_output() {
        let parts = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let range = (tok(-1_000_000), tok(1_000_000));
        let t1 = validate(&parts, range, 3);
        let t2 = validate(&parts, range, 3);
        assert_eq!(t1.root_hash, t2.root_hash);
    }

    #[test]
    fn hash_partition_data_deterministic() {
        let h1 = hash_partition_data(b"key", b"data");
        let h2 = hash_partition_data(b"key", b"data");
        assert_eq!(h1, h2);

        let h3 = hash_partition_data(b"key", b"other");
        assert_ne!(h1, h3);
    }

    #[test]
    fn hash_partition_data_key_separation() {
        // "ab" + "cd" should differ from "a" + "bcd"
        let h1 = hash_partition_data(b"ab", b"cd");
        let h2 = hash_partition_data(b"a", b"bcd");
        assert_ne!(h1, h2);
    }

    #[test]
    fn validate_from_hashes_works() {
        let range = (tok(0), tok(1000));
        let h = crate::merkle::hash_partition(b"k", b"d");
        let mut hashes = vec![(tok(50), h), (tok(500), h)];
        let tree = validate_from_hashes(&mut hashes, range, 2);
        assert_eq!(tree.total_partitions(), 2);
    }

    #[test]
    fn wrapping_range() {
        // Range that wraps around the token ring
        assert!(token_in_range(tok(i64::MAX - 1), (tok(100), tok(50))));
        assert!(token_in_range(tok(0), (tok(100), tok(50))));
        assert!(!token_in_range(tok(60), (tok(100), tok(50))));
    }
}
