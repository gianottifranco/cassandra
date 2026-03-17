// Licensed under Apache License, Version 2.0.

//! Bloom filter for fast term non-existence checks in SAI segments.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.BloomFilter`
//!
//! Uses double-hashing to generate k hash positions from two base hashes.

use serde::{Deserialize, Serialize};

/// A probabilistic set membership filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new bloom filter sized for `expected_items` with the given
    /// false-positive rate.
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let fp_rate = fp_rate.clamp(1e-10, 1.0);
        let num_bits = optimal_num_bits(expected_items, fp_rate);
        let num_hashes = optimal_num_hashes(num_bits, expected_items);

        let words = (num_bits + 63) / 64;
        Self {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
        }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = combined_hash(h1, h2, i, self.num_bits);
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// Check if a key *may* be in the set. Returns `false` if definitely not present.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = combined_hash(h1, h2, i, self.num_bits);
            if self.bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("bloom filter serialization should not fail")
    }

    /// Deserialize a bloom filter from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(data).map_err(|e| e.to_string())
    }
}

/// Compute optimal number of bits for given items and fp_rate.
fn optimal_num_bits(n: usize, fp: f64) -> usize {
    let m = -(n as f64 * fp.ln()) / (2.0f64.ln().powi(2));
    (m.ceil() as usize).max(64)
}

/// Compute optimal number of hash functions.
fn optimal_num_hashes(m: usize, n: usize) -> u32 {
    let k = (m as f64 / n.max(1) as f64) * 2.0f64.ln();
    (k.ceil() as u32).clamp(1, 30)
}

/// Double-hashing: h(i) = (h1 + i * h2) mod m
fn combined_hash(h1: u64, h2: u64, i: u32, m: usize) -> usize {
    (h1.wrapping_add((i as u64).wrapping_mul(h2)) % (m as u64)) as usize
}

/// Produce two independent hash values from a key using FNV-1a variants.
fn hash_pair(key: &[u8]) -> (u64, u64) {
    // FNV-1a with standard offset
    let mut h1: u64 = 0xcbf29ce484222325;
    for &b in key {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x100000001b3);
    }

    // FNV-1a with different offset for independence
    let mut h2: u64 = 0x6c62272e07bb0142;
    for &b in key {
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0x100000001b3);
    }

    (h1, h2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_check() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"hello");
        bf.insert(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
    }

    #[test]
    fn absent_key_usually_negative() {
        let mut bf = BloomFilter::new(1000, 0.001);
        for i in 0..100u32 {
            bf.insert(&i.to_be_bytes());
        }

        // Check keys that were NOT inserted
        let mut false_positives = 0;
        for i in 1000..2000u32 {
            if bf.may_contain(&i.to_be_bytes()) {
                false_positives += 1;
            }
        }

        // With fp_rate=0.001 and 1000 checks, expect ~1 FP.
        // Allow generous margin for randomness.
        assert!(
            false_positives < 50,
            "too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn serialize_round_trip() {
        let mut bf = BloomFilter::new(50, 0.01);
        bf.insert(b"key1");
        bf.insert(b"key2");

        let bytes = bf.serialize();
        let restored = BloomFilter::deserialize(&bytes).unwrap();

        assert!(restored.may_contain(b"key1"));
        assert!(restored.may_contain(b"key2"));
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.may_contain(b"anything"));
    }

    #[test]
    fn small_filter_parameters() {
        let bf = BloomFilter::new(1, 0.5);
        assert!(bf.num_bits >= 64);
        assert!(bf.num_hashes >= 1);
    }
}
