// Licensed under Apache License, Version 2.0.

//! Bloom filter utilities.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.BloomFilter`

use crate::murmur3::murmur3_128;

/// A Murmur3-backed Bloom filter for byte keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u64>,
    bit_count: usize,
    hash_count: usize,
}

impl BloomFilter {
    /// Create a filter sized for `expected_items` and `false_positive_rate`.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(0.000_001, 0.999_999);
        let bit_count = optimal_bit_count(expected_items, false_positive_rate).max(64);
        let hash_count = optimal_hash_count(expected_items, bit_count).max(1);
        let word_count = bit_count.div_ceil(64);
        Self {
            bits: vec![0; word_count],
            bit_count,
            hash_count,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        let bits = self.bit_indexes(key).collect::<Vec<_>>();
        for bit in bits {
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.bit_indexes(key)
            .all(|bit| (self.bits[bit / 64] & (1u64 << (bit % 64))) != 0)
    }

    pub fn bit_count(&self) -> usize {
        self.bit_count
    }

    pub fn hash_count(&self) -> usize {
        self.hash_count
    }

    fn bit_indexes<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        let (h1, h2) = murmur3_128(key, 0);
        let h1 = h1 as u64;
        let h2 = h2 as u64;
        (0..self.hash_count)
            .map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % self.bit_count)
    }
}

fn optimal_bit_count(expected_items: usize, false_positive_rate: f64) -> usize {
    let n = expected_items as f64;
    let p = false_positive_rate;
    (-(n * p.ln()) / std::f64::consts::LN_2.powi(2)).ceil() as usize
}

fn optimal_hash_count(expected_items: usize, bit_count: usize) -> usize {
    ((bit_count as f64 / expected_items as f64) * std::f64::consts::LN_2).round() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_inserted_keys() {
        let mut filter = BloomFilter::new(100, 0.01);
        filter.add(b"alpha");
        filter.add(b"beta");
        assert!(filter.might_contain(b"alpha"));
        assert!(filter.might_contain(b"beta"));
    }

    #[test]
    fn rejects_most_missing_keys() {
        let mut filter = BloomFilter::new(10, 0.001);
        filter.add(b"present");
        let misses = (0..100)
            .filter(|i| !filter.might_contain(format!("missing-{i}").as_bytes()))
            .count();
        assert!(misses > 90);
    }

    #[test]
    fn sizing_is_stable() {
        let filter = BloomFilter::new(1000, 0.01);
        assert!(filter.bit_count() >= 64);
        assert!(filter.hash_count() > 1);
    }
}
