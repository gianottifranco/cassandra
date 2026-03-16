// Licensed under Apache License, Version 2.0.

//! Bloom filter for SSTable partition key lookups.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.BloomFilter`
//! - `org.apache.cassandra.utils.MurmurHash`
//!
//! Uses a simple double-hashing scheme with murmur3-derived hash values,
//! compatible with Cassandra's Bloom filter format.

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

/// A simple Bloom filter using murmur3-based double hashing.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hash_functions: u32,
    bit_count: u64,
}

impl BloomFilter {
    /// Create a new Bloom filter for `num_elements` elements with the given
    /// false positive rate.
    pub fn new(num_elements: usize, fp_rate: f64) -> Self {
        let bit_count = optimal_num_bits(num_elements, fp_rate);
        let num_hash_functions = optimal_num_hashes(bit_count, num_elements);
        let word_count = ((bit_count + 63) / 64) as usize;

        Self {
            bits: vec![0u64; word_count],
            num_hash_functions,
            bit_count,
        }
    }

    /// Add a key to the filter.
    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = murmur3_hash128(key);
        for i in 0..self.num_hash_functions {
            let bit = self.get_bit_index(h1, h2, i);
            let word = (bit / 64) as usize;
            let bit_in_word = bit % 64;
            if word < self.bits.len() {
                self.bits[word] |= 1u64 << bit_in_word;
            }
        }
    }

    /// Test if a key might be in the set. False positives possible.
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = murmur3_hash128(key);
        for i in 0..self.num_hash_functions {
            let bit = self.get_bit_index(h1, h2, i);
            let word = (bit / 64) as usize;
            let bit_in_word = bit % 64;
            if word >= self.bits.len() || (self.bits[word] & (1u64 << bit_in_word)) == 0 {
                return false;
            }
        }
        true
    }

    fn get_bit_index(&self, h1: i64, h2: i64, i: u32) -> u64 {
        let combined = h1.wrapping_add((i as i64).wrapping_mul(h2));
        // Make positive
        let positive = (combined as u64) & 0x7FFF_FFFF_FFFF_FFFF;
        positive % self.bit_count
    }

    /// Serialize the bloom filter, for the Filter.db component.
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<BigEndian>(self.num_hash_functions)?;
        writer.write_u64::<BigEndian>(self.bit_count)?;
        writer.write_u32::<BigEndian>(self.bits.len() as u32)?;
        for word in &self.bits {
            writer.write_u64::<BigEndian>(*word)?;
        }
        Ok(())
    }

    /// Deserialize a bloom filter from Filter.db.
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let num_hash_functions = reader.read_u32::<BigEndian>()?;
        let bit_count = reader.read_u64::<BigEndian>()?;
        let word_count = reader.read_u32::<BigEndian>()? as usize;
        let mut bits = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            bits.push(reader.read_u64::<BigEndian>()?);
        }
        Ok(Self {
            bits,
            num_hash_functions,
            bit_count,
        })
    }
}

// ─── Murmur3 hash ──────────────────────────────────────────────────────────

/// Simplified murmur3-128 hash returning two i64 values for double hashing.
/// This provides the same distribution properties needed for bloom filters.
fn murmur3_hash128(key: &[u8]) -> (i64, i64) {
    let seed: u64 = 0;
    let mut h1: u64 = seed;
    let mut h2: u64 = seed;
    let c1: u64 = 0x87c3_7b91_1142_53d5;
    let c2: u64 = 0x4cf5_ad43_2745_937f;

    // Process 16-byte chunks
    let chunks = key.len() / 16;
    for i in 0..chunks {
        let offset = i * 16;
        let mut k1 = u64::from_le_bytes(key[offset..offset + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(key[offset + 8..offset + 16].try_into().unwrap());

        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(c2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(c2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(c1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    // Process remaining bytes
    let tail = &key[chunks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    if tail.len() >= 15 {
        k2 ^= (tail[14] as u64) << 48;
    }
    if tail.len() >= 14 {
        k2 ^= (tail[13] as u64) << 40;
    }
    if tail.len() >= 13 {
        k2 ^= (tail[12] as u64) << 32;
    }
    if tail.len() >= 12 {
        k2 ^= (tail[11] as u64) << 24;
    }
    if tail.len() >= 11 {
        k2 ^= (tail[10] as u64) << 16;
    }
    if tail.len() >= 10 {
        k2 ^= (tail[9] as u64) << 8;
    }
    if tail.len() >= 9 {
        k2 ^= tail[8] as u64;
        k2 = k2.wrapping_mul(c2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(c1);
        h2 ^= k2;
    }
    if tail.len() >= 8 {
        k1 ^= (tail[7] as u64) << 56;
    }
    if tail.len() >= 7 {
        k1 ^= (tail[6] as u64) << 48;
    }
    if tail.len() >= 6 {
        k1 ^= (tail[5] as u64) << 40;
    }
    if tail.len() >= 5 {
        k1 ^= (tail[4] as u64) << 32;
    }
    if tail.len() >= 4 {
        k1 ^= (tail[3] as u64) << 24;
    }
    if tail.len() >= 3 {
        k1 ^= (tail[2] as u64) << 16;
    }
    if tail.len() >= 2 {
        k1 ^= (tail[1] as u64) << 8;
    }
    if !tail.is_empty() {
        k1 ^= tail[0] as u64;
        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(c2);
        h1 ^= k1;
    }

    // Finalization
    h1 ^= key.len() as u64;
    h2 ^= key.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1 as i64, h2 as i64)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

// ─── Optimal sizing ────────────────────────────────────────────────────────

fn optimal_num_bits(n: usize, fp_rate: f64) -> u64 {
    let n = n.max(1) as f64;
    let bits = -(n * fp_rate.ln()) / (2.0_f64.ln().powi(2));
    (bits.ceil() as u64).max(64) // minimum 64 bits
}

fn optimal_num_hashes(bits: u64, n: usize) -> u32 {
    let n = n.max(1) as f64;
    let k = (bits as f64 / n) * 2.0_f64.ln();
    (k.ceil() as u32).max(1).min(30)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_bloom_filter() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(b"hello");
        bf.add(b"world");

        assert!(bf.might_contain(b"hello"));
        assert!(bf.might_contain(b"world"));
        // Not added — should (probably) not be found
        // Due to false positives this isn't guaranteed, but for 2 elements
        // in a filter sized for 100, it's virtually impossible.
        assert!(!bf.might_contain(b"missing"));
    }

    #[test]
    fn bloom_filter_serialize_roundtrip() {
        let mut bf = BloomFilter::new(50, 0.01);
        for i in 0..50 {
            bf.add(format!("key_{i}").as_bytes());
        }

        let mut buf = Vec::new();
        bf.serialize(&mut buf).unwrap();

        let bf2 = BloomFilter::deserialize(&mut buf.as_slice()).unwrap();

        for i in 0..50 {
            assert!(bf2.might_contain(format!("key_{i}").as_bytes()));
        }
    }

    #[test]
    fn false_positive_rate() {
        let n = 1000;
        let mut bf = BloomFilter::new(n, 0.01);
        for i in 0..n {
            bf.add(format!("key_{i}").as_bytes());
        }

        let mut false_positives = 0;
        let tests = 10_000;
        for i in n..n + tests {
            if bf.might_contain(format!("missing_{i}").as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / tests as f64;
        // Should be approximately <= 0.01, allow some margin
        assert!(
            fp_rate < 0.05,
            "FP rate too high: {fp_rate} ({false_positives}/{tests})"
        );
    }

    #[test]
    fn empty_bloom_filter() {
        let bf = BloomFilter::new(10, 0.01);
        assert!(!bf.might_contain(b"anything"));
    }

    #[test]
    fn murmur3_consistency() {
        // Same input always produces same hash
        let (h1a, h2a) = murmur3_hash128(b"test_key");
        let (h1b, h2b) = murmur3_hash128(b"test_key");
        assert_eq!(h1a, h1b);
        assert_eq!(h2a, h2b);

        // Different inputs produce different hashes
        let (h1c, _) = murmur3_hash128(b"other_key");
        assert_ne!(h1a, h1c);
    }
}
