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

//! Bitset utilities used by Bloom filters and index structures.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.utils.obs.OpenBitSet`
//! - `org.apache.cassandra.utils.obs.OffHeapBitSet`

use std::mem;

/// Growable bitset backed by 64-bit words.
///
/// This mirrors the core operations Cassandra uses from `OpenBitSet`:
/// indexed get/set/clear, population count, and bitwise set operations. The
/// storage is heap-backed; callers that require strict off-heap allocation can
/// swap the storage layer behind the same API later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenBitSet {
    words: Vec<u64>,
    bit_len: usize,
}

/// Rust native-owned implementation of the `OffHeapBitSet` surface.
///
/// Cassandra's Java implementation stores words outside the JVM heap. Rust has
/// no JVM heap distinction, so this type keeps a separate native word buffer
/// and exposes explicit off-heap size accounting for Bloom filters and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffHeapBitSet {
    words: Box<[u64]>,
    bit_len: usize,
}

impl OpenBitSet {
    /// Create a bitset that can represent `bit_len` bits.
    pub fn new(bit_len: usize) -> Self {
        let word_count = bit_len.div_ceil(64);
        Self {
            words: vec![0; word_count],
            bit_len,
        }
    }

    /// Create a bitset from raw words, masking unused trailing bits.
    pub fn from_words(bit_len: usize, mut words: Vec<u64>) -> Self {
        let expected = bit_len.div_ceil(64);
        words.resize(expected, 0);
        words.truncate(expected);
        let mut bitset = Self { words, bit_len };
        bitset.clear_unused_bits();
        bitset
    }

    /// Number of addressable bits.
    pub fn len(&self) -> usize {
        self.bit_len
    }

    /// Whether the bitset has zero addressable bits.
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Return the underlying words in little-endian bit order.
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    /// Set a bit. Panics if `index >= len`.
    pub fn set(&mut self, index: usize) {
        self.assert_in_bounds(index);
        self.words[index / 64] |= 1u64 << (index % 64);
    }

    /// Clear a bit. Panics if `index >= len`.
    pub fn clear(&mut self, index: usize) {
        self.assert_in_bounds(index);
        self.words[index / 64] &= !(1u64 << (index % 64));
    }

    /// Clear all bits.
    pub fn clear_all(&mut self) {
        self.words.fill(0);
    }

    /// Test a bit. Panics if `index >= len`.
    pub fn get(&self, index: usize) -> bool {
        self.assert_in_bounds(index);
        (self.words[index / 64] & (1u64 << (index % 64))) != 0
    }

    /// Count set bits.
    pub fn cardinality(&self) -> u64 {
        self.words.iter().map(|word| word.count_ones() as u64).sum()
    }

    /// In-place union. Both bitsets must have the same logical length.
    pub fn union_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            *left |= *right;
        }
    }

    /// In-place intersection. Both bitsets must have the same logical length.
    pub fn intersect_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            *left &= *right;
        }
    }

    /// In-place difference. Both bitsets must have the same logical length.
    pub fn difference_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            *left &= !*right;
        }
        self.clear_unused_bits();
    }

    /// Return the next set bit at or after `from`.
    pub fn next_set_bit(&self, from: usize) -> Option<usize> {
        if from >= self.bit_len {
            return None;
        }

        let mut word_index = from / 64;
        let mut word = self.words[word_index] & (!0u64 << (from % 64));
        loop {
            if word != 0 {
                let bit = word.trailing_zeros() as usize;
                let index = word_index * 64 + bit;
                return (index < self.bit_len).then_some(index);
            }
            word_index += 1;
            if word_index >= self.words.len() {
                return None;
            }
            word = self.words[word_index];
        }
    }

    fn assert_in_bounds(&self, index: usize) {
        assert!(
            index < self.bit_len,
            "bit index {index} out of bounds for length {}",
            self.bit_len
        );
    }

    fn assert_same_len(&self, other: &Self) {
        assert_eq!(
            self.bit_len, other.bit_len,
            "bitset lengths must match for bitwise operations"
        );
    }

    fn clear_unused_bits(&mut self) {
        let unused = self.words.len() * 64 - self.bit_len;
        if unused == 0 || self.words.is_empty() {
            return;
        }
        let used = 64 - unused;
        let mask = if used == 64 {
            u64::MAX
        } else {
            (1u64 << used) - 1
        };
        if let Some(last) = self.words.last_mut() {
            *last &= mask;
        }
    }
}

impl OffHeapBitSet {
    /// Create an off-heap bitset that can represent `bit_len` bits.
    pub fn new(bit_len: usize) -> Self {
        let word_count = bit_len.div_ceil(64);
        Self {
            words: vec![0; word_count].into_boxed_slice(),
            bit_len,
        }
    }

    /// Create an off-heap bitset from raw words, masking unused trailing bits.
    pub fn from_words(bit_len: usize, mut words: Vec<u64>) -> Self {
        let expected = bit_len.div_ceil(64);
        words.resize(expected, 0);
        words.truncate(expected);
        let mut bitset = Self {
            words: words.into_boxed_slice(),
            bit_len,
        };
        bitset.clear_unused_bits();
        bitset
    }

    /// Number of addressable bits.
    pub fn len(&self) -> usize {
        self.bit_len
    }

    /// Whether the bitset has zero addressable bits.
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Return the underlying words in little-endian bit order.
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    /// Bytes owned by the native word buffer.
    pub fn off_heap_size(&self) -> usize {
        self.words.len() * mem::size_of::<u64>()
    }

    /// Set a bit. Panics if `index >= len`.
    pub fn set(&mut self, index: usize) {
        self.assert_in_bounds(index);
        self.words[index / 64] |= 1u64 << (index % 64);
    }

    /// Clear a bit. Panics if `index >= len`.
    pub fn clear(&mut self, index: usize) {
        self.assert_in_bounds(index);
        self.words[index / 64] &= !(1u64 << (index % 64));
    }

    /// Clear all bits.
    pub fn clear_all(&mut self) {
        self.words.fill(0);
    }

    /// Test a bit. Panics if `index >= len`.
    pub fn get(&self, index: usize) -> bool {
        self.assert_in_bounds(index);
        (self.words[index / 64] & (1u64 << (index % 64))) != 0
    }

    /// Count set bits.
    pub fn cardinality(&self) -> u64 {
        self.words.iter().map(|word| word.count_ones() as u64).sum()
    }

    /// In-place union. Both bitsets must have the same logical length.
    pub fn union_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(other.words.iter()) {
            *left |= *right;
        }
    }

    /// In-place intersection. Both bitsets must have the same logical length.
    pub fn intersect_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(other.words.iter()) {
            *left &= *right;
        }
    }

    /// In-place difference. Both bitsets must have the same logical length.
    pub fn difference_with(&mut self, other: &Self) {
        self.assert_same_len(other);
        for (left, right) in self.words.iter_mut().zip(other.words.iter()) {
            *left &= !*right;
        }
        self.clear_unused_bits();
    }

    /// Return the next set bit at or after `from`.
    pub fn next_set_bit(&self, from: usize) -> Option<usize> {
        if from >= self.bit_len {
            return None;
        }

        let mut word_index = from / 64;
        let mut word = self.words[word_index] & (!0u64 << (from % 64));
        loop {
            if word != 0 {
                let bit = word.trailing_zeros() as usize;
                let index = word_index * 64 + bit;
                return (index < self.bit_len).then_some(index);
            }
            word_index += 1;
            if word_index >= self.words.len() {
                return None;
            }
            word = self.words[word_index];
        }
    }

    fn assert_in_bounds(&self, index: usize) {
        assert!(
            index < self.bit_len,
            "bit index {index} out of bounds for length {}",
            self.bit_len
        );
    }

    fn assert_same_len(&self, other: &Self) {
        assert_eq!(
            self.bit_len, other.bit_len,
            "bitset lengths must match for bitwise operations"
        );
    }

    fn clear_unused_bits(&mut self) {
        let unused = self.words.len() * 64 - self.bit_len;
        if unused == 0 || self.words.is_empty() {
            return;
        }
        let used = 64 - unused;
        let mask = if used == 64 {
            u64::MAX
        } else {
            (1u64 << used) - 1
        };
        if let Some(last) = self.words.last_mut() {
            *last &= mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_clear_bits_across_words() {
        let mut bits = OpenBitSet::new(130);
        bits.set(0);
        bits.set(64);
        bits.set(129);

        assert!(bits.get(0));
        assert!(bits.get(64));
        assert!(bits.get(129));
        assert_eq!(bits.cardinality(), 3);

        bits.clear(64);
        assert!(!bits.get(64));
        assert_eq!(bits.cardinality(), 2);
    }

    #[test]
    fn masks_unused_trailing_bits_from_words() {
        let bits = OpenBitSet::from_words(3, vec![u64::MAX]);
        assert_eq!(bits.cardinality(), 3);
        assert_eq!(bits.words()[0], 0b111);
    }

    #[test]
    fn bitwise_operations_preserve_expected_membership() {
        let mut left = OpenBitSet::new(96);
        left.set(1);
        left.set(63);
        left.set(95);

        let mut right = OpenBitSet::new(96);
        right.set(63);
        right.set(64);

        let mut union = left.clone();
        union.union_with(&right);
        assert_eq!(union.cardinality(), 4);
        assert!(union.get(1));
        assert!(union.get(63));
        assert!(union.get(64));
        assert!(union.get(95));

        let mut intersection = left.clone();
        intersection.intersect_with(&right);
        assert_eq!(intersection.cardinality(), 1);
        assert!(intersection.get(63));

        let mut difference = union;
        difference.difference_with(&right);
        assert_eq!(difference.cardinality(), 2);
        assert!(difference.get(1));
        assert!(difference.get(95));
    }

    #[test]
    fn next_set_bit_skips_empty_ranges() {
        let mut bits = OpenBitSet::new(140);
        bits.set(7);
        bits.set(72);
        bits.set(139);

        assert_eq!(bits.next_set_bit(0), Some(7));
        assert_eq!(bits.next_set_bit(8), Some(72));
        assert_eq!(bits.next_set_bit(73), Some(139));
        assert_eq!(bits.next_set_bit(140), None);
    }

    #[test]
    fn off_heap_bitset_preserves_open_bitset_behavior() {
        let mut bits = OffHeapBitSet::new(8);
        bits.set(4);
        assert!(bits.get(4));
        assert_eq!(bits.off_heap_size(), 8);
    }

    #[test]
    fn off_heap_bitset_masks_words_and_scans() {
        let mut bits = OffHeapBitSet::from_words(65, vec![u64::MAX, u64::MAX]);
        assert_eq!(bits.cardinality(), 65);
        assert_eq!(bits.next_set_bit(64), Some(64));
        assert_eq!(bits.next_set_bit(65), None);

        bits.clear(64);
        assert_eq!(bits.cardinality(), 64);
    }
}
