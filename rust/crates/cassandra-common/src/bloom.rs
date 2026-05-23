// Licensed under Apache License, Version 2.0.

//! Bloom filter utilities.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.BloomFilter`
//! - `org.apache.cassandra.utils.FilterFactory`

use crate::bitset::OffHeapBitSet;

use crate::murmur3::murmur3_128;

const BITSET_EXCESS: usize = 20;
const MIN_BUCKETS: usize = 2;
const MIN_K: usize = 1;
const BLOOM_PROBS: &[&[f64]] = &[
    &[1.0],
    &[1.0, 1.0],
    &[1.0, 0.393, 0.400],
    &[1.0, 0.283, 0.237, 0.253],
    &[1.0, 0.221, 0.155, 0.147, 0.160],
    &[1.0, 0.181, 0.109, 0.092, 0.092, 0.101],
    &[1.0, 0.154, 0.0804, 0.0609, 0.0561, 0.0578, 0.0638],
    &[1.0, 0.133, 0.0618, 0.0423, 0.0359, 0.0347, 0.0364],
    &[1.0, 0.118, 0.0489, 0.0306, 0.024, 0.0217, 0.0216, 0.0229],
    &[
        1.0, 0.105, 0.0397, 0.0228, 0.0166, 0.0141, 0.0133, 0.0135, 0.0145,
    ],
    &[
        1.0, 0.0952, 0.0329, 0.0174, 0.0118, 0.00943, 0.00844, 0.00819, 0.00846,
    ],
    &[
        1.0, 0.0869, 0.0276, 0.0136, 0.00864, 0.0065, 0.00552, 0.00513, 0.00509,
    ],
    &[
        1.0, 0.08, 0.0236, 0.0108, 0.00646, 0.00459, 0.00371, 0.00329, 0.00314,
    ],
    &[
        1.0, 0.074, 0.0203, 0.00875, 0.00492, 0.00332, 0.00255, 0.00217, 0.00199, 0.00194,
    ],
    &[
        1.0, 0.0689, 0.0177, 0.00718, 0.00381, 0.00244, 0.00179, 0.00146, 0.00129, 0.00121, 0.0012,
    ],
    &[
        1.0, 0.0645, 0.0156, 0.00596, 0.003, 0.00183, 0.00128, 0.001, 0.000852, 0.000775, 0.000744,
    ],
    &[
        1.0, 0.0606, 0.0138, 0.005, 0.00239, 0.00139, 0.000935, 0.000702, 0.000574, 0.000505,
        0.00047, 0.000459,
    ],
    &[
        1.0, 0.0571, 0.0123, 0.00423, 0.00193, 0.00107, 0.000692, 0.000499, 0.000394, 0.000335,
        0.000302, 0.000287, 0.000284,
    ],
    &[
        1.0, 0.054, 0.0111, 0.00362, 0.00158, 0.000839, 0.000519, 0.00036, 0.000275, 0.000226,
        0.000198, 0.000183, 0.000176,
    ],
    &[
        1.0, 0.0513, 0.00998, 0.00312, 0.0013, 0.000663, 0.000394, 0.000264, 0.000194, 0.000155,
        0.000132, 0.000118, 0.000111, 0.000109,
    ],
    &[
        1.0, 0.0488, 0.00906, 0.0027, 0.00108, 0.00053, 0.000303, 0.000196, 0.00014, 0.000108,
        0.0000889, 0.0000777, 0.0000712, 0.0000679, 0.0000671,
    ],
];

/// A Murmur3-backed Bloom filter for byte keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: OffHeapBitSet,
    hash_count: usize,
    always_present: bool,
}

impl BloomFilter {
    /// Create a filter sized for `expected_items` and `false_positive_rate`.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        FilterFactory::get_filter(expected_items, false_positive_rate)
    }

    /// Create a filter with explicit bit and hash counts.
    pub fn with_parameters(bit_count: usize, hash_count: usize) -> Self {
        Self {
            bits: OffHeapBitSet::new(bit_count.max(1)),
            hash_count: hash_count.max(1),
            always_present: false,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        if self.always_present {
            return;
        }
        let bits = self.bit_indexes(key).collect::<Vec<_>>();
        for bit in bits {
            self.bits.set(bit);
        }
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        if self.always_present {
            return true;
        }
        self.bit_indexes(key).all(|bit| self.bits.get(bit))
    }

    pub fn bit_count(&self) -> usize {
        self.bits.len()
    }

    pub fn hash_count(&self) -> usize {
        self.hash_count
    }

    pub fn is_informative(&self) -> bool {
        !self.always_present
    }

    pub fn bitset(&self) -> &OffHeapBitSet {
        &self.bits
    }

    fn bit_indexes<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        let (h1, h2) = murmur3_128(key, 0);
        let capacity = self.bit_count() as i64;
        (0..self.hash_count).map(move |i| {
            h2.wrapping_add((i as i64).wrapping_mul(h1))
                .wrapping_rem(capacity)
                .unsigned_abs() as usize
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BloomSpecification {
    pub hash_count: usize,
    pub buckets_per_element: usize,
}

/// Factory for Java-compatible Bloom filter sizing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FilterFactory;

impl FilterFactory {
    /// Create a filter sized for the expected element count and false positive chance.
    pub fn get_filter(expected_items: usize, false_positive_rate: f64) -> BloomFilter {
        if false_positive_rate >= 1.0 {
            return BloomFilter {
                bits: OffHeapBitSet::new(1),
                hash_count: 1,
                always_present: true,
            };
        }

        let max_buckets = Self::max_buckets_per_element(expected_items);
        let spec =
            Self::compute_bloom_spec(max_buckets, false_positive_rate).unwrap_or_else(|| {
                Self::compute_bloom_spec(max_buckets, Self::min_supported_fp_chance()).unwrap()
            });
        Self::create_filter(expected_items, spec)
    }

    /// Create a filter for a target number of buckets per element.
    pub fn get_filter_for_buckets(
        expected_items: usize,
        target_buckets_per_element: usize,
    ) -> BloomFilter {
        let max_buckets = Self::max_buckets_per_element(expected_items).max(1);
        let buckets_per_element = target_buckets_per_element.min(max_buckets).max(1);
        let spec = Self::compute_bloom_spec_for_buckets(buckets_per_element);
        Self::create_filter(expected_items, spec)
    }

    /// Create a filter with explicit bit/hash counts, used when loading persisted metadata.
    pub fn get_filter_with_parameters(bit_count: usize, hash_count: usize) -> BloomFilter {
        BloomFilter::with_parameters(bit_count, hash_count)
    }

    /// Number of bits per element for a target false positive probability.
    pub fn buckets_per_element(false_positive_rate: f64) -> usize {
        Self::compute_bloom_spec(BLOOM_PROBS.len() - 1, false_positive_rate)
            .map(|spec| spec.buckets_per_element)
            .unwrap_or(BLOOM_PROBS.len() - 1)
    }

    /// Hash function count for a bits-per-element value.
    pub fn hash_count_for_buckets_per_element(buckets_per_element: usize) -> usize {
        Self::compute_bloom_spec_for_buckets(buckets_per_element).hash_count
    }

    pub fn max_buckets_per_element(expected_items: usize) -> usize {
        let expected_items = expected_items.max(1);
        let max_for_capacity = (usize::MAX - BITSET_EXCESS) / expected_items;
        max_for_capacity.min(BLOOM_PROBS.len() - 1).max(1)
    }

    pub fn min_supported_fp_chance() -> f64 {
        let row = BLOOM_PROBS
            .last()
            .expect("bloom probability table is non-empty");
        *row.last().expect("bloom probability rows are non-empty")
    }

    pub fn compute_bloom_spec_for_buckets(buckets_per_element: usize) -> BloomSpecification {
        let buckets_per_element = buckets_per_element.clamp(1, BLOOM_PROBS.len() - 1);
        let row = BLOOM_PROBS[buckets_per_element];
        let hash_count = row
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(index, _)| index.max(MIN_K))
            .unwrap_or(MIN_K);
        BloomSpecification {
            hash_count,
            buckets_per_element,
        }
    }

    pub fn compute_bloom_spec(
        max_buckets_per_element: usize,
        false_positive_rate: f64,
    ) -> Option<BloomSpecification> {
        let max_buckets_per_element = max_buckets_per_element.clamp(1, BLOOM_PROBS.len() - 1);
        let max_k = BLOOM_PROBS[max_buckets_per_element].len() - 1;
        if false_positive_rate >= BLOOM_PROBS[MIN_BUCKETS][MIN_K] {
            return Some(BloomSpecification {
                hash_count: 2,
                buckets_per_element: MIN_BUCKETS,
            });
        }
        if false_positive_rate < BLOOM_PROBS[max_buckets_per_element][max_k] {
            return None;
        }

        let mut buckets_per_element = MIN_BUCKETS;
        let mut hash_count = Self::compute_bloom_spec_for_buckets(buckets_per_element).hash_count;
        while BLOOM_PROBS[buckets_per_element][hash_count] > false_positive_rate {
            buckets_per_element += 1;
            hash_count = Self::compute_bloom_spec_for_buckets(buckets_per_element).hash_count;
        }
        while hash_count > 1
            && BLOOM_PROBS[buckets_per_element][hash_count - 1] <= false_positive_rate
        {
            hash_count -= 1;
        }

        Some(BloomSpecification {
            hash_count,
            buckets_per_element,
        })
    }

    fn create_filter(expected_items: usize, spec: BloomSpecification) -> BloomFilter {
        let expected_items = expected_items.max(1);
        BloomFilter {
            bits: OffHeapBitSet::new(
                expected_items
                    .saturating_mul(spec.buckets_per_element)
                    .saturating_add(BITSET_EXCESS),
            ),
            hash_count: spec.hash_count.max(1),
            always_present: false,
        }
    }
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
        assert_eq!(filter.bit_count(), 10_020);
        assert!(filter.hash_count() > 1);
    }

    #[test]
    fn factory_matches_direct_filter_sizing() {
        let direct = BloomFilter::new(1000, 0.01);
        let factory = FilterFactory::get_filter(1000, 0.01);
        assert_eq!(factory.bit_count(), direct.bit_count());
        assert_eq!(factory.hash_count(), direct.hash_count());
    }

    #[test]
    fn factory_explicit_parameters_roundtrip_membership() {
        let mut filter = FilterFactory::get_filter_with_parameters(128, 4);
        assert_eq!(filter.bit_count(), 128);
        assert_eq!(filter.hash_count(), 4);
        filter.add(b"key");
        assert!(filter.might_contain(b"key"));
        assert!(filter.bitset().cardinality() <= 4);
    }

    #[test]
    fn buckets_per_element_and_hash_count_are_java_style() {
        let buckets = FilterFactory::buckets_per_element(0.01);
        assert_eq!(buckets, 10);
        assert_eq!(
            FilterFactory::hash_count_for_buckets_per_element(buckets),
            7
        );
    }

    #[test]
    fn factory_supports_java_bucket_overload_and_excess_bits() {
        let filter = FilterFactory::get_filter_for_buckets(100, 4);
        assert_eq!(filter.bit_count(), 420);
        assert_eq!(filter.hash_count(), 3);
    }

    #[test]
    fn false_positive_rate_one_is_always_present() {
        let mut filter = FilterFactory::get_filter(10, 1.0);
        assert!(!filter.is_informative());
        assert!(filter.might_contain(b"missing-before-add"));
        filter.add(b"anything");
        assert!(filter.might_contain(b"missing-after-add"));
    }

    #[test]
    fn exposes_java_bloom_calculation_limits() {
        assert_eq!(FilterFactory::max_buckets_per_element(1), 20);
        assert_eq!(FilterFactory::min_supported_fp_chance(), 0.0000671);
        assert_eq!(
            FilterFactory::compute_bloom_spec(20, 0.01).unwrap(),
            BloomSpecification {
                hash_count: 5,
                buckets_per_element: 10,
            }
        );
    }
}
