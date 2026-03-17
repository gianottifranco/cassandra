// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

//! Log-scale bucket histogram matching Java's EstimatedHistogram.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.EstimatedHistogram`

use std::sync::atomic::{AtomicI64, Ordering};

/// A histogram with log-scale buckets for efficiently tracking value distributions.
///
/// Default 164 buckets cover values from 1 to ~2^63. Uses atomic counters
/// for lock-free concurrent updates.
pub struct EstimatedHistogram {
    bucket_offsets: Vec<i64>,
    buckets: Vec<AtomicI64>,
}

impl EstimatedHistogram {
    /// Create a histogram with `num_buckets` log-scale buckets.
    ///
    /// Bucket offsets: first is 1, each subsequent is `(prev * 1.2) + 1` rounded,
    /// with the last being `i64::MAX`.
    pub fn new(num_buckets: usize) -> Self {
        assert!(num_buckets >= 1, "need at least 1 bucket");
        let mut offsets = Vec::with_capacity(num_buckets);
        if num_buckets == 1 {
            offsets.push(i64::MAX);
        } else {
            offsets.push(1);
            for i in 1..num_buckets {
                if i == num_buckets - 1 {
                    offsets.push(i64::MAX);
                } else {
                    let prev = *offsets.last().unwrap() as f64;
                    offsets.push((prev * 1.2 + 1.0).round() as i64);
                }
            }
        }
        let buckets = (0..num_buckets).map(|_| AtomicI64::new(0)).collect();
        Self { bucket_offsets: offsets, buckets }
    }

    /// Create with the default 164 buckets.
    pub fn with_default_buckets() -> Self {
        Self::new(164)
    }

    /// Add a value to the histogram by finding its bucket via binary search.
    pub fn add(&self, value: i64) {
        let idx = match self.bucket_offsets.binary_search(&value) {
            Ok(i) => i,
            Err(i) => i,
        };
        let idx = idx.min(self.buckets.len() - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Total count of all recorded values.
    pub fn count(&self) -> i64 {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    /// Minimum recorded value (first non-zero bucket offset), or 0 if empty.
    pub fn min(&self) -> i64 {
        for (i, b) in self.buckets.iter().enumerate() {
            if b.load(Ordering::Relaxed) > 0 {
                return self.bucket_offsets[i];
            }
        }
        0
    }

    /// Maximum recorded value (last non-zero bucket offset), or 0 if empty.
    pub fn max(&self) -> i64 {
        for i in (0..self.buckets.len()).rev() {
            if self.buckets[i].load(Ordering::Relaxed) > 0 {
                return self.bucket_offsets[i];
            }
        }
        0
    }

    /// Weighted average using bucket midpoints.
    pub fn mean(&self) -> f64 {
        let mut sum = 0.0_f64;
        let mut total = 0_i64;
        for (i, b) in self.buckets.iter().enumerate() {
            let count = b.load(Ordering::Relaxed);
            if count > 0 {
                let midpoint = if i == 0 {
                    self.bucket_offsets[0] as f64 / 2.0
                } else {
                    (self.bucket_offsets[i - 1] as f64 + self.bucket_offsets[i] as f64) / 2.0
                };
                sum += midpoint * count as f64;
                total += count;
            }
        }
        if total == 0 { 0.0 } else { sum / total as f64 }
    }

    /// Find the bucket offset at percentile `p` (0.0..=1.0).
    pub fn percentile(&self, p: f64) -> i64 {
        assert!((0.0..=1.0).contains(&p), "percentile must be in 0.0..=1.0");
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p).ceil() as i64;
        let mut acc = 0_i64;
        for (i, b) in self.buckets.iter().enumerate() {
            acc += b.load(Ordering::Relaxed);
            if acc >= target {
                return self.bucket_offsets[i];
            }
        }
        *self.bucket_offsets.last().unwrap()
    }

    /// Merge another histogram's counts into this one. Panics if sizes differ.
    pub fn merge(&self, other: &EstimatedHistogram) {
        assert_eq!(self.buckets.len(), other.buckets.len(), "histogram size mismatch");
        for (a, b) in self.buckets.iter().zip(other.buckets.iter()) {
            a.fetch_add(b.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    /// Return the bucket offset array.
    pub fn bucket_offsets(&self) -> &[i64] {
        &self.bucket_offsets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram() {
        let h = EstimatedHistogram::with_default_buckets();
        assert_eq!(h.count(), 0);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
    }

    #[test]
    fn single_add_correct_bucket() {
        let h = EstimatedHistogram::new(10);
        h.add(1);
        assert_eq!(h.count(), 1);
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 1);
    }

    #[test]
    fn percentile_known_values() {
        let h = EstimatedHistogram::new(164);
        for _ in 0..100 {
            h.add(5);
        }
        for _ in 0..100 {
            h.add(50);
        }
        let p50 = h.percentile(0.50);
        let p99 = h.percentile(0.99);
        assert!(p50 > 0);
        assert!(p99 >= p50);
    }

    #[test]
    fn merge_two_histograms() {
        let a = EstimatedHistogram::new(10);
        let b = EstimatedHistogram::new(10);
        a.add(5);
        b.add(10);
        a.merge(&b);
        assert_eq!(a.count(), 2);
    }

    #[test]
    fn default_bucket_count() {
        let h = EstimatedHistogram::with_default_buckets();
        assert_eq!(h.bucket_offsets().len(), 164);
        assert_eq!(h.buckets.len(), 164);
    }
}
