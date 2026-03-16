// Licensed under Apache License, Version 2.0.

//! Unified Compaction Strategy (UCS) — experimental.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.UnifiedCompactionStrategy`
//!
//! ## Architecture
//!
//! UCS adaptively chooses between tiered and leveled behavior based
//! on a scaling parameter (W). The strategy uses "shards" (buckets)
//! organized by density (data size / key coverage).
//!
//! - W > 0: tiered behavior (write-optimized)
//! - W < 0: leveled behavior (read-optimized)
//! - W = 0: balanced
//!
//! This is an experimental feature gated behind the `ucs` feature flag.
//! Matches the Cassandra 5.0+ trunk implementation.

use super::{CompactionStrategy, SSTableMetadata};
use crate::sstable::format::SSTableId;

/// Scaling parameter controlling tiered vs leveled behavior.
#[derive(Debug, Clone, Copy)]
pub struct ScalingParameter {
    /// W values per level. Positive = tiered, negative = leveled, 0 = balanced.
    /// If fewer entries than levels, the last value is repeated.
    pub values: [i32; 8],
    pub count: usize,
}

impl Default for ScalingParameter {
    fn default() -> Self {
        // Default: balanced at all levels
        Self {
            values: [0; 8],
            count: 1,
        }
    }
}

impl ScalingParameter {
    pub fn get(&self, level: usize) -> i32 {
        if level < self.count {
            self.values[level]
        } else if self.count > 0 {
            self.values[self.count - 1]
        } else {
            0
        }
    }
}

/// Unified Compaction Strategy.
#[derive(Debug, Clone)]
pub struct UnifiedCompactionStrategy {
    /// Scaling parameters per level.
    pub scaling: ScalingParameter,
    /// Target SSTable size (bytes).
    pub target_sstable_size: u64,
    /// Base shard count (number of density buckets).
    pub base_shard_count: u32,
    /// Minimum SSTables to compact.
    pub min_threshold: usize,
    /// Maximum SSTables to compact.
    pub max_threshold: usize,
    /// Survival factor: data expected to survive compaction (0.0 - 1.0).
    pub survival_factor: f64,
}

impl Default for UnifiedCompactionStrategy {
    fn default() -> Self {
        Self {
            scaling: ScalingParameter::default(),
            target_sstable_size: 256 * 1024 * 1024, // 256 MiB
            base_shard_count: 4,
            min_threshold: 4,
            max_threshold: 32,
            survival_factor: 1.0,
        }
    }
}

/// A "bucket" of SSTables with similar density.
#[derive(Debug)]
struct DensityBucket {
    sstables: Vec<SSTableId>,
    total_size: u64,
    #[allow(dead_code)]
    min_density: f64,
    max_density: f64,
}

impl UnifiedCompactionStrategy {
    /// Calculate the "density" of an SSTable.
    /// Density = data_size / partition_count. Higher density means more
    /// data per partition (possibly more versions/tombstones).
    fn density(sst: &SSTableMetadata) -> f64 {
        if sst.partition_count == 0 {
            return 0.0;
        }
        sst.data_size as f64 / sst.partition_count as f64
    }

    /// The fan factor for a given level based on scaling parameter W.
    /// - W > 0 (tiered): fan = 2 + W (more SSTables per level)
    /// - W < 0 (leveled): fan = 2 (fewer, non-overlapping)
    /// - W = 0: fan = 2 (balanced)
    fn fan_factor(&self, level: usize) -> u32 {
        let w = self.scaling.get(level);
        if w > 0 { (2 + w) as u32 } else { 2 }
    }

    /// Group SSTables into density buckets.
    fn bucket_by_density(&self, sstables: &[SSTableMetadata]) -> Vec<DensityBucket> {
        if sstables.is_empty() {
            return vec![];
        }

        let mut sorted: Vec<_> = sstables.to_vec();
        sorted.sort_by(|a, b| {
            Self::density(a)
                .partial_cmp(&Self::density(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Create buckets by exponential density ranges
        let mut buckets: Vec<DensityBucket> = Vec::new();
        let min_density = Self::density(&sorted[0]).max(1.0);
        let mut boundary = min_density;
        let fan = self.fan_factor(0) as f64;

        let mut current_bucket = DensityBucket {
            sstables: Vec::new(),
            total_size: 0,
            min_density: boundary,
            max_density: boundary * fan,
        };

        for sst in &sorted {
            let d = Self::density(sst);
            while d > current_bucket.max_density {
                let next_boundary = current_bucket.max_density;
                if !current_bucket.sstables.is_empty() {
                    buckets.push(current_bucket);
                }
                boundary = next_boundary;
                current_bucket = DensityBucket {
                    sstables: Vec::new(),
                    total_size: 0,
                    min_density: boundary,
                    max_density: boundary * fan,
                };
            }
            current_bucket.sstables.push(sst.id);
            current_bucket.total_size += sst.data_size;
        }

        if !current_bucket.sstables.is_empty() {
            buckets.push(current_bucket);
        }

        buckets
    }
}

impl CompactionStrategy for UnifiedCompactionStrategy {
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
        let buckets = self.bucket_by_density(sstables);

        let mut result = Vec::new();

        for bucket in buckets {
            if bucket.sstables.len() >= self.min_threshold {
                let mut group = bucket.sstables;
                group.truncate(self.max_threshold);
                result.push(group);
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: u64, size: u64, partitions: u64) -> SSTableMetadata {
        SSTableMetadata {
            id,
            data_size: size,
            partition_count: partitions,
            min_timestamp: 0,
            max_timestamp: 1000,
        }
    }

    #[test]
    fn density_calculation() {
        let meta = make_meta(1, 1000, 100);
        assert!((UnifiedCompactionStrategy::density(&meta) - 10.0).abs() < 0.01);
    }

    #[test]
    fn fan_factor_tiered() {
        let ucs = UnifiedCompactionStrategy {
            scaling: ScalingParameter {
                values: [2, 0, 0, 0, 0, 0, 0, 0],
                count: 1,
            },
            ..Default::default()
        };
        assert_eq!(ucs.fan_factor(0), 4); // 2 + W=2
    }

    #[test]
    fn fan_factor_leveled() {
        let ucs = UnifiedCompactionStrategy {
            scaling: ScalingParameter {
                values: [-2, 0, 0, 0, 0, 0, 0, 0],
                count: 1,
            },
            ..Default::default()
        };
        assert_eq!(ucs.fan_factor(0), 2);
    }

    #[test]
    fn picks_similar_density_sstables() {
        let ucs = UnifiedCompactionStrategy {
            min_threshold: 2,
            ..Default::default()
        };

        // SSTables with similar density
        let sstables = vec![
            make_meta(1, 1000, 100),   // density 10
            make_meta(2, 1200, 100),   // density 12
            make_meta(3, 1100, 100),   // density 11
            make_meta(4, 100000, 100), // density 1000 (different bucket)
        ];

        let picks = ucs.pick_compaction(&sstables);
        assert!(!picks.is_empty());
        // First group should have the similar-density SSTables
        let first = &picks[0];
        assert!(first.len() >= 2);
    }

    #[test]
    fn no_compaction_below_threshold() {
        let ucs = UnifiedCompactionStrategy::default();
        let sstables = vec![make_meta(1, 100, 10)];
        let picks = ucs.pick_compaction(&sstables);
        assert!(picks.is_empty());
    }

    #[test]
    fn scaling_parameter_defaults() {
        let sp = ScalingParameter::default();
        assert_eq!(sp.get(0), 0);
        assert_eq!(sp.get(5), 0);
        assert_eq!(sp.get(100), 0);
    }
}
