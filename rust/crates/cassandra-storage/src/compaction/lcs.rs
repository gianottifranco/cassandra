// Licensed under Apache License, Version 2.0.

//! Leveled Compaction Strategy (LCS).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.LeveledCompactionStrategy`
//!
//! ## Architecture
//!
//! Organizes SSTables into levels (L0, L1, L2, ...).
//! - L0: incoming flushes, compacted using STCS-like bucketing
//! - L1+: non-overlapping SSTables per level
//! - Compaction picks the level with the highest score
//! - Score = actual_size / max_size_for_level
//! - When compacting from Ln to Ln+1, overlapping SSTables at Ln+1
//!   are included in the merge

use std::collections::HashMap;

use super::{
    CompactionStrategy, SSTableMetadata, parse_bool, parse_positive_u32, parse_positive_usize,
};
use crate::sstable::format::SSTableId;

/// Leveled Compaction Strategy.
#[derive(Debug, Clone)]
pub struct LeveledCompactionStrategy {
    /// Target SSTable size (bytes). Default: 160 MiB.
    pub sstable_size_in_mb: u64,
    /// Fanout size: level L can hold fanout^L SSTables (default: 10).
    pub fanout_size: u32,
    /// Maximum number of SSTables in L0 before triggering compaction.
    pub l0_threshold: usize,
    /// Maximum number of L0 SSTables to compact together.
    pub max_threshold: usize,
    /// Java `single_sstable_uplevel` option.
    pub single_sstable_uplevel: bool,
}

impl Default for LeveledCompactionStrategy {
    fn default() -> Self {
        Self {
            sstable_size_in_mb: 160,
            fanout_size: 10,
            l0_threshold: 4,
            max_threshold: 32,
            single_sstable_uplevel: true,
        }
    }
}

/// Level assignment for an SSTable.
#[derive(Debug, Clone)]
pub struct LeveledSSTable {
    pub metadata: SSTableMetadata,
    pub level: u32,
    /// Approximate start of the key range (for overlap detection).
    pub min_key_hash: u64,
    /// Approximate end of the key range.
    pub max_key_hash: u64,
}

impl LeveledCompactionStrategy {
    /// Build LCS from the Java compaction option map.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, String> {
        let mut strategy = Self::default();
        if let Some(value) = options.get("sstable_size_in_mb") {
            strategy.sstable_size_in_mb = parse_positive_usize(value, "sstable_size_in_mb")? as u64;
        }
        if let Some(value) = options.get("fanout_size") {
            strategy.fanout_size = parse_positive_u32(value, "fanout_size")?;
        }
        if let Some(value) = options.get("min_threshold") {
            strategy.l0_threshold = parse_positive_usize(value, "min_threshold")?;
        }
        if let Some(value) = options.get("max_threshold") {
            strategy.max_threshold = parse_positive_usize(value, "max_threshold")?;
        }
        if strategy.max_threshold < strategy.l0_threshold {
            return Err("max_threshold must be greater than or equal to min_threshold".to_string());
        }
        if let Some(value) = options.get("single_sstable_uplevel") {
            strategy.single_sstable_uplevel = parse_bool(value, "single_sstable_uplevel")?;
        }
        Ok(strategy)
    }

    /// Maximum total size for a given level (in bytes).
    pub fn max_bytes_for_level(&self, level: u32) -> u64 {
        let target = self.sstable_size_in_mb * 1024 * 1024;
        if level == 0 {
            return target * self.l0_threshold as u64;
        }
        target * (self.fanout_size as u64).pow(level)
    }

    /// Compute the score for a level: actual_size / max_size.
    pub fn level_score(&self, level: u32, actual_bytes: u64) -> f64 {
        let max = self.max_bytes_for_level(level);
        if max == 0 {
            return 0.0;
        }
        actual_bytes as f64 / max as f64
    }

    /// Pick compaction from leveled SSTables.
    /// Returns groups of SSTable IDs to compact together.
    pub fn pick_leveled_compaction(&self, sstables: &[LeveledSSTable]) -> Vec<Vec<SSTableId>> {
        // Group by level
        let mut levels: std::collections::BTreeMap<u32, Vec<&LeveledSSTable>> =
            std::collections::BTreeMap::new();
        for sst in sstables {
            levels.entry(sst.level).or_default().push(sst);
        }

        // L0 special case: if too many SSTables, compact all L0 + overlapping L1
        if let Some(l0) = levels.get(&0) {
            if l0.len() >= self.l0_threshold {
                let mut group: Vec<SSTableId> = l0
                    .iter()
                    .take(self.max_threshold)
                    .map(|s| s.metadata.id)
                    .collect();
                // Include overlapping L1 SSTables
                if let Some(l1) = levels.get(&1) {
                    let l0_min = l0.iter().map(|s| s.min_key_hash).min().unwrap_or(0);
                    let l0_max = l0.iter().map(|s| s.max_key_hash).max().unwrap_or(u64::MAX);
                    for l1_sst in l1 {
                        if l1_sst.max_key_hash >= l0_min && l1_sst.min_key_hash <= l0_max {
                            group.push(l1_sst.metadata.id);
                        }
                    }
                }
                return vec![group];
            }
        }

        // Find the level with the highest score > 1.0
        let mut best_level = None;
        let mut best_score = 1.0;

        for (&level, ssts) in &levels {
            if level == 0 {
                continue;
            }
            let total: u64 = ssts.iter().map(|s| s.metadata.data_size).sum();
            let score = self.level_score(level, total);
            if score > best_score {
                best_score = score;
                best_level = Some(level);
            }
        }

        if let Some(level) = best_level {
            if let Some(ssts) = levels.get(&level) {
                // Pick the oldest SSTable at this level
                if let Some(oldest) = ssts.iter().min_by_key(|s| s.metadata.id) {
                    let mut group = vec![oldest.metadata.id];
                    // Include overlapping SSTables in the next level
                    let next_level = level + 1;
                    if let Some(next_ssts) = levels.get(&next_level) {
                        for next_sst in next_ssts {
                            if next_sst.max_key_hash >= oldest.min_key_hash
                                && next_sst.min_key_hash <= oldest.max_key_hash
                            {
                                group.push(next_sst.metadata.id);
                            }
                        }
                    }
                    return vec![group];
                }
            }
        }

        vec![]
    }
}

impl CompactionStrategy for LeveledCompactionStrategy {
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
        // Without level information, treat all as L0
        if sstables.len() < self.l0_threshold {
            return vec![];
        }
        // If enough at "L0", compact them all together
        let ids: Vec<SSTableId> = sstables
            .iter()
            .take(self.max_threshold)
            .map(|s| s.id)
            .collect();
        vec![ids]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: u64, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            id,
            data_size: size,
            partition_count: 100,
            min_timestamp: 0,
            max_timestamp: 1000,
        }
    }

    fn make_leveled(id: u64, size: u64, level: u32, min_k: u64, max_k: u64) -> LeveledSSTable {
        LeveledSSTable {
            metadata: make_meta(id, size),
            level,
            min_key_hash: min_k,
            max_key_hash: max_k,
        }
    }

    #[test]
    fn max_bytes_for_level() {
        let lcs = LeveledCompactionStrategy::default();
        let l1 = lcs.max_bytes_for_level(1);
        let l2 = lcs.max_bytes_for_level(2);
        assert_eq!(l1, 160 * 1024 * 1024 * 10);
        assert_eq!(l2, 160 * 1024 * 1024 * 100);
    }

    #[test]
    fn l0_compaction_trigger() {
        let lcs = LeveledCompactionStrategy {
            l0_threshold: 2,
            ..Default::default()
        };

        let sstables = vec![
            make_leveled(1, 100, 0, 0, 50),
            make_leveled(2, 100, 0, 25, 75),
            make_leveled(3, 100, 1, 0, 100),
        ];

        let picks = lcs.pick_leveled_compaction(&sstables);
        assert_eq!(picks.len(), 1);
        // Should include L0 SSTables and overlapping L1
        assert!(picks[0].contains(&1));
        assert!(picks[0].contains(&2));
        assert!(picks[0].contains(&3));
    }

    #[test]
    fn level_score_calculation() {
        let lcs = LeveledCompactionStrategy::default();
        let score = lcs.level_score(1, lcs.max_bytes_for_level(1));
        assert!((score - 1.0).abs() < 0.01);
    }

    #[test]
    fn no_compaction_when_empty() {
        let lcs = LeveledCompactionStrategy::default();
        let picks = lcs.pick_leveled_compaction(&[]);
        assert!(picks.is_empty());
    }

    #[test]
    fn parses_java_options() {
        let options = HashMap::from([
            ("sstable_size_in_mb".to_string(), "32".to_string()),
            ("fanout_size".to_string(), "4".to_string()),
            ("min_threshold".to_string(), "2".to_string()),
            ("max_threshold".to_string(), "3".to_string()),
            ("single_sstable_uplevel".to_string(), "false".to_string()),
        ]);
        let lcs = LeveledCompactionStrategy::from_options(&options).unwrap();
        assert_eq!(lcs.sstable_size_in_mb, 32);
        assert_eq!(lcs.fanout_size, 4);
        assert_eq!(lcs.l0_threshold, 2);
        assert_eq!(lcs.max_threshold, 3);
        assert!(!lcs.single_sstable_uplevel);
    }
}
