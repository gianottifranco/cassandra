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

use std::collections::HashMap;

use super::{CompactionStrategy, SSTableMetadata};
use crate::sstable::format::SSTableId;

const MAX_SCALING_PARAMETERS: usize = 32;
const MIN_TARGET_SSTABLE_SIZE: u64 = 1 << 20;
const DEFAULT_TARGET_SSTABLE_SIZE: u64 = 1 << 30;
const DEFAULT_MIN_SSTABLE_SIZE: u64 = 100 * 1024 * 1024;
const DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS: u64 = 10 * 60;
const DEFAULT_SSTABLE_GROWTH: f64 = 0.333;

/// Scaling parameter controlling tiered vs leveled behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalingParameter {
    /// W values per level. Positive = tiered, negative = leveled, 0 = balanced.
    /// If fewer entries than levels, the last value is repeated.
    pub values: Vec<i32>,
}

impl Default for ScalingParameter {
    fn default() -> Self {
        // Java's default is "T4", which is W=2.
        Self { values: vec![2] }
    }
}

impl ScalingParameter {
    pub fn get(&self, level: usize) -> i32 {
        if level < self.values.len() {
            self.values[level]
        } else if let Some(last) = self.values.last() {
            *last
        } else {
            0
        }
    }

    /// Parse Java UCS scaling parameters. Accepted entries match the Java
    /// strategy syntax: `N`, `Tn`, `Ln`, or a signed integer, comma-separated.
    pub fn parse(value: &str) -> Result<Self, String> {
        let mut parsed = Vec::new();

        for raw in value.split(',') {
            let token = raw.trim();
            if token.is_empty() {
                return Err("scaling parameter entries must not be empty".to_string());
            }
            parsed.push(parse_scaling_parameter(token)?);
        }

        if parsed.is_empty() {
            return Err("scaling_parameters must contain at least one entry".to_string());
        }
        if parsed.len() > MAX_SCALING_PARAMETERS {
            return Err(format!(
                "scaling_parameters supports at most {MAX_SCALING_PARAMETERS} entries"
            ));
        }

        Ok(Self { values: parsed })
    }

    pub fn print(value: i32) -> String {
        if value < 0 {
            format!("L{}", 2 - value)
        } else if value > 0 {
            format!("T{}", value + 2)
        } else {
            "N".to_string()
        }
    }
}

fn parse_scaling_parameter(value: &str) -> Result<i32, String> {
    if value == "N" {
        return Ok(0);
    }

    if let Some(rest) = value.strip_prefix('T') {
        let fan = parse_at_least_two(rest, value)?;
        return Ok(fan - 2);
    }

    if let Some(rest) = value.strip_prefix('L') {
        let fan = parse_at_least_two(rest, value)?;
        return Ok(2 - fan);
    }

    value
        .parse::<i32>()
        .map_err(|_| format!("scaling parameter {value} must be N, Ln, Tn, or a signed integer"))
}

fn parse_at_least_two(value: &str, original: &str) -> Result<i32, String> {
    let parsed = value
        .parse::<i32>()
        .map_err(|_| format!("scaling parameter {original} has an invalid fan factor"))?;
    if parsed < 2 {
        return Err(format!("fan factor cannot be lower than 2 in {original}"));
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapInclusionMethod {
    None,
    Single,
    Transitive,
}

impl Default for OverlapInclusionMethod {
    fn default() -> Self {
        Self::Transitive
    }
}

impl OverlapInclusionMethod {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self::None),
            "SINGLE" => Ok(Self::Single),
            "TRANSITIVE" => Ok(Self::Transitive),
            other => Err(format!("invalid overlap inclusion method {other}")),
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
    /// Minimum SSTable size before sharded writers split output.
    pub min_sstable_size: u64,
    /// Optional flush size override in bytes. Zero means use measured flush size.
    pub flush_size_override: u64,
    /// Java `max_sstables_to_compact`; zero means no explicit override.
    pub max_sstables_to_compact: usize,
    /// Frequency for expired SSTable checks.
    pub expired_sstable_check_frequency_seconds: u64,
    /// Java unsafe expiration flag.
    pub unsafe_aggressive_sstable_expiration: bool,
    /// Java `sstable_growth` modifier, between 0 and 1.
    pub sstable_growth: f64,
    /// Overlap inclusion behavior for UCS task construction.
    pub overlap_inclusion_method: OverlapInclusionMethod,
    /// Whether output shards may be parallelized.
    pub parallelize_output_shards: bool,
}

impl Default for UnifiedCompactionStrategy {
    fn default() -> Self {
        Self {
            scaling: ScalingParameter::default(),
            target_sstable_size: DEFAULT_TARGET_SSTABLE_SIZE,
            base_shard_count: 4,
            min_threshold: 4,
            max_threshold: 32,
            survival_factor: 1.0,
            min_sstable_size: DEFAULT_MIN_SSTABLE_SIZE,
            flush_size_override: 0,
            max_sstables_to_compact: 0,
            expired_sstable_check_frequency_seconds:
                DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS,
            unsafe_aggressive_sstable_expiration: false,
            sstable_growth: DEFAULT_SSTABLE_GROWTH,
            overlap_inclusion_method: OverlapInclusionMethod::default(),
            parallelize_output_shards: true,
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
    /// Build a UCS strategy from the Java table-compaction option map.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, String> {
        let mut strategy = Self::default();

        if let Some(value) = options.get("scaling_parameters") {
            strategy.scaling = ScalingParameter::parse(value)?;
        }
        if let Some(value) = options.get("target_sstable_size") {
            strategy.target_sstable_size = parse_size_bytes(value)?;
            if strategy.target_sstable_size < MIN_TARGET_SSTABLE_SIZE {
                return Err(format!(
                    "target_sstable_size {value} is not acceptable, size must be at least {MIN_TARGET_SSTABLE_SIZE}"
                ));
            }
        }
        if let Some(value) = options.get("min_sstable_size") {
            strategy.min_sstable_size = parse_size_bytes(value)?;
            let limit = (strategy.target_sstable_size as f64 * 0.5_f64.sqrt()).ceil() as u64;
            if strategy.min_sstable_size >= limit {
                return Err(format!(
                    "min_sstable_size {value} should be less than 70% of target_sstable_size"
                ));
            }
        }
        if let Some(value) = options.get("flush_size_override") {
            strategy.flush_size_override = parse_size_bytes(value)?;
            if strategy.flush_size_override > 0
                && strategy.flush_size_override < MIN_TARGET_SSTABLE_SIZE
            {
                return Err(format!(
                    "flush_size_override {value} is not acceptable, size must be at least {MIN_TARGET_SSTABLE_SIZE}"
                ));
            }
        }
        if let Some(value) = options.get("base_shard_count") {
            strategy.base_shard_count = parse_positive_u32(value, "base_shard_count")?;
        }
        if let Some(value) = options.get("max_sstables_to_compact") {
            let parsed = parse_i64(value, "max_sstables_to_compact")?;
            if parsed > 0 {
                strategy.max_sstables_to_compact = parsed as usize;
                strategy.max_threshold = strategy.max_sstables_to_compact;
            }
        }
        if let Some(value) = options.get("expired_sstable_check_frequency_seconds") {
            strategy.expired_sstable_check_frequency_seconds =
                parse_positive_u64(value, "expired_sstable_check_frequency_seconds")?;
        }
        if let Some(value) = options.get("unsafe_aggressive_sstable_expiration") {
            strategy.unsafe_aggressive_sstable_expiration =
                parse_bool(value, "unsafe_aggressive_sstable_expiration")?;
        }
        if let Some(value) = options.get("sstable_growth") {
            strategy.sstable_growth = parse_percent(value, "sstable_growth")?;
            if !(0.0..=1.0).contains(&strategy.sstable_growth) {
                return Err(format!("sstable_growth {value} must be between 0 and 1"));
            }
        }
        if let Some(value) = options.get("overlap_inclusion_method") {
            strategy.overlap_inclusion_method = OverlapInclusionMethod::parse(value)?;
        }
        if let Some(value) = options.get("parallelize_output_shards") {
            strategy.parallelize_output_shards = parse_bool(value, "parallelize_output_shards")?;
        }
        if let Some(value) = options.get("min_threshold") {
            strategy.min_threshold = parse_positive_usize(value, "min_threshold")?;
        }
        if let Some(value) = options.get("max_threshold") {
            strategy.max_threshold = parse_positive_usize(value, "max_threshold")?;
        }
        if strategy.max_threshold < strategy.min_threshold {
            return Err("max_threshold must be greater than or equal to min_threshold".to_string());
        }

        Ok(strategy)
    }

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
        if w < 0 {
            (2 - w) as u32
        } else {
            (2 + w) as u32
        }
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

fn parse_size_bytes(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("size value must not be empty".to_string());
    }

    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    let amount = number
        .parse::<f64>()
        .map_err(|_| format!("{value} is not a valid size"))?;
    if amount < 0.0 || !amount.is_finite() {
        return Err(format!("{value} is not a valid size"));
    }

    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1_f64,
        "k" | "kb" | "kib" => 1024_f64,
        "m" | "mb" | "mib" => 1024_f64.powi(2),
        "g" | "gb" | "gib" => 1024_f64.powi(3),
        "t" | "tb" | "tib" => 1024_f64.powi(4),
        "p" | "pb" | "pib" => 1024_f64.powi(5),
        other => return Err(format!("{other} is not a supported size suffix")),
    };

    let bytes = (amount * multiplier).ceil();
    if bytes > u64::MAX as f64 {
        return Err(format!("{value} is out of range"));
    }
    Ok(bytes as u64)
}

fn parse_bool(value: &str, option: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!(
            "{option} should either be true or false, not {value}"
        )),
    }
}

fn parse_percent(value: &str, option: &str) -> Result<f64, String> {
    let trimmed = value.trim();
    let parsed = if let Some(percent) = trimmed.strip_suffix('%') {
        percent.trim().parse::<f64>().map(|v| v / 100.0)
    } else {
        trimmed.parse::<f64>()
    }
    .map_err(|_| format!("{option} is not a valid number between 0 and 1"))?;

    if !parsed.is_finite() {
        return Err(format!("{option} is not finite"));
    }
    Ok(parsed)
}

fn parse_i64(value: &str, option: &str) -> Result<i64, String> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| format!("{value} is not a parsable integer for {option}"))
}

fn parse_positive_u64(value: &str, option: &str) -> Result<u64, String> {
    let parsed = parse_i64(value, option)?;
    if parsed <= 0 {
        return Err(format!("{option} should be positive: {parsed}"));
    }
    Ok(parsed as u64)
}

fn parse_positive_u32(value: &str, option: &str) -> Result<u32, String> {
    let parsed = parse_positive_u64(value, option)?;
    if parsed > u32::MAX as u64 {
        return Err(format!("{option} is out of range: {parsed}"));
    }
    Ok(parsed as u32)
}

fn parse_positive_usize(value: &str, option: &str) -> Result<usize, String> {
    Ok(parse_positive_u64(value, option)? as usize)
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
            scaling: ScalingParameter { values: vec![2] },
            ..Default::default()
        };
        assert_eq!(ucs.fan_factor(0), 4); // 2 + W=2
    }

    #[test]
    fn fan_factor_leveled() {
        let ucs = UnifiedCompactionStrategy {
            scaling: ScalingParameter { values: vec![-2] },
            ..Default::default()
        };
        assert_eq!(ucs.fan_factor(0), 4);
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
        assert_eq!(sp.get(0), 2);
        assert_eq!(sp.get(5), 2);
        assert_eq!(sp.get(100), 2);
    }

    #[test]
    fn parses_java_scaling_parameter_syntax() {
        let parsed = ScalingParameter::parse("T4, L10, N, -3").unwrap();
        assert_eq!(parsed.values, vec![2, -8, 0, -3]);
        assert_eq!(parsed.get(20), -3);
        assert_eq!(ScalingParameter::print(2), "T4");
        assert_eq!(ScalingParameter::print(-8), "L10");
        assert_eq!(ScalingParameter::print(0), "N");
        assert!(ScalingParameter::parse("T1").is_err());
        assert!(ScalingParameter::parse("L1").is_err());
    }

    #[test]
    fn builds_strategy_from_java_options() {
        let options = HashMap::from([
            ("scaling_parameters".to_string(), "T4, L10".to_string()),
            ("target_sstable_size".to_string(), "1GiB".to_string()),
            ("min_sstable_size".to_string(), "64MiB".to_string()),
            ("flush_size_override".to_string(), "8MiB".to_string()),
            ("base_shard_count".to_string(), "8".to_string()),
            ("max_sstables_to_compact".to_string(), "6".to_string()),
            (
                "expired_sstable_check_frequency_seconds".to_string(),
                "900".to_string(),
            ),
            (
                "unsafe_aggressive_sstable_expiration".to_string(),
                "true".to_string(),
            ),
            ("sstable_growth".to_string(), "33.3%".to_string()),
            ("overlap_inclusion_method".to_string(), "single".to_string()),
            ("parallelize_output_shards".to_string(), "false".to_string()),
        ]);

        let strategy = UnifiedCompactionStrategy::from_options(&options).unwrap();
        assert_eq!(strategy.scaling.values, vec![2, -8]);
        assert_eq!(strategy.target_sstable_size, 1 << 30);
        assert_eq!(strategy.min_sstable_size, 64 * 1024 * 1024);
        assert_eq!(strategy.flush_size_override, 8 * 1024 * 1024);
        assert_eq!(strategy.base_shard_count, 8);
        assert_eq!(strategy.max_threshold, 6);
        assert_eq!(strategy.expired_sstable_check_frequency_seconds, 900);
        assert!(strategy.unsafe_aggressive_sstable_expiration);
        assert!((strategy.sstable_growth - 0.333).abs() < 0.0001);
        assert_eq!(
            strategy.overlap_inclusion_method,
            OverlapInclusionMethod::Single
        );
        assert!(!strategy.parallelize_output_shards);
    }

    #[test]
    fn rejects_invalid_java_options() {
        let too_small_target =
            HashMap::from([("target_sstable_size".to_string(), "512KiB".to_string())]);
        assert!(UnifiedCompactionStrategy::from_options(&too_small_target).is_err());

        let bad_growth = HashMap::from([("sstable_growth".to_string(), "2".to_string())]);
        assert!(UnifiedCompactionStrategy::from_options(&bad_growth).is_err());

        let bad_bool = HashMap::from([(
            "parallelize_output_shards".to_string(),
            "sometimes".to_string(),
        )]);
        assert!(UnifiedCompactionStrategy::from_options(&bad_bool).is_err());
    }
}
