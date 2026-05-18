// Licensed under Apache License, Version 2.0.

//! Time-Window Compaction Strategy (TWCS).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy`
//!
//! ## Architecture
//!
//! Groups SSTables by time window based on their max timestamp.
//! Within each window, STCS-like bucketing is used. Cross-window
//! compaction is avoided to preserve time-based reads.
//! Ideal for time-series workloads with TTL.

use std::collections::HashMap;

use super::{
    CompactionStrategy, SSTableMetadata, SizeTieredCompactionStrategy, parse_bool, parse_i64,
    parse_positive_usize,
};
use crate::sstable::format::SSTableId;

/// Time unit for window calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minutes,
    Hours,
    Days,
}

impl TimeUnit {
    fn as_micros(self) -> i64 {
        match self {
            TimeUnit::Minutes => 60 * 1_000_000,
            TimeUnit::Hours => 3600 * 1_000_000,
            TimeUnit::Days => 86400 * 1_000_000,
        }
    }
}

impl TryFrom<&str> for TimeUnit {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "MINUTES" => Ok(Self::Minutes),
            "HOURS" => Ok(Self::Hours),
            "DAYS" => Ok(Self::Days),
            other => Err(format!("{other} is not valid for compaction_window_unit")),
        }
    }
}

/// Timestamp unit used by Java TWCS for max-timestamp windowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampResolution {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl Default for TimestampResolution {
    fn default() -> Self {
        Self::Microseconds
    }
}

impl TryFrom<&str> for TimestampResolution {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "SECONDS" => Ok(Self::Seconds),
            "MILLISECONDS" => Ok(Self::Milliseconds),
            "MICROSECONDS" => Ok(Self::Microseconds),
            "NANOSECONDS" => Ok(Self::Nanoseconds),
            other => Err(format!("{other} is not valid for timestamp_resolution")),
        }
    }
}

/// Time-Window Compaction Strategy.
#[derive(Debug, Clone)]
pub struct TimeWindowCompactionStrategy {
    /// Time unit for the window.
    pub time_unit: TimeUnit,
    /// Window size in the given time unit.
    pub window_size: i64,
    /// STCS parameters for within-window compaction.
    pub stcs: SizeTieredCompactionStrategy,
    /// Timestamp resolution used when bucketing SSTable max timestamps.
    pub timestamp_resolution: TimestampResolution,
    /// Java expired SSTable check frequency, in seconds.
    pub expired_sstable_check_frequency_seconds: u64,
    /// If true, expired SSTables (all data past TTL) are dropped eagerly.
    pub unsafe_aggressive_sstable_expiration: bool,
}

impl Default for TimeWindowCompactionStrategy {
    fn default() -> Self {
        Self {
            time_unit: TimeUnit::Days,
            window_size: 1,
            stcs: SizeTieredCompactionStrategy {
                min_threshold: 4,
                ..Default::default()
            },
            timestamp_resolution: TimestampResolution::default(),
            expired_sstable_check_frequency_seconds: 10 * 60,
            unsafe_aggressive_sstable_expiration: false,
        }
    }
}

impl TimeWindowCompactionStrategy {
    /// Build TWCS from the Java compaction option map.
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, String> {
        let mut strategy = Self::default();
        strategy.stcs = SizeTieredCompactionStrategy::from_options(options)?;

        if let Some(value) = options.get("compaction_window_unit") {
            strategy.time_unit = TimeUnit::try_from(value.as_str())?;
        }
        if let Some(value) = options.get("compaction_window_size") {
            strategy.window_size = parse_i64(value, "compaction_window_size")?;
            if strategy.window_size < 1 {
                return Err(format!(
                    "compaction_window_size must be positive: {}",
                    strategy.window_size
                ));
            }
        }
        if let Some(value) = options.get("timestamp_resolution") {
            strategy.timestamp_resolution = TimestampResolution::try_from(value.as_str())?;
        }
        if let Some(value) = options.get("expired_sstable_check_frequency_seconds") {
            let parsed = parse_i64(value, "expired_sstable_check_frequency_seconds")?;
            if parsed < 0 {
                return Err(format!(
                    "expired_sstable_check_frequency_seconds must not be negative: {parsed}"
                ));
            }
            strategy.expired_sstable_check_frequency_seconds = parsed as u64;
        }
        if let Some(value) = options.get("unsafe_aggressive_sstable_expiration") {
            strategy.unsafe_aggressive_sstable_expiration =
                parse_bool(value, "unsafe_aggressive_sstable_expiration")?;
        }
        if let Some(value) = options.get("min_threshold") {
            strategy.stcs.min_threshold = parse_positive_usize(value, "min_threshold")?;
        }
        if let Some(value) = options.get("max_threshold") {
            strategy.stcs.max_threshold = parse_positive_usize(value, "max_threshold")?;
        }
        Ok(strategy)
    }

    /// Calculate the window ID for a given timestamp (microseconds since epoch).
    pub fn window_for(&self, timestamp_micros: i64) -> i64 {
        let window_micros = self.time_unit.as_micros() * self.window_size;
        if window_micros == 0 {
            return 0;
        }
        timestamp_micros / window_micros
    }

    /// The current window based on system time.
    pub fn current_window(&self) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        self.window_for(now)
    }

    /// Group SSTables by time window. Returns window → SSTables mapping.
    fn group_by_window<'a>(
        &self,
        sstables: &'a [SSTableMetadata],
    ) -> std::collections::BTreeMap<i64, Vec<&'a SSTableMetadata>> {
        let mut groups: std::collections::BTreeMap<i64, Vec<&SSTableMetadata>> =
            std::collections::BTreeMap::new();
        for sst in sstables {
            let window = self.window_for(sst.max_timestamp);
            groups.entry(window).or_default().push(sst);
        }
        groups
    }
}

impl CompactionStrategy for TimeWindowCompactionStrategy {
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
        let groups = self.group_by_window(sstables);
        let current_window = self.current_window();

        let mut result = Vec::new();

        for (&window, ssts) in &groups {
            // Only compact within non-current windows (current window still receiving writes)
            // or if the current window has enough SSTables
            if window == current_window && ssts.len() < self.stcs.min_threshold * 2 {
                continue;
            }

            // Convert to SSTableMetadata slice for STCS
            let sst_vec: Vec<SSTableMetadata> = ssts.iter().map(|s| (*s).clone()).collect();
            let sub_picks = self.stcs.pick_compaction(&sst_vec);
            result.extend(sub_picks);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: u64, size: u64, max_ts: i64) -> SSTableMetadata {
        SSTableMetadata {
            id,
            data_size: size,
            partition_count: 100,
            min_timestamp: max_ts - 3600_000_000, // 1 hour before max
            max_timestamp: max_ts,
        }
    }

    #[test]
    fn window_calculation() {
        let twcs = TimeWindowCompactionStrategy {
            time_unit: TimeUnit::Hours,
            window_size: 1,
            ..Default::default()
        };

        let hour_micros = 3600 * 1_000_000i64;
        assert_eq!(twcs.window_for(0), 0);
        assert_eq!(twcs.window_for(hour_micros - 1), 0);
        assert_eq!(twcs.window_for(hour_micros), 1);
        assert_eq!(twcs.window_for(hour_micros * 2 + 1), 2);
    }

    #[test]
    fn groups_by_window() {
        let twcs = TimeWindowCompactionStrategy {
            time_unit: TimeUnit::Hours,
            window_size: 1,
            ..Default::default()
        };

        let hour = 3600 * 1_000_000i64;
        let sstables = vec![
            make_meta(1, 100, hour * 0 + 100),
            make_meta(2, 100, hour * 0 + 200),
            make_meta(3, 100, hour * 1 + 100),
        ];

        let groups = twcs.group_by_window(&sstables);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&0].len(), 2);
        assert_eq!(groups[&1].len(), 1);
    }

    #[test]
    fn compacts_within_closed_window() {
        let twcs = TimeWindowCompactionStrategy {
            time_unit: TimeUnit::Hours,
            window_size: 1,
            stcs: SizeTieredCompactionStrategy {
                min_threshold: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        // Old window (window 0) with enough SSTables
        let sstables = vec![
            make_meta(1, 100, 100),
            make_meta(2, 110, 200),
            make_meta(3, 105, 150),
        ];

        let picks = twcs.pick_compaction(&sstables);
        // Should pick from window 0 since it's not the current window
        assert!(!picks.is_empty());
    }

    #[test]
    fn no_compaction_single_sst() {
        let twcs = TimeWindowCompactionStrategy::default();
        let sstables = vec![make_meta(1, 100, 100)];
        let picks = twcs.pick_compaction(&sstables);
        assert!(picks.is_empty());
    }

    #[test]
    fn parses_java_options() {
        let options = HashMap::from([
            ("compaction_window_unit".to_string(), "HOURS".to_string()),
            ("compaction_window_size".to_string(), "6".to_string()),
            (
                "timestamp_resolution".to_string(),
                "MILLISECONDS".to_string(),
            ),
            (
                "expired_sstable_check_frequency_seconds".to_string(),
                "120".to_string(),
            ),
            (
                "unsafe_aggressive_sstable_expiration".to_string(),
                "true".to_string(),
            ),
            ("min_threshold".to_string(), "2".to_string()),
            ("max_threshold".to_string(), "8".to_string()),
            ("bucket_low".to_string(), "0.4".to_string()),
            ("bucket_high".to_string(), "1.6".to_string()),
        ]);
        let twcs = TimeWindowCompactionStrategy::from_options(&options).unwrap();
        assert_eq!(twcs.time_unit, TimeUnit::Hours);
        assert_eq!(twcs.window_size, 6);
        assert_eq!(twcs.timestamp_resolution, TimestampResolution::Milliseconds);
        assert_eq!(twcs.expired_sstable_check_frequency_seconds, 120);
        assert!(twcs.unsafe_aggressive_sstable_expiration);
        assert_eq!(twcs.stcs.min_threshold, 2);
        assert_eq!(twcs.stcs.max_threshold, 8);
        assert!((twcs.stcs.bucket_low - 0.4).abs() < 0.001);
        assert!((twcs.stcs.bucket_high - 1.6).abs() < 0.001);
    }
}
