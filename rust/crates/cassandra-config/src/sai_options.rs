// Licensed under Apache License, Version 2.0.

//! Storage Attached Index (SAI) configuration options.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sai.StorageAttachedIndexOptions`

use crate::units::DataSize;
use serde::{Deserialize, Serialize};

/// Configuration for Storage Attached Indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageAttachedIndexOptions {
    /// Maximum segment buffer size for index building.
    #[serde(default = "defaults::segment_buffer_size")]
    pub segment_buffer_size: DataSize,

    /// Whether to optimize on-disk index for reads (more CPU at write time).
    #[serde(default)]
    pub optimize_for: OptimizeFor,

    /// Maximum number of terms in a single posting list segment.
    #[serde(default = "defaults::max_terms_per_segment")]
    pub max_terms_per_segment: u32,

    /// Whether vector index support is enabled.
    #[serde(default = "defaults::vector_enabled")]
    pub vector_enabled: bool,

    /// Maximum dimensions for vector indexes.
    #[serde(default = "defaults::max_vector_dimensions")]
    pub max_vector_dimensions: u32,
}

/// Optimization target for SAI index building.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizeFor {
    /// Balance between read and write performance (default).
    #[default]
    Balanced,
    /// Optimize for read latency at the cost of write throughput.
    Reads,
    /// Optimize for write throughput at the cost of read latency.
    Writes,
}

impl Default for StorageAttachedIndexOptions {
    fn default() -> Self {
        Self {
            segment_buffer_size: defaults::segment_buffer_size(),
            optimize_for: OptimizeFor::default(),
            max_terms_per_segment: defaults::max_terms_per_segment(),
            vector_enabled: defaults::vector_enabled(),
            max_vector_dimensions: defaults::max_vector_dimensions(),
        }
    }
}

impl StorageAttachedIndexOptions {
    /// Validate the options, returning errors if invalid.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.segment_buffer_size.bytes() == 0 {
            errors.push("segment_buffer_size must be > 0".to_string());
        }
        if self.max_terms_per_segment == 0 {
            errors.push("max_terms_per_segment must be > 0".to_string());
        }
        if self.vector_enabled && self.max_vector_dimensions == 0 {
            errors.push("max_vector_dimensions must be > 0 when vectors enabled".to_string());
        }
        errors
    }
}

mod defaults {
    use super::*;

    /// 128 MiB default segment buffer.
    pub fn segment_buffer_size() -> DataSize {
        DataSize::from_mebibytes(128)
    }

    pub fn max_terms_per_segment() -> u32 {
        65536
    }

    pub fn vector_enabled() -> bool {
        true
    }

    pub fn max_vector_dimensions() -> u32 {
        2048
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options() {
        let opts = StorageAttachedIndexOptions::default();
        assert_eq!(opts.segment_buffer_size.mebibytes(), 128);
        assert_eq!(opts.optimize_for, OptimizeFor::Balanced);
        assert!(opts.vector_enabled);
    }

    #[test]
    fn validation_passes_defaults() {
        let opts = StorageAttachedIndexOptions::default();
        assert!(opts.validate().is_empty());
    }

    #[test]
    fn validation_catches_zero_buffer() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.segment_buffer_size = DataSize::ZERO;
        assert!(!opts.validate().is_empty());
    }

    #[test]
    fn serde_roundtrip() {
        let opts = StorageAttachedIndexOptions::default();
        let json = serde_json::to_string(&opts).unwrap();
        let opts2: StorageAttachedIndexOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(opts2.segment_buffer_size, opts.segment_buffer_size);
    }
}
