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

        // segment_buffer_size: 1 MiB .. 1 GiB
        let seg_bytes = self.segment_buffer_size.bytes();
        if seg_bytes == 0 {
            errors.push("segment_buffer_size must be > 0".to_string());
        } else if seg_bytes < DataSize::from_mebibytes(1).bytes() {
            errors.push("segment_buffer_size must be >= 1 MiB".to_string());
        } else if seg_bytes > DataSize::from_gibibytes(1).bytes() {
            errors.push("segment_buffer_size must be <= 1 GiB".to_string());
        }

        // max_terms_per_segment: 1024 .. 1_000_000
        if self.max_terms_per_segment < 1024 {
            errors.push("max_terms_per_segment must be >= 1024".to_string());
        } else if self.max_terms_per_segment > 1_000_000 {
            errors.push("max_terms_per_segment must be <= 1000000".to_string());
        }

        // max_vector_dimensions: <= 8192
        if self.vector_enabled && self.max_vector_dimensions == 0 {
            errors.push("max_vector_dimensions must be > 0 when vectors enabled".to_string());
        }
        if self.max_vector_dimensions > 8192 {
            errors.push("max_vector_dimensions must be <= 8192".to_string());
        }

        errors
    }
}

/// Known SAI option keys for CREATE INDEX validation.
const KNOWN_SAI_OPTION_KEYS: &[&str] = &[
    "target",
    "class_name",
    "vector_dimensions",
    "vector_similarity_metric",
    "segment_buffer_size",
    "max_terms_per_segment",
    "optimize_for",
    "case_sensitive",
    "normalize",
    "ascii",
];

/// Validate a CREATE INDEX option map for SAI indexes.
/// Returns a list of warnings and errors.
pub fn validate_index_definition(
    options: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    let mut warnings = Vec::new();

    // Check for unknown keys
    for key in options.keys() {
        if !KNOWN_SAI_OPTION_KEYS.contains(&key.as_str()) {
            warnings.push(format!("unknown SAI option key: '{}'", key));
        }
    }

    // Validate vector_dimensions range
    if let Some(dims_str) = options.get("vector_dimensions") {
        match dims_str.parse::<u32>() {
            Ok(dims) if dims == 0 => {
                warnings.push("vector_dimensions must be > 0".to_string());
            }
            Ok(dims) if dims > 8192 => {
                warnings.push("vector_dimensions must be <= 8192".to_string());
            }
            Err(_) => {
                warnings.push(format!("invalid vector_dimensions value: '{}'", dims_str));
            }
            _ => {}
        }
    }

    // Validate similarity_metric enum
    if let Some(metric) = options.get("vector_similarity_metric") {
        let valid = ["cosine", "euclidean", "dot_product"];
        if !valid.contains(&metric.to_lowercase().as_str()) {
            warnings.push(format!(
                "unknown vector_similarity_metric '{}'; expected one of: cosine, euclidean, dot_product",
                metric
            ));
        }
    }

    warnings
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
    fn validation_catches_small_buffer() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.segment_buffer_size = DataSize(100); // 100 bytes, below 1 MiB
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("1 MiB")));
    }

    #[test]
    fn validation_catches_large_buffer() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.segment_buffer_size = DataSize::from_gibibytes(2);
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("1 GiB")));
    }

    #[test]
    fn validation_catches_small_terms() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.max_terms_per_segment = 100;
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("1024")));
    }

    #[test]
    fn validation_catches_large_terms() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.max_terms_per_segment = 2_000_000;
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("1000000")));
    }

    #[test]
    fn validation_catches_large_vector_dims() {
        let mut opts = StorageAttachedIndexOptions::default();
        opts.max_vector_dimensions = 10000;
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("8192")));
    }

    #[test]
    fn validate_index_definition_unknown_key() {
        let mut opts = std::collections::HashMap::new();
        opts.insert("unknown_key".into(), "value".into());
        let warnings = super::validate_index_definition(&opts);
        assert!(warnings.iter().any(|w| w.contains("unknown")));
    }

    #[test]
    fn validate_index_definition_bad_dims() {
        let mut opts = std::collections::HashMap::new();
        opts.insert("vector_dimensions".into(), "0".into());
        let warnings = super::validate_index_definition(&opts);
        assert!(!warnings.is_empty());
    }

    #[test]
    fn validate_index_definition_bad_metric() {
        let mut opts = std::collections::HashMap::new();
        opts.insert("vector_similarity_metric".into(), "manhattan".into());
        let warnings = super::validate_index_definition(&opts);
        assert!(warnings.iter().any(|w| w.contains("manhattan")));
    }

    #[test]
    fn validate_index_definition_valid() {
        let mut opts = std::collections::HashMap::new();
        opts.insert("vector_dimensions".into(), "128".into());
        opts.insert("vector_similarity_metric".into(), "cosine".into());
        let warnings = super::validate_index_definition(&opts);
        assert!(warnings.is_empty());
    }

    #[test]
    fn serde_roundtrip() {
        let opts = StorageAttachedIndexOptions::default();
        let json = serde_json::to_string(&opts).unwrap();
        let opts2: StorageAttachedIndexOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(opts2.segment_buffer_size, opts.segment_buffer_size);
    }
}
