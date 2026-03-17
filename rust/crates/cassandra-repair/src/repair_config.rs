// Licensed under Apache License, Version 2.0.

//! Repair configuration.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.RepairConfig`
//! - `org.apache.cassandra.config.RetrySpec`

use serde::{Deserialize, Serialize};

/// Configuration for the repair subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairConfig {
    /// Maximum number of concurrent repair sessions per node.
    #[serde(default = "defaults::max_concurrent_sessions")]
    pub max_concurrent_sessions: u32,

    /// Retry configuration for failed repair tasks.
    #[serde(default)]
    pub retry: RetrySpec,

    /// Merkle tree response configuration.
    #[serde(default)]
    pub merkle_tree: MerkleTreeResponseSpec,

    /// Whether to enable preview repairs (read-only validation).
    #[serde(default = "defaults::preview_enabled")]
    pub preview_enabled: bool,

    /// Timeout for a single repair session in milliseconds.
    #[serde(default = "defaults::session_timeout_ms")]
    pub session_timeout_ms: u64,

    /// Maximum depth of merkle trees.
    #[serde(default = "defaults::max_merkle_tree_depth")]
    pub max_merkle_tree_depth: u32,
}

/// Retry behaviour for failed repair tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrySpec {
    /// Maximum number of retries.
    #[serde(default = "defaults::max_retries")]
    pub max_retries: u32,

    /// Initial backoff delay in milliseconds.
    #[serde(default = "defaults::initial_backoff_ms")]
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds.
    #[serde(default = "defaults::max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// Backoff multiplier (exponential backoff).
    #[serde(default = "defaults::backoff_multiplier")]
    pub backoff_multiplier: f64,
}

/// Merkle tree response sub-spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTreeResponseSpec {
    /// Timeout waiting for a merkle tree response in milliseconds.
    #[serde(default = "defaults::merkle_response_timeout_ms")]
    pub response_timeout_ms: u64,

    /// Maximum size of a single merkle tree in bytes.
    #[serde(default = "defaults::max_merkle_tree_size")]
    pub max_tree_size: u64,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_concurrent_sessions: defaults::max_concurrent_sessions(),
            retry: RetrySpec::default(),
            merkle_tree: MerkleTreeResponseSpec::default(),
            preview_enabled: defaults::preview_enabled(),
            session_timeout_ms: defaults::session_timeout_ms(),
            max_merkle_tree_depth: defaults::max_merkle_tree_depth(),
        }
    }
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            max_retries: defaults::max_retries(),
            initial_backoff_ms: defaults::initial_backoff_ms(),
            max_backoff_ms: defaults::max_backoff_ms(),
            backoff_multiplier: defaults::backoff_multiplier(),
        }
    }
}

impl Default for MerkleTreeResponseSpec {
    fn default() -> Self {
        Self {
            response_timeout_ms: defaults::merkle_response_timeout_ms(),
            max_tree_size: defaults::max_merkle_tree_size(),
        }
    }
}

impl RepairConfig {
    /// Validate the configuration, returning errors if invalid.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.max_concurrent_sessions == 0 {
            errors.push("max_concurrent_sessions must be > 0".to_string());
        }
        if self.session_timeout_ms == 0 {
            errors.push("session_timeout_ms must be > 0".to_string());
        }
        if self.retry.backoff_multiplier < 1.0 {
            errors.push("backoff_multiplier must be >= 1.0".to_string());
        }
        if self.retry.initial_backoff_ms > self.retry.max_backoff_ms {
            errors.push("initial_backoff_ms must be <= max_backoff_ms".to_string());
        }
        if self.max_merkle_tree_depth == 0 {
            errors.push("max_merkle_tree_depth must be > 0".to_string());
        }
        errors
    }
}

mod defaults {
    pub fn max_concurrent_sessions() -> u32 {
        4
    }
    pub fn max_retries() -> u32 {
        3
    }
    pub fn initial_backoff_ms() -> u64 {
        1000
    }
    pub fn max_backoff_ms() -> u64 {
        60_000
    }
    pub fn backoff_multiplier() -> f64 {
        2.0
    }
    pub fn merkle_response_timeout_ms() -> u64 {
        600_000 // 10 minutes
    }
    pub fn max_merkle_tree_size() -> u64 {
        256 * 1024 * 1024 // 256 MiB
    }
    pub fn preview_enabled() -> bool {
        true
    }
    pub fn session_timeout_ms() -> u64 {
        3_600_000 // 1 hour
    }
    pub fn max_merkle_tree_depth() -> u32 {
        20
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let cfg = RepairConfig::default();
        assert!(cfg.validate().is_empty());
    }

    #[test]
    fn validation_catches_zero_sessions() {
        let mut cfg = RepairConfig::default();
        cfg.max_concurrent_sessions = 0;
        assert!(!cfg.validate().is_empty());
    }

    #[test]
    fn validation_catches_bad_backoff() {
        let mut cfg = RepairConfig::default();
        cfg.retry.backoff_multiplier = 0.5;
        assert!(!cfg.validate().is_empty());
    }

    #[test]
    fn validation_catches_inverted_backoff() {
        let mut cfg = RepairConfig::default();
        cfg.retry.initial_backoff_ms = 100_000;
        cfg.retry.max_backoff_ms = 1_000;
        assert!(!cfg.validate().is_empty());
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = RepairConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let cfg2: RepairConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg2.max_concurrent_sessions, cfg.max_concurrent_sessions);
    }
}
