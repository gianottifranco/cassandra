// Licensed under Apache License, Version 2.0.

//! Configuration validation.

use crate::config::CassandraConfig;

/// A configuration validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config error [{}]: {}", self.field, self.message)
    }
}
impl std::error::Error for ConfigError {}

/// Validate a configuration, returning all errors found.
pub fn validate(config: &CassandraConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();

    // Cluster name must not be empty
    if config.cluster_name.is_empty() {
        errors.push(ConfigError {
            field: "cluster_name".into(),
            message: "cluster_name must not be empty".into(),
        });
    }

    // num_tokens must be >= 1
    if config.num_tokens == 0 {
        errors.push(ConfigError {
            field: "num_tokens".into(),
            message: "num_tokens must be at least 1".into(),
        });
    }

    // Port range checks
    if config.native_transport_port == 0 {
        errors.push(ConfigError {
            field: "native_transport_port".into(),
            message: "native_transport_port must be > 0".into(),
        });
    }
    if config.storage_port == 0 {
        errors.push(ConfigError {
            field: "storage_port".into(),
            message: "storage_port must be > 0".into(),
        });
    }

    // Concurrent must be > 0
    for (name, val) in [
        ("concurrent_reads", config.concurrent_reads),
        ("concurrent_writes", config.concurrent_writes),
        (
            "concurrent_counter_writes",
            config.concurrent_counter_writes,
        ),
    ] {
        if val == 0 {
            errors.push(ConfigError {
                field: name.into(),
                message: format!("{} must be > 0", name),
            });
        }
    }

    // commitlog_sync must be valid
    match config.commitlog_sync.as_str() {
        "periodic" | "batch" | "group" => {}
        other => {
            errors.push(ConfigError {
                field: "commitlog_sync".into(),
                message: format!(
                    "invalid commitlog_sync mode: '{}' (expected periodic|batch|group)",
                    other
                ),
            });
        }
    }

    // disk_failure_policy validation
    match config.disk_failure_policy.as_str() {
        "die" | "stop_paranoid" | "stop" | "best_effort" | "ignore" => {}
        other => {
            errors.push(ConfigError {
                field: "disk_failure_policy".into(),
                message: format!("invalid disk_failure_policy: '{}'", other),
            });
        }
    }

    // commitlog_segment_size cross-check (must be positive if set)
    if let Some(ref seg_size) = config.commitlog_segment_size {
        if seg_size.bytes() == 0 {
            errors.push(ConfigError {
                field: "commitlog_segment_size".into(),
                message: "commitlog_segment_size must be > 0".into(),
            });
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let cfg = CassandraConfig::default();
        let errors = validate(&cfg);
        assert!(errors.is_empty(), "default config has errors: {:?}", errors);
    }

    #[test]
    fn empty_cluster_name() {
        let yaml = r#"cluster_name: """#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "cluster_name"));
    }

    #[test]
    fn invalid_commitlog_sync() {
        let yaml = r#"commitlog_sync: "invalid""#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "commitlog_sync"));
    }

    #[test]
    fn zero_concurrent_reads() {
        let yaml = r#"concurrent_reads: 0"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "concurrent_reads"));
    }
}
