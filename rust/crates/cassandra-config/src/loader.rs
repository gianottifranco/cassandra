// Licensed under Apache License, Version 2.0.

//! Configuration file loader.
//!
//! Loads `cassandra.yaml`, applies environment overrides, and validates.

use crate::config::CassandraConfig;
use crate::properties;
use crate::validation::{self, ConfigError};
use std::path::Path;

/// Error loading configuration.
#[derive(Debug)]
pub enum LoadError {
    Io(std::io::Error),
    Parse(serde_yaml::Error),
    Validation(Vec<ConfigError>),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Parse(e) => write!(f, "YAML parse error: {}", e),
            Self::Validation(errs) => {
                write!(f, "{} validation error(s):", errs.len())?;
                for e in errs {
                    write!(f, "\n  - {}", e)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for LoadError {}

/// Load and validate a `CassandraConfig` from a YAML file.
///
/// Returns the validated configuration or the first set of errors encountered.
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<CassandraConfig, LoadError> {
    let content = std::fs::read_to_string(path.as_ref()).map_err(LoadError::Io)?;
    let mut config: CassandraConfig = serde_yaml::from_str(&content).map_err(LoadError::Parse)?;
    properties::apply_overrides(&mut config);
    let errors = validation::validate(&config);
    if errors.is_empty() {
        Ok(config)
    } else {
        Err(LoadError::Validation(errors))
    }
}

/// Load configuration from a YAML string (useful for testing).
pub fn load_config_from_str(yaml: &str) -> Result<CassandraConfig, LoadError> {
    let config: CassandraConfig = serde_yaml::from_str(yaml).map_err(LoadError::Parse)?;
    let errors = validation::validate(&config);
    if errors.is_empty() {
        Ok(config)
    } else {
        Err(LoadError::Validation(errors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_from_str() {
        let yaml = r#"
cluster_name: TestCluster
num_tokens: 256
native_transport_port: 9042
"#;
        let cfg = load_config_from_str(yaml).unwrap();
        assert_eq!(cfg.cluster_name, "TestCluster");
        assert_eq!(cfg.num_tokens, 256);
    }

    #[test]
    fn load_invalid_rejects() {
        let yaml = r#"cluster_name: """#;
        assert!(load_config_from_str(yaml).is_err());
    }

    #[test]
    fn load_missing_file() {
        assert!(load_config("/nonexistent/cassandra.yaml").is_err());
    }
}
