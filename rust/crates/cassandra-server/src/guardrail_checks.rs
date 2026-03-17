// Licensed under Apache License, Version 2.0.

//! Guardrails enforcement at the CQL query execution layer.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.guardrails.Guardrails`
//! - `org.apache.cassandra.cql3.statements.CreateTableStatement` (guardrail hooks)
//!
//! Wires guardrail checks into query execution paths:
//! - CREATE TABLE → tables/columns limits
//! - CREATE INDEX → index limits
//! - SELECT ALLOW FILTERING → feature flag
//! - TRUNCATE → feature flag
//! - Page size → threshold check

use cassandra_config::{GuardrailAction, GuardrailViolation, GuardrailsConfig};
use tracing::warn;

/// Result of a guardrail check.
#[derive(Debug)]
pub enum GuardrailResult {
    /// Operation is allowed.
    Allowed,
    /// Operation is allowed but generated a warning.
    Warned(String),
    /// Operation is rejected.
    Rejected(String),
}

impl GuardrailResult {
    pub fn is_rejected(&self) -> bool {
        matches!(self, GuardrailResult::Rejected(_))
    }
}

/// Check guardrails for CREATE TABLE.
pub fn check_create_table(
    config: &GuardrailsConfig,
    tables_in_keyspace: i64,
    column_count: i64,
) -> GuardrailResult {
    // Check tables per keyspace
    if let Some(v) = check_threshold(config, "tables_per_keyspace", tables_in_keyspace + 1) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
    }

    // Check columns per table
    if let Some(v) = check_threshold(config, "columns_per_table", column_count) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }

    GuardrailResult::Allowed
}

/// Check guardrails for CREATE INDEX.
pub fn check_create_index(
    config: &GuardrailsConfig,
    indexes_on_table: i64,
) -> GuardrailResult {
    if let Some(v) = check_threshold(config, "secondary_indexes_per_table", indexes_on_table + 1) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }
    GuardrailResult::Allowed
}

/// Check guardrails for ALLOW FILTERING.
pub fn check_allow_filtering(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "allow_filtering", "ALLOW FILTERING")
}

/// Check guardrails for TRUNCATE.
pub fn check_truncate(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "truncate", "TRUNCATE")
}

/// Check page size guardrail.
pub fn check_page_size(config: &GuardrailsConfig, page_size: i64) -> GuardrailResult {
    if let Some(v) = check_threshold(config, "page_size", page_size) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }
    GuardrailResult::Allowed
}

/// Check DROP KEYSPACE guardrail.
pub fn check_drop_keyspace(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "drop_keyspace", "DROP KEYSPACE")
}

fn check_threshold(
    config: &GuardrailsConfig,
    name: &str,
    value: i64,
) -> Option<GuardrailViolation> {
    let action = config.check_threshold(name, value);
    match action {
        GuardrailAction::Disabled => None,
        _ => Some(GuardrailViolation {
            guardrail_name: name.to_string(),
            message: format!(
                "{} = {} exceeds {} threshold",
                name,
                value,
                if action == GuardrailAction::Warn {
                    "warn"
                } else {
                    "fail"
                }
            ),
            action,
        }),
    }
}

fn check_feature(config: &GuardrailsConfig, feature: &str, display: &str) -> GuardrailResult {
    match config.check_feature(feature) {
        GuardrailAction::Fail => {
            GuardrailResult::Rejected(format!("{display} is not allowed by guardrails"))
        }
        _ => GuardrailResult::Allowed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_config::ThresholdGuardrail;

    #[test]
    fn create_table_allowed() {
        let config = GuardrailsConfig::default();
        let result = check_create_table(&config, 5, 10);
        assert!(!result.is_rejected());
    }

    #[test]
    fn create_table_rejected_too_many_tables() {
        let mut config = GuardrailsConfig::default();
        config.tables_per_keyspace = ThresholdGuardrail {
            warn_threshold: Some(5),
            fail_threshold: Some(10),
        };
        let result = check_create_table(&config, 10, 5);
        assert!(result.is_rejected());
    }

    #[test]
    fn create_table_warned_columns() {
        let mut config = GuardrailsConfig::default();
        config.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        let result = check_create_table(&config, 1, 25);
        assert!(matches!(result, GuardrailResult::Warned(_)));
    }

    #[test]
    fn create_index_rejected() {
        let mut config = GuardrailsConfig::default();
        config.secondary_indexes_per_table = ThresholdGuardrail {
            warn_threshold: Some(3),
            fail_threshold: Some(5),
        };
        let result = check_create_index(&config, 5);
        assert!(result.is_rejected());
    }

    #[test]
    fn allow_filtering_disabled() {
        let mut config = GuardrailsConfig::default();
        config.allow_filtering_enabled = false;
        let result = check_allow_filtering(&config);
        assert!(result.is_rejected());
    }

    #[test]
    fn truncate_disabled() {
        let mut config = GuardrailsConfig::default();
        config.truncate_enabled = false;
        let result = check_truncate(&config);
        assert!(result.is_rejected());
    }

    #[test]
    fn page_size_check() {
        let mut config = GuardrailsConfig::default();
        config.page_size = ThresholdGuardrail {
            warn_threshold: Some(1000),
            fail_threshold: Some(5000),
        };
        assert!(!check_page_size(&config, 500).is_rejected());
        assert!(matches!(
            check_page_size(&config, 1000),
            GuardrailResult::Warned(_)
        ));
        assert!(check_page_size(&config, 5000).is_rejected());
    }

    #[test]
    fn drop_keyspace_allowed_by_default() {
        let config = GuardrailsConfig::default();
        assert!(!check_drop_keyspace(&config).is_rejected());
    }

    #[test]
    fn drop_keyspace_disabled() {
        let mut config = GuardrailsConfig::default();
        config.drop_keyspace_enabled = false;
        assert!(check_drop_keyspace(&config).is_rejected());
    }
}
