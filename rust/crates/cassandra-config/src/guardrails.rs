// Licensed under Apache License, Version 2.0.

//! Guardrails: configuration-driven limits and controls.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.guardrails.Guardrails`
//! - `org.apache.cassandra.db.guardrails.GuardrailsConfig`
//!
//! Guardrails enforce operational limits on CQL operations to prevent
//! accidental damage (e.g., unbounded partition growth, too many columns).
//! Each guardrail can be in one of three modes: Disabled, Warn, or Fail.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The action a guardrail takes when triggered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GuardrailAction {
    /// No limit enforced.
    #[default]
    Disabled,
    /// Log a warning but allow the operation.
    Warn,
    /// Reject the operation with an error.
    Fail,
}

/// A threshold guardrail with optional warn and fail values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThresholdGuardrail {
    /// Threshold that triggers a warning. `None` = disabled.
    pub warn_threshold: Option<i64>,
    /// Threshold that triggers a failure. `None` = disabled.
    pub fail_threshold: Option<i64>,
}

impl ThresholdGuardrail {
    /// Check a value against this guardrail.
    pub fn check(&self, value: i64) -> GuardrailAction {
        if let Some(fail) = self.fail_threshold {
            if value >= fail {
                return GuardrailAction::Fail;
            }
        }
        if let Some(warn) = self.warn_threshold {
            if value >= warn {
                return GuardrailAction::Warn;
            }
        }
        GuardrailAction::Disabled
    }
}

/// Full set of guardrails configuration.
///
/// Matches the Java `GuardrailsConfig` / `Guardrails` classes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardrailsConfig {
    // ─── Table / Keyspace Limits ─────────────────────────────────────────
    /// Maximum number of tables per keyspace.
    pub tables_per_keyspace: ThresholdGuardrail,
    /// Maximum number of columns per table.
    pub columns_per_table: ThresholdGuardrail,
    /// Maximum number of secondary indexes per table.
    pub secondary_indexes_per_table: ThresholdGuardrail,
    /// Maximum number of materialized views per table.
    pub materialized_views_per_table: ThresholdGuardrail,
    /// Maximum number of fields in a UDT.
    pub fields_per_udt: ThresholdGuardrail,

    // ─── Collection Limits ───────────────────────────────────────────────
    /// Maximum number of items in a collection (list, set, map).
    pub collection_size: ThresholdGuardrail,
    /// Maximum size (in bytes) of a single collection element.
    pub item_size: ThresholdGuardrail,

    // ─── Partition Limits ────────────────────────────────────────────────
    /// Maximum partition size in bytes.
    pub partition_size: ThresholdGuardrail,
    /// Maximum number of rows per partition.
    pub partition_tombstones: ThresholdGuardrail,

    // ─── Query Limits ────────────────────────────────────────────────────
    /// Maximum page size in rows.
    pub page_size: ThresholdGuardrail,
    /// Maximum number of IN restrictions in a query.
    pub in_select_cartesian_product: ThresholdGuardrail,

    // ─── Feature Flags ───────────────────────────────────────────────────
    /// Whether ALLOW FILTERING is permitted.
    pub allow_filtering_enabled: bool,
    /// Whether compact storage is permitted for new tables.
    pub compact_tables_enabled: bool,
    /// Whether user-defined aggregates are permitted.
    pub user_aggregates_enabled: bool,
    /// Whether group-by queries are permitted.
    pub group_by_enabled: bool,
    /// Whether TRUNCATE is permitted.
    pub truncate_enabled: bool,
    /// Whether DROP KEYSPACE of non-empty keyspaces is permitted.
    pub drop_keyspace_enabled: bool,
    /// Whether uncompressed SSTables are permitted.
    pub uncompressed_tables_enabled: bool,

    // ─── Read/Write Consistency ──────────────────────────────────────────
    /// Minimum read consistency level allowed.
    pub minimum_replication_factor: ThresholdGuardrail,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            tables_per_keyspace: ThresholdGuardrail::default(),
            columns_per_table: ThresholdGuardrail::default(),
            secondary_indexes_per_table: ThresholdGuardrail::default(),
            materialized_views_per_table: ThresholdGuardrail::default(),
            fields_per_udt: ThresholdGuardrail::default(),
            collection_size: ThresholdGuardrail::default(),
            item_size: ThresholdGuardrail::default(),
            partition_size: ThresholdGuardrail::default(),
            partition_tombstones: ThresholdGuardrail::default(),
            page_size: ThresholdGuardrail::default(),
            in_select_cartesian_product: ThresholdGuardrail::default(),
            allow_filtering_enabled: true,
            compact_tables_enabled: false,
            user_aggregates_enabled: true,
            group_by_enabled: true,
            truncate_enabled: true,
            drop_keyspace_enabled: true,
            uncompressed_tables_enabled: true,
            minimum_replication_factor: ThresholdGuardrail::default(),
        }
    }
}

impl GuardrailsConfig {
    /// Check a guardrail by name for a given value.
    /// Returns the action to take.
    pub fn check_threshold(&self, name: &str, value: i64) -> GuardrailAction {
        match name {
            "tables_per_keyspace" => self.tables_per_keyspace.check(value),
            "columns_per_table" => self.columns_per_table.check(value),
            "secondary_indexes_per_table" => self.secondary_indexes_per_table.check(value),
            "materialized_views_per_table" => self.materialized_views_per_table.check(value),
            "fields_per_udt" => self.fields_per_udt.check(value),
            "collection_size" => self.collection_size.check(value),
            "item_size" => self.item_size.check(value),
            "partition_size" => self.partition_size.check(value),
            "partition_tombstones" => self.partition_tombstones.check(value),
            "page_size" => self.page_size.check(value),
            "in_select_cartesian_product" => self.in_select_cartesian_product.check(value),
            "minimum_replication_factor" => self.minimum_replication_factor.check(value),
            _ => GuardrailAction::Disabled,
        }
    }

    /// Check a feature-flag guardrail by name.
    pub fn check_feature(&self, name: &str) -> GuardrailAction {
        let enabled = match name {
            "allow_filtering" => self.allow_filtering_enabled,
            "compact_tables" => self.compact_tables_enabled,
            "user_aggregates" => self.user_aggregates_enabled,
            "group_by" => self.group_by_enabled,
            "truncate" => self.truncate_enabled,
            "drop_keyspace" => self.drop_keyspace_enabled,
            "uncompressed_tables" => self.uncompressed_tables_enabled,
            _ => return GuardrailAction::Disabled,
        };
        if enabled {
            GuardrailAction::Disabled
        } else {
            GuardrailAction::Fail
        }
    }
}

// ─── Guardrail Enforcement Framework ──────────────────────────────────────

/// A violation produced when a guardrail check fails.
#[derive(Debug, Clone)]
pub struct GuardrailViolation {
    pub guardrail_name: String,
    pub message: String,
    pub action: GuardrailAction,
}

impl fmt::Display for GuardrailViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.action {
            GuardrailAction::Warn => "WARNING",
            GuardrailAction::Fail => "REJECTED",
            GuardrailAction::Disabled => "OK",
        };
        write!(f, "[{}] {}: {}", level, self.guardrail_name, self.message)
    }
}

impl std::error::Error for GuardrailViolation {}

/// Trait for guardrail implementations.
pub trait Guardrail: Send + Sync {
    /// The name of this guardrail (for error messages and logging).
    fn name(&self) -> &str;

    /// Check the guardrail, returning a violation if triggered.
    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation>;
}

/// A guardrail that checks whether a feature is enabled.
pub struct EnableFlagGuardrail {
    pub feature_name: &'static str,
    pub display_name: &'static str,
}

impl Guardrail for EnableFlagGuardrail {
    fn name(&self) -> &str {
        self.display_name
    }

    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation> {
        match config.check_feature(self.feature_name) {
            GuardrailAction::Fail => Some(GuardrailViolation {
                guardrail_name: self.display_name.to_string(),
                message: format!("{} is not allowed", self.display_name),
                action: GuardrailAction::Fail,
            }),
            _ => None,
        }
    }
}

/// A guardrail that checks a numeric value against a threshold.
pub struct ValuesGuardrail<F: Fn() -> i64 + Send + Sync> {
    pub threshold_name: &'static str,
    pub value_fn: F,
}

impl<F: Fn() -> i64 + Send + Sync> Guardrail for ValuesGuardrail<F> {
    fn name(&self) -> &str {
        self.threshold_name
    }

    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation> {
        let value = (self.value_fn)();
        let action = config.check_threshold(self.threshold_name, value);
        match action {
            GuardrailAction::Disabled => None,
            _ => Some(GuardrailViolation {
                guardrail_name: self.threshold_name.to_string(),
                message: format!(
                    "{} = {} exceeds {} threshold",
                    self.threshold_name,
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
}

/// Password policy guardrail.
pub struct PasswordPolicyGuardrail {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_digit: bool,
}

impl Default for PasswordPolicyGuardrail {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_digit: true,
        }
    }
}

impl PasswordPolicyGuardrail {
    /// Check a password against the policy.
    pub fn check_password(&self, password: &str) -> Option<GuardrailViolation> {
        if password.len() < self.min_length {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: format!(
                    "password must be at least {} characters",
                    self.min_length
                ),
                action: GuardrailAction::Fail,
            });
        }
        if self.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: "password must contain at least one uppercase letter".to_string(),
                action: GuardrailAction::Fail,
            });
        }
        if self.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: "password must contain at least one digit".to_string(),
                action: GuardrailAction::Fail,
            });
        }
        None
    }
}

/// Registry for all active guardrails.
pub struct GuardrailRegistry {
    guardrails: Vec<Box<dyn Guardrail>>,
}

impl GuardrailRegistry {
    pub fn new() -> Self {
        Self {
            guardrails: Vec::new(),
        }
    }

    /// Register a guardrail.
    pub fn register(&mut self, guardrail: Box<dyn Guardrail>) {
        self.guardrails.push(guardrail);
    }

    /// Run all registered guardrails, returning any violations.
    pub fn check_all(&self, config: &GuardrailsConfig) -> Vec<GuardrailViolation> {
        self.guardrails
            .iter()
            .filter_map(|g| g.check(config))
            .collect()
    }

    /// Check all guardrails and return `Err` if any are `Fail`.
    pub fn enforce(&self, config: &GuardrailsConfig) -> Result<Vec<GuardrailViolation>, GuardrailViolation> {
        let violations = self.check_all(config);
        if let Some(fail) = violations.iter().find(|v| v.action == GuardrailAction::Fail) {
            return Err(fail.clone());
        }
        // Return warnings only
        Ok(violations)
    }
}

impl Default for GuardrailRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_guardrails_are_disabled() {
        let g = GuardrailsConfig::default();
        assert_eq!(g.tables_per_keyspace.check(9999), GuardrailAction::Disabled);
    }

    #[test]
    fn threshold_warn() {
        let t = ThresholdGuardrail {
            warn_threshold: Some(100),
            fail_threshold: Some(500),
        };
        assert_eq!(t.check(50), GuardrailAction::Disabled);
        assert_eq!(t.check(100), GuardrailAction::Warn);
        assert_eq!(t.check(250), GuardrailAction::Warn);
        assert_eq!(t.check(500), GuardrailAction::Fail);
        assert_eq!(t.check(1000), GuardrailAction::Fail);
    }

    #[test]
    fn threshold_only_warn() {
        let t = ThresholdGuardrail {
            warn_threshold: Some(10),
            fail_threshold: None,
        };
        assert_eq!(t.check(5), GuardrailAction::Disabled);
        assert_eq!(t.check(10), GuardrailAction::Warn);
        assert_eq!(t.check(9999), GuardrailAction::Warn);
    }

    #[test]
    fn threshold_only_fail() {
        let t = ThresholdGuardrail {
            warn_threshold: None,
            fail_threshold: Some(100),
        };
        assert_eq!(t.check(50), GuardrailAction::Disabled);
        assert_eq!(t.check(100), GuardrailAction::Fail);
    }

    #[test]
    fn check_by_name() {
        let mut g = GuardrailsConfig::default();
        g.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        assert_eq!(
            g.check_threshold("columns_per_table", 10),
            GuardrailAction::Disabled
        );
        assert_eq!(
            g.check_threshold("columns_per_table", 20),
            GuardrailAction::Warn
        );
        assert_eq!(
            g.check_threshold("columns_per_table", 100),
            GuardrailAction::Fail
        );
        assert_eq!(g.check_threshold("unknown", 100), GuardrailAction::Disabled);
    }

    #[test]
    fn feature_flags_defaults() {
        let g = GuardrailsConfig::default();
        assert!(g.allow_filtering_enabled);
        assert!(!g.compact_tables_enabled);
        assert!(g.truncate_enabled);
    }

    #[test]
    fn serialization_roundtrip() {
        let g = GuardrailsConfig::default();
        let json = serde_json::to_string(&g).unwrap();
        let g2: GuardrailsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(g2.allow_filtering_enabled, g.allow_filtering_enabled);
    }

    #[test]
    fn check_feature_disabled() {
        let mut g = GuardrailsConfig::default();
        g.truncate_enabled = false;
        assert_eq!(g.check_feature("truncate"), GuardrailAction::Fail);
        assert_eq!(g.check_feature("allow_filtering"), GuardrailAction::Disabled);
    }

    #[test]
    fn enable_flag_guardrail() {
        let guard = EnableFlagGuardrail {
            feature_name: "truncate",
            display_name: "TRUNCATE",
        };
        let mut config = GuardrailsConfig::default();
        assert!(guard.check(&config).is_none());

        config.truncate_enabled = false;
        let v = guard.check(&config).unwrap();
        assert_eq!(v.action, GuardrailAction::Fail);
    }

    #[test]
    fn values_guardrail() {
        let guard = ValuesGuardrail {
            threshold_name: "columns_per_table",
            value_fn: || 50,
        };
        let mut config = GuardrailsConfig::default();
        config.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        let v = guard.check(&config).unwrap();
        assert_eq!(v.action, GuardrailAction::Warn);
    }

    #[test]
    fn password_policy_basic() {
        let policy = PasswordPolicyGuardrail::default();
        assert!(policy.check_password("short").is_some());
        assert!(policy.check_password("alllowercase1").is_some());
        assert!(policy.check_password("NoDigitsHere").is_some());
        assert!(policy.check_password("Valid1Pass").is_none());
    }

    #[test]
    fn guardrail_registry() {
        let mut registry = GuardrailRegistry::new();
        registry.register(Box::new(EnableFlagGuardrail {
            feature_name: "truncate",
            display_name: "TRUNCATE",
        }));

        let config = GuardrailsConfig::default();
        let violations = registry.check_all(&config);
        assert!(violations.is_empty());

        let mut config2 = GuardrailsConfig::default();
        config2.truncate_enabled = false;
        let violations = registry.check_all(&config2);
        assert_eq!(violations.len(), 1);
        assert!(registry.enforce(&config2).is_err());
    }

    #[test]
    fn guardrail_violation_display() {
        let v = GuardrailViolation {
            guardrail_name: "test".to_string(),
            message: "exceeded".to_string(),
            action: GuardrailAction::Warn,
        };
        assert!(v.to_string().contains("WARNING"));
    }
}
