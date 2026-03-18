// Licensed under Apache License, Version 2.0.

//! # Triggers (Stub)
//!
//! ## Status: DEFERRED — behind `triggers` feature flag
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.triggers.ITrigger`
//! `org.apache.cassandra.triggers.TriggerExecutor`
//!
//! ## Gap Documentation
//!
//! - **What's missing**: Trigger execution engine, trigger class loading,
//!   mutation interception on write path.
//! - **Why deferred**: Java triggers require loading arbitrary JVM classes.
//!   In a Rust implementation, this would need either JNI (maintaining a JVM
//!   dependency) or an alternative plugin system (WASM, dynamic libraries).
//! - **Closure path**: Implement a WASM-based trigger sandbox that loads
//!   trigger functions as WASM modules. Estimated effort: 3-4 weeks.
//!   Alternative: support Lua/Rhai scripting for simple triggers.
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! cassandra-storage = { path = ".", features = ["triggers"] }
//! ```

use serde::{Deserialize, Serialize};

/// Trigger definition metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDefinition {
    /// Trigger name.
    pub name: String,
    /// Keyspace.
    pub keyspace: String,
    /// Table the trigger is attached to.
    pub table: String,
    /// Trigger class/module identifier (in Java: fully qualified class name).
    pub trigger_class: String,
}

/// Trait for trigger implementations.
///
/// In Java, this is `ITrigger.augment(Partition)`.
/// Triggers receive the incoming mutation and can produce additional
/// mutations to be applied atomically.
pub trait Trigger: Send + Sync + std::fmt::Debug {
    /// Called before a mutation is applied. Returns additional mutations
    /// to apply, or an empty vec for no-op.
    fn augment(&self, mutation: &[u8]) -> Result<Vec<Vec<u8>>, String>;

    /// Get the trigger definition.
    fn definition(&self) -> &TriggerDefinition;
}

/// Trigger executor: invokes registered trigger implementations on mutations.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.triggers.TriggerExecutor` — singleton that loads
/// trigger classes and calls `ITrigger.augment()` for each registered trigger.
///
/// ## Status
///
/// Behind `#[cfg(feature = "triggers")]`. The executor holds `Box<dyn Trigger>`
/// instances. Until a plugin system (WASM/FFI) is implemented, no triggers
/// can actually be loaded at runtime.
#[cfg(feature = "triggers")]
#[derive(Debug, Default)]
pub struct TriggerExecutor {
    /// Loaded trigger implementations, keyed by (keyspace, table).
    triggers: std::collections::HashMap<(String, String), Vec<Box<dyn Trigger>>>,
}

#[cfg(feature = "triggers")]
impl TriggerExecutor {
    /// Create a new empty executor.
    pub fn new() -> Self {
        Self {
            triggers: std::collections::HashMap::new(),
        }
    }

    /// Register a trigger implementation for a table.
    pub fn register(&mut self, trigger: Box<dyn Trigger>) {
        let def = trigger.definition();
        let key = (def.keyspace.clone(), def.table.clone());
        self.triggers.entry(key).or_default().push(trigger);
    }

    /// Execute all triggers for a (keyspace, table) against the given mutation bytes.
    ///
    /// Returns a vec of augmented mutation byte blobs produced by the triggers.
    /// If a trigger returns an error, it is logged and skipped (non-fatal).
    pub fn execute(&self, keyspace: &str, table: &str, mutation: &[u8]) -> Vec<Vec<u8>> {
        let key = (keyspace.to_string(), table.to_string());
        let Some(triggers) = self.triggers.get(&key) else {
            return Vec::new();
        };

        let mut augmented = Vec::new();
        for trigger in triggers {
            match trigger.augment(mutation) {
                Ok(mutations) => augmented.extend(mutations),
                Err(_e) => {
                    // Non-fatal: log and continue (matches Java behavior where
                    // a failing trigger does not abort the write).
                }
            }
        }
        augmented
    }

    /// Check if any triggers are registered for the given table.
    pub fn has_triggers_for(&self, keyspace: &str, table: &str) -> bool {
        let key = (keyspace.to_string(), table.to_string());
        self.triggers.get(&key).is_some_and(|v| !v.is_empty())
    }
}

/// Stub trigger manager.
#[derive(Debug, Default)]
pub struct TriggerManager {
    _triggers: Vec<TriggerDefinition>,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self {
            _triggers: Vec::new(),
        }
    }

    /// Register a trigger. Stub — always returns error.
    pub fn register(&mut self, _def: TriggerDefinition) -> Result<(), String> {
        Err(
            "Triggers are not implemented. Feature is deferred (requires WASM/FFI plugin system)."
                .to_string(),
        )
    }

    /// Check if any triggers exist for a table.
    pub fn has_triggers_for(&self, _keyspace: &str, _table: &str) -> bool {
        false
    }

    /// Augment a mutation by iterating all triggers for the given table (WU-19).
    ///
    /// Returns a vec of augmented mutation byte blobs. Since no triggers can
    /// currently be registered (stub), this always returns an empty vec.
    ///
    /// ## Java Oracle
    ///
    /// `TriggerExecutor.execute()` — iterates triggers, calls `augment()`,
    /// collects additional mutations.
    #[cfg(feature = "triggers")]
    pub fn augment_mutation(
        &self,
        _keyspace: &str,
        _table: &str,
        _mutation: &[u8],
    ) -> Vec<Vec<u8>> {
        // No triggers can be registered via the stub manager, so this is a no-op.
        // When the plugin system is implemented, this will delegate to TriggerExecutor.
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_manager_stub() {
        let mut mgr = TriggerManager::new();
        let def = TriggerDefinition {
            name: "audit_trigger".to_string(),
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger_class: "com.example.AuditTrigger".to_string(),
        };
        assert!(mgr.register(def).is_err());
        assert!(!mgr.has_triggers_for("ks", "users"));
    }

    #[cfg(feature = "triggers")]
    #[test]
    fn trigger_executor_empty() {
        let executor = TriggerExecutor::new();
        assert!(!executor.has_triggers_for("ks", "users"));
        let result = executor.execute("ks", "users", b"mutation_bytes");
        assert!(result.is_empty());
    }

    #[cfg(feature = "triggers")]
    #[test]
    fn trigger_manager_augment_empty() {
        let mgr = TriggerManager::new();
        let result = mgr.augment_mutation("ks", "users", b"mutation_bytes");
        assert!(result.is_empty());
    }
}
