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
}
