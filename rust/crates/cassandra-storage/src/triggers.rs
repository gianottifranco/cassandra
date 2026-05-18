// Licensed under Apache License, Version 2.0.

//! # Triggers
//!
//! ## Status: feature-gated plugin registry
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.triggers.ITrigger`
//! `org.apache.cassandra.triggers.TriggerExecutor`
//!
//! The Rust implementation stores trigger metadata and executes registered
//! trigger plugins through a pluggable runtime registry. Loading arbitrary
//! Java trigger classes is intentionally modeled as external plugin loading
//! rather than JVM embedding.
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! cassandra-storage = { path = ".", features = ["triggers"] }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
/// Behind `#[cfg(feature = "triggers")]`. The executor holds concrete
/// `Box<dyn Trigger>` implementations supplied by the embedding runtime or
/// tests.
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

pub trait TriggerPlugin: Send + Sync + std::fmt::Debug {
    fn augment(
        &self,
        definition: &TriggerDefinition,
        mutation: &[u8],
    ) -> Result<Vec<Vec<u8>>, String>;
}

#[derive(Debug, Clone)]
pub struct StaticTriggerPlugin {
    outputs: Vec<Vec<u8>>,
}

impl StaticTriggerPlugin {
    pub fn new(outputs: Vec<Vec<u8>>) -> Self {
        Self { outputs }
    }
}

impl TriggerPlugin for StaticTriggerPlugin {
    fn augment(
        &self,
        _definition: &TriggerDefinition,
        _mutation: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        Ok(self.outputs.clone())
    }
}

#[derive(Debug, Default)]
pub struct TriggerPluginRegistry {
    plugins: HashMap<String, Arc<dyn TriggerPlugin>>,
}

impl TriggerPluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_plugin(
        &mut self,
        trigger_class: impl Into<String>,
        plugin: Arc<dyn TriggerPlugin>,
    ) {
        self.plugins.insert(trigger_class.into(), plugin);
    }

    pub fn get(&self, trigger_class: &str) -> Option<Arc<dyn TriggerPlugin>> {
        self.plugins.get(trigger_class).cloned()
    }

    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }
}

/// Trigger manager backed by a pluggable runtime registry.
#[derive(Debug, Default)]
pub struct TriggerManager {
    triggers: Vec<TriggerDefinition>,
    registry: TriggerPluginRegistry,
    disabled: HashSet<(String, String, String)>,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self {
            triggers: Vec::new(),
            registry: TriggerPluginRegistry::new(),
            disabled: HashSet::new(),
        }
    }

    pub fn with_registry(registry: TriggerPluginRegistry) -> Self {
        Self {
            triggers: Vec::new(),
            registry,
            disabled: HashSet::new(),
        }
    }

    pub fn registry_mut(&mut self) -> &mut TriggerPluginRegistry {
        &mut self.registry
    }

    /// Register a trigger definition after verifying its plugin is available.
    pub fn register(&mut self, def: TriggerDefinition) -> Result<(), String> {
        if self.registry.get(&def.trigger_class).is_none() {
            return Err(format!(
                "Trigger plugin '{}' is not loaded",
                def.trigger_class
            ));
        }
        if self.triggers.iter().any(|existing| {
            existing.keyspace == def.keyspace
                && existing.table == def.table
                && existing.name == def.name
        }) {
            return Err(format!("Trigger '{}' already exists", def.name));
        }
        self.triggers.push(def);
        Ok(())
    }

    /// Disable a registered trigger by name.
    pub fn disable(&mut self, keyspace: &str, table: &str, name: &str) -> bool {
        if !self
            .triggers
            .iter()
            .any(|def| def.keyspace == keyspace && def.table == table && def.name == name)
        {
            return false;
        }
        self.disabled
            .insert((keyspace.to_string(), table.to_string(), name.to_string()));
        true
    }

    /// Re-enable a disabled trigger by name.
    pub fn enable(&mut self, keyspace: &str, table: &str, name: &str) -> bool {
        self.disabled
            .remove(&(keyspace.to_string(), table.to_string(), name.to_string()))
    }

    pub fn is_enabled(&self, keyspace: &str, table: &str, name: &str) -> bool {
        self.triggers
            .iter()
            .any(|def| def.keyspace == keyspace && def.table == table && def.name == name)
            && !self
                .disabled
                .contains(&(keyspace.to_string(), table.to_string(), name.to_string()))
    }

    pub fn has_enabled_triggers_for(&self, keyspace: &str, table: &str) -> bool {
        self.triggers.iter().any(|def| {
            def.keyspace == keyspace
                && def.table == table
                && self.is_enabled(&def.keyspace, &def.table, &def.name)
        })
    }

    /// Check if any triggers exist for a table.
    pub fn has_triggers_for(&self, keyspace: &str, table: &str) -> bool {
        self.triggers
            .iter()
            .any(|def| def.keyspace == keyspace && def.table == table)
    }

    /// Augment a mutation by iterating all triggers for the given table (WU-19).
    ///
    /// Returns a vec of augmented mutation byte blobs produced by loaded
    /// plugins. Missing plugins are skipped because registration validates
    /// plugin availability.
    ///
    /// ## Java Oracle
    ///
    /// `TriggerExecutor.execute()` — iterates triggers, calls `augment()`,
    /// collects additional mutations.
    pub fn augment_mutation(&self, keyspace: &str, table: &str, mutation: &[u8]) -> Vec<Vec<u8>> {
        let mut augmented = Vec::new();
        for def in self
            .triggers
            .iter()
            .filter(|def| def.keyspace == keyspace && def.table == table)
        {
            if !self.is_enabled(&def.keyspace, &def.table, &def.name) {
                continue;
            }
            if let Some(plugin) = self.registry.get(&def.trigger_class) {
                if let Ok(mut mutations) = plugin.augment(def, mutation) {
                    augmented.append(&mut mutations);
                }
            }
        }
        augmented
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_manager_requires_loaded_plugin() {
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

    #[test]
    fn trigger_manager_executes_registered_plugin() {
        let mut registry = TriggerPluginRegistry::new();
        registry.register_plugin(
            "wasm://audit",
            Arc::new(StaticTriggerPlugin::new(vec![b"augmented".to_vec()])),
        );
        let mut mgr = TriggerManager::with_registry(registry);
        mgr.register(TriggerDefinition {
            name: "audit_trigger".to_string(),
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger_class: "wasm://audit".to_string(),
        })
        .unwrap();

        assert!(mgr.has_triggers_for("ks", "users"));
        assert_eq!(
            mgr.augment_mutation("ks", "users", b"mutation"),
            vec![b"augmented".to_vec()]
        );
    }

    #[test]
    fn trigger_manager_can_disable_and_enable_triggers() {
        let mut registry = TriggerPluginRegistry::new();
        registry.register_plugin(
            "wasm://audit",
            Arc::new(StaticTriggerPlugin::new(vec![b"augmented".to_vec()])),
        );
        let mut mgr = TriggerManager::with_registry(registry);
        mgr.register(TriggerDefinition {
            name: "audit_trigger".to_string(),
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger_class: "wasm://audit".to_string(),
        })
        .unwrap();

        assert!(mgr.is_enabled("ks", "users", "audit_trigger"));
        assert!(mgr.has_enabled_triggers_for("ks", "users"));
        assert!(mgr.disable("ks", "users", "audit_trigger"));
        assert!(!mgr.is_enabled("ks", "users", "audit_trigger"));
        assert!(!mgr.has_enabled_triggers_for("ks", "users"));
        assert!(mgr.augment_mutation("ks", "users", b"mutation").is_empty());
        assert!(mgr.enable("ks", "users", "audit_trigger"));
        assert_eq!(
            mgr.augment_mutation("ks", "users", b"mutation"),
            vec![b"augmented".to_vec()]
        );
        assert!(!mgr.disable("ks", "users", "missing"));
    }

    #[cfg(feature = "triggers")]
    #[test]
    fn trigger_executor_empty() {
        let executor = TriggerExecutor::new();
        assert!(!executor.has_triggers_for("ks", "users"));
        let result = executor.execute("ks", "users", b"mutation_bytes");
        assert!(result.is_empty());
    }

    #[test]
    fn trigger_manager_augment_empty() {
        let mgr = TriggerManager::new();
        let result = mgr.augment_mutation("ks", "users", b"mutation_bytes");
        assert!(result.is_empty());
    }
}
