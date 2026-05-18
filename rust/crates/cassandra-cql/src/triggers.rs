// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Trigger registry and execution framework.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.triggers.ITrigger`
//! - `org.apache.cassandra.triggers.TriggerExecutor`
//!
//! ## Design
//! Java Cassandra triggers implement ITrigger and are loaded via a custom
//! classloader. Our Rust implementation provides:
//! 1. A `Trigger` trait matching ITrigger's interface
//! 2. A registry for managing trigger instances per table
//! 3. Pre/post mutation hook points
//!
//! The feature is gated behind a compile-time flag.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata for a registered trigger.
#[derive(Debug, Clone)]
pub struct TriggerMetadata {
    pub name: String,
    pub keyspace: String,
    pub table: String,
    /// Java class name or Rust module path for the trigger implementation.
    pub trigger_class: String,
}

/// Represents a mutation that a trigger receives.
#[derive(Debug, Clone)]
pub struct MutationEvent {
    /// Keyspace of the mutated table.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Partition key values (serialized).
    pub partition_key: Vec<Vec<u8>>,
    /// The type of mutation.
    pub mutation_type: MutationType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationType {
    Insert,
    Update,
    Delete,
}

/// Additional mutations to apply as a result of the trigger.
#[derive(Debug, Clone)]
pub struct TriggerMutation {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<Vec<u8>>,
    pub mutations: Vec<(String, Vec<u8>)>, // (column, value)
}

/// Trait matching Java's ITrigger interface.
///
/// Triggers receive a partition mutation and can return additional
/// mutations to be atomically applied.
pub trait Trigger: Send + Sync {
    /// Called before the mutation is applied.
    ///
    /// Returns additional mutations to be included in the batch,
    /// or an empty vec for no side effects.
    fn augment(&self, event: &MutationEvent) -> Result<Vec<TriggerMutation>, String>;
}

/// Registry for triggers, organized by (keyspace, table).
pub struct TriggerRegistry {
    /// Key: "keyspace.table" → list of (metadata, trigger_impl)
    triggers: RwLock<HashMap<String, Vec<TriggerMetadata>>>,
    implementations: RwLock<HashMap<String, Arc<dyn Trigger>>>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            implementations: RwLock::new(HashMap::new()),
        }
    }

    fn make_key(keyspace: &str, table: &str) -> String {
        format!("{}.{}", keyspace, table)
    }

    /// Register a trigger for a table.
    pub fn register(&self, metadata: TriggerMetadata) -> Result<(), String> {
        let key = Self::make_key(&metadata.keyspace, &metadata.table);
        let mut triggers = self.triggers.write();
        let entry = triggers.entry(key).or_default();

        // Check for duplicate name.
        if entry.iter().any(|t| t.name == metadata.name) {
            return Err(format!(
                "trigger '{}' already exists on {}.{}",
                metadata.name, metadata.keyspace, metadata.table
            ));
        }
        entry.push(metadata);
        Ok(())
    }

    /// Register a trigger implementation by class/module name.
    pub fn register_implementation(
        &self,
        trigger_class: impl Into<String>,
        implementation: Arc<dyn Trigger>,
    ) {
        self.implementations
            .write()
            .insert(trigger_class.into(), implementation);
    }

    /// Register metadata and implementation together.
    pub fn register_with_implementation(
        &self,
        metadata: TriggerMetadata,
        implementation: Arc<dyn Trigger>,
    ) -> Result<(), String> {
        self.register_implementation(metadata.trigger_class.clone(), implementation);
        self.register(metadata)
    }

    /// Unregister a trigger by name from a table.
    pub fn unregister(&self, keyspace: &str, table: &str, name: &str) -> bool {
        let key = Self::make_key(keyspace, table);
        let mut triggers = self.triggers.write();
        if let Some(entry) = triggers.get_mut(&key) {
            let before = entry.len();
            entry.retain(|t| t.name != name);
            return entry.len() < before;
        }
        false
    }

    /// Get all triggers registered on a table.
    pub fn get_triggers(&self, keyspace: &str, table: &str) -> Vec<TriggerMetadata> {
        let key = Self::make_key(keyspace, table);
        self.triggers.read().get(&key).cloned().unwrap_or_default()
    }

    /// Check if any triggers are registered on a table.
    pub fn has_triggers(&self, keyspace: &str, table: &str) -> bool {
        let key = Self::make_key(keyspace, table);
        self.triggers
            .read()
            .get(&key)
            .is_some_and(|v| !v.is_empty())
    }

    /// Execute loaded trigger implementations for a mutation event.
    pub fn execute(&self, event: &MutationEvent) -> Result<Vec<TriggerMutation>, String> {
        let key = Self::make_key(&event.keyspace, &event.table);
        let triggers = self.triggers.read().get(&key).cloned().unwrap_or_default();
        let implementations = self.implementations.read();
        let mut mutations = Vec::new();
        for metadata in triggers {
            let Some(trigger) = implementations.get(&metadata.trigger_class) else {
                continue;
            };
            mutations.extend(trigger.augment(event)?);
        }
        Ok(mutations)
    }

    /// Total number of registered triggers across all tables.
    pub fn total_count(&self) -> usize {
        self.triggers.read().values().map(|v| v.len()).sum()
    }
}

impl Default for TriggerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get() {
        let registry = TriggerRegistry::new();
        registry
            .register(TriggerMetadata {
                name: "audit_log".into(),
                keyspace: "ks".into(),
                table: "users".into(),
                trigger_class: "org.example.AuditTrigger".into(),
            })
            .unwrap();

        let triggers = registry.get_triggers("ks", "users");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].name, "audit_log");
        assert!(registry.has_triggers("ks", "users"));
        assert!(!registry.has_triggers("ks", "other"));
    }

    #[test]
    fn duplicate_name_rejected() {
        let registry = TriggerRegistry::new();
        registry
            .register(TriggerMetadata {
                name: "t1".into(),
                keyspace: "ks".into(),
                table: "t".into(),
                trigger_class: "class1".into(),
            })
            .unwrap();

        let result = registry.register(TriggerMetadata {
            name: "t1".into(),
            keyspace: "ks".into(),
            table: "t".into(),
            trigger_class: "class2".into(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn unregister() {
        let registry = TriggerRegistry::new();
        registry
            .register(TriggerMetadata {
                name: "t1".into(),
                keyspace: "ks".into(),
                table: "t".into(),
                trigger_class: "class1".into(),
            })
            .unwrap();

        assert!(registry.unregister("ks", "t", "t1"));
        assert!(!registry.has_triggers("ks", "t"));
        assert!(!registry.unregister("ks", "t", "t1")); // Already removed.
    }

    #[test]
    fn total_count() {
        let registry = TriggerRegistry::new();
        for i in 0..3 {
            registry
                .register(TriggerMetadata {
                    name: format!("t{}", i),
                    keyspace: "ks".into(),
                    table: "users".into(),
                    trigger_class: format!("class{}", i),
                })
                .unwrap();
        }
        registry
            .register(TriggerMetadata {
                name: "t0".into(),
                keyspace: "ks".into(),
                table: "orders".into(),
                trigger_class: "class0".into(),
            })
            .unwrap();
        assert_eq!(registry.total_count(), 4);
    }

    #[derive(Debug)]
    struct AuditTrigger;

    impl Trigger for AuditTrigger {
        fn augment(&self, event: &MutationEvent) -> Result<Vec<TriggerMutation>, String> {
            Ok(vec![TriggerMutation {
                keyspace: event.keyspace.clone(),
                table: "audit".to_string(),
                partition_key: event.partition_key.clone(),
                mutations: vec![("event_type".to_string(), b"insert".to_vec())],
            }])
        }
    }

    #[test]
    fn execute_loaded_trigger() {
        let registry = TriggerRegistry::new();
        registry
            .register_with_implementation(
                TriggerMetadata {
                    name: "audit".to_string(),
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    trigger_class: "audit.Trigger".to_string(),
                },
                Arc::new(AuditTrigger),
            )
            .unwrap();

        let mutations = registry
            .execute(&MutationEvent {
                keyspace: "ks".to_string(),
                table: "users".to_string(),
                partition_key: vec![b"pk1".to_vec()],
                mutation_type: MutationType::Insert,
            })
            .unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table, "audit");
    }
}
