// Licensed under Apache License, Version 2.0.

//! Bootstraps system keyspaces into the schema catalog.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.SchemaManager` (bootstrap logic)

use parking_lot::Mutex;

use crate::catalog::SchemaCatalog;
use crate::schema_constants;
use crate::system_keyspaces::all_system_keyspaces;

/// Non-virtual system keyspace names (the 5 that `all_system_keyspaces()` provides).
const NON_VIRTUAL_SYSTEM_KEYSPACES: &[&str] = &[
    schema_constants::SYSTEM_KEYSPACE,
    schema_constants::SCHEMA_KEYSPACE,
    schema_constants::AUTH_KEYSPACE,
    schema_constants::TRACE_KEYSPACE,
    schema_constants::DISTRIBUTED_KEYSPACE,
];

/// Manages bootstrapping of system keyspaces into the schema catalog.
///
/// Tracks whether system keyspaces have been loaded so the operation
/// is performed at most once per node lifecycle.
pub struct SystemKeyspaceManager {
    bootstrapped: Mutex<bool>,
}

impl SystemKeyspaceManager {
    /// Create a new manager (not yet bootstrapped).
    pub fn new() -> Self {
        Self {
            bootstrapped: Mutex::new(false),
        }
    }

    /// Add all system keyspaces to the catalog and mark as bootstrapped.
    ///
    /// Returns a new `SchemaCatalog` containing the 5 non-virtual system
    /// keyspaces (system, system_schema, system_auth, system_traces,
    /// system_distributed).
    pub fn bootstrap(&self, catalog: SchemaCatalog) -> SchemaCatalog {
        let mut result = catalog;
        for ks in all_system_keyspaces() {
            result = result.with_keyspace(ks);
        }
        *self.bootstrapped.lock() = true;
        result
    }

    /// Returns `true` after `bootstrap` has been called.
    pub fn is_bootstrapped(&self) -> bool {
        *self.bootstrapped.lock()
    }

    /// The non-virtual system keyspace names.
    pub fn system_keyspace_names() -> &'static [&'static str] {
        NON_VIRTUAL_SYSTEM_KEYSPACES
    }

    /// Check whether a keyspace name is a system keyspace.
    pub fn is_system_keyspace(name: &str) -> bool {
        schema_constants::is_system_keyspace(name)
    }
}

impl Default for SystemKeyspaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_not_bootstrapped() {
        let mgr = SystemKeyspaceManager::new();
        assert!(!mgr.is_bootstrapped());
    }

    #[test]
    fn test_bootstrap_adds_all_system_keyspaces() {
        let mgr = SystemKeyspaceManager::new();
        let catalog = SchemaCatalog::new();
        assert_eq!(catalog.snapshot().keyspace_count(), 0);

        let catalog = mgr.bootstrap(catalog);
        assert_eq!(catalog.snapshot().keyspace_count(), 5);
        assert!(mgr.is_bootstrapped());

        // Verify each expected keyspace is present.
        let snap = catalog.snapshot();
        for name in NON_VIRTUAL_SYSTEM_KEYSPACES {
            assert!(snap.keyspace(name).is_some(), "missing keyspace: {name}");
        }
    }

    #[test]
    fn test_system_keyspace_names_returns_five() {
        let names = SystemKeyspaceManager::system_keyspace_names();
        assert_eq!(names.len(), 5);
        assert!(names.contains(&"system"));
        assert!(names.contains(&"system_schema"));
        assert!(names.contains(&"system_auth"));
        assert!(names.contains(&"system_traces"));
        assert!(names.contains(&"system_distributed"));
    }

    #[test]
    fn test_is_system_keyspace_delegates() {
        assert!(SystemKeyspaceManager::is_system_keyspace("system"));
        assert!(SystemKeyspaceManager::is_system_keyspace("system_auth"));
        assert!(SystemKeyspaceManager::is_system_keyspace("system_views")); // virtual
        assert!(!SystemKeyspaceManager::is_system_keyspace("my_keyspace"));
    }

    #[test]
    fn test_bootstrap_is_idempotent() {
        let mgr = SystemKeyspaceManager::new();
        let catalog = SchemaCatalog::new();
        let catalog = mgr.bootstrap(catalog);
        // Bootstrap again on top of the already-bootstrapped catalog.
        let catalog = mgr.bootstrap(catalog);
        // Still 5 keyspaces (with_keyspace replaces duplicates).
        assert_eq!(catalog.snapshot().keyspace_count(), 5);
    }

    #[test]
    fn test_default_trait() {
        let mgr = SystemKeyspaceManager::default();
        assert!(!mgr.is_bootstrapped());
    }
}
