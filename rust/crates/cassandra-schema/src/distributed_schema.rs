// Licensed under Apache License, Version 2.0.

//! Distributed schema combining catalog, change notifier, and version computation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.DistributedSchema`

use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::{SchemaCatalog, SchemaSnapshot};
use crate::keyspace::KeyspaceMetadata;
use crate::schema_agreement::compute_schema_version;
use crate::schema_change::{SchemaChangeEvent, SchemaChangeNotifier};

/// Distributed schema combining catalog state, change notification, and versioning.
pub struct DistributedSchema {
    catalog: SchemaCatalog,
    notifier: SchemaChangeNotifier,
    version: Uuid,
}

impl DistributedSchema {
    /// Create a new distributed schema from a catalog.
    pub fn new(catalog: SchemaCatalog) -> Self {
        let version = compute_schema_version(&catalog.snapshot());
        Self {
            catalog,
            notifier: SchemaChangeNotifier::new(),
            version,
        }
    }

    /// Create with an existing notifier.
    pub fn with_notifier(catalog: SchemaCatalog, notifier: SchemaChangeNotifier) -> Self {
        let version = compute_schema_version(&catalog.snapshot());
        Self {
            catalog,
            notifier,
            version,
        }
    }

    /// Apply a keyspace mutation (add/update), recompute version, and notify listeners.
    pub fn apply_keyspace(&mut self, ks: KeyspaceMetadata, event: SchemaChangeEvent) {
        self.catalog = self.catalog.with_keyspace(ks);
        self.recompute_version();
        self.notifier.notify(&event);
    }

    /// Remove a keyspace, recompute version, and notify listeners.
    pub fn remove_keyspace(&mut self, name: &str) {
        self.catalog = self.catalog.without_keyspace(name);
        self.recompute_version();
        self.notifier.notify(&SchemaChangeEvent::KeyspaceDropped(name.to_string()));
    }

    /// Current schema version UUID.
    pub fn current_version(&self) -> Uuid {
        self.version
    }

    /// Get a snapshot of the current schema.
    pub fn snapshot(&self) -> Arc<SchemaSnapshot> {
        self.catalog.snapshot()
    }

    /// Get a reference to the underlying catalog.
    pub fn catalog(&self) -> &SchemaCatalog {
        &self.catalog
    }

    /// Get a reference to the notifier for registering listeners.
    pub fn notifier(&self) -> &SchemaChangeNotifier {
        &self.notifier
    }

    /// Recompute the schema version from current state.
    fn recompute_version(&mut self) {
        self.version = compute_schema_version(&self.catalog.snapshot());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::schema_change::SchemaChangeListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingListener {
        count: AtomicUsize,
    }

    impl SchemaChangeListener for CountingListener {
        fn on_change(&self, _event: &SchemaChangeEvent) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn version_changes_on_mutation() {
        let catalog = SchemaCatalog::new();
        let mut ds = DistributedSchema::new(catalog);
        let v1 = ds.current_version();

        ds.apply_keyspace(
            KeyspaceMetadata::new("ks1", KeyspaceParams::default()),
            SchemaChangeEvent::KeyspaceCreated("ks1".into()),
        );
        let v2 = ds.current_version();
        assert_ne!(v1, v2);

        ds.remove_keyspace("ks1");
        let v3 = ds.current_version();
        assert_ne!(v2, v3);
        // After removing, should match original empty schema
        assert_eq!(v1, v3);
    }

    #[test]
    fn notifies_listeners() {
        let catalog = SchemaCatalog::new();
        let notifier = SchemaChangeNotifier::new();
        let listener = Arc::new(CountingListener {
            count: AtomicUsize::new(0),
        });
        notifier.register(listener.clone());

        let mut ds = DistributedSchema::with_notifier(catalog, notifier);
        ds.apply_keyspace(
            KeyspaceMetadata::new("ks1", KeyspaceParams::default()),
            SchemaChangeEvent::KeyspaceCreated("ks1".into()),
        );
        assert_eq!(listener.count.load(Ordering::Relaxed), 1);

        ds.remove_keyspace("ks1");
        assert_eq!(listener.count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn snapshot_reflects_mutations() {
        let catalog = SchemaCatalog::new();
        let mut ds = DistributedSchema::new(catalog);

        assert_eq!(ds.snapshot().keyspace_count(), 0);

        ds.apply_keyspace(
            KeyspaceMetadata::new("ks1", KeyspaceParams::default()),
            SchemaChangeEvent::KeyspaceCreated("ks1".into()),
        );
        assert_eq!(ds.snapshot().keyspace_count(), 1);
        assert!(ds.snapshot().keyspace("ks1").is_some());
    }
}
