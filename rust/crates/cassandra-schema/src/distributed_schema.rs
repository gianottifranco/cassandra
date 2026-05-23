// Licensed under Apache License, Version 2.0.

//! Distributed schema combining catalog, change notifier, and version computation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.DistributedSchema`

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{SchemaCatalog, SchemaSnapshot};
use crate::index::IndexMetadata;
use crate::keyspace::KeyspaceMetadata;
use crate::schema_agreement::compute_schema_version;
use crate::schema_change::{SchemaChangeEvent, SchemaChangeNotifier};
use crate::table::TableMetadata;
use crate::trigger::TriggerDefinition;
use crate::user_function::{UserAggregate, UserFunction};
use crate::user_type::UserType;
use crate::view::ViewMetadata;

/// A schema mutation that can be coordinated through `DistributedSchema`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaMigration {
    CreateKeyspace(KeyspaceMetadata),
    AlterKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(TableMetadata),
    AlterTable(TableMetadata),
    DropTable {
        keyspace: String,
        table: String,
    },
    CreateView(ViewMetadata),
    DropView {
        keyspace: String,
        view: String,
    },
    CreateType(UserType),
    DropType {
        keyspace: String,
        type_name: String,
    },
    CreateFunction(UserFunction),
    DropFunction {
        keyspace: String,
        signature: String,
    },
    CreateAggregate(UserAggregate),
    DropAggregate {
        keyspace: String,
        signature: String,
    },
    CreateIndex {
        keyspace: String,
        table: String,
        index: IndexMetadata,
    },
    DropIndex {
        keyspace: String,
        table: String,
        index: String,
    },
    CreateTrigger {
        keyspace: String,
        table: String,
        trigger: TriggerDefinition,
    },
    DropTrigger {
        keyspace: String,
        table: String,
        trigger: String,
    },
}

/// Error returned when a schema migration cannot be applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaMigrationError {
    KeyspaceNotFound(String),
    TableNotFound {
        keyspace: String,
        table: String,
    },
    TypeNotFound {
        keyspace: String,
        type_name: String,
    },
    FunctionNotFound {
        keyspace: String,
        signature: String,
    },
    AggregateNotFound {
        keyspace: String,
        signature: String,
    },
    ViewNotFound {
        keyspace: String,
        view: String,
    },
    IndexNotFound {
        keyspace: String,
        table: String,
        index: String,
    },
    TriggerNotFound {
        keyspace: String,
        table: String,
        trigger: String,
    },
    VersionMismatch {
        source: String,
        expected: Uuid,
        actual: Uuid,
    },
}

/// Result of applying a coordinated schema migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMigrationResult {
    pub previous_version: Uuid,
    pub new_version: Uuid,
    pub catalog_version: u64,
    pub event: SchemaChangeEvent,
}

/// A schema migration received from, or prepared for, another node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteSchemaMigration {
    pub source: String,
    pub base_version: Uuid,
    pub migration: SchemaMigration,
}

impl RemoteSchemaMigration {
    pub fn new(source: impl Into<String>, base_version: Uuid, migration: SchemaMigration) -> Self {
        Self {
            source: source.into(),
            base_version,
            migration,
        }
    }
}

/// Result of applying a remote schema migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSchemaMigrationResult {
    pub source: String,
    pub base_version: Uuid,
    pub result: SchemaMigrationResult,
}

/// Decision for a remote schema migration before mutating local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteSchemaMigrationPlan {
    Apply,
    AlreadyApplied {
        source: String,
        local_version: Uuid,
        remote_base_version: Uuid,
    },
    PullRequired {
        source: String,
        local_version: Uuid,
        remote_base_version: Uuid,
    },
}

/// Outcome of reconciling a remote schema migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteSchemaMigrationOutcome {
    Applied(RemoteSchemaMigrationResult),
    AlreadyApplied {
        source: String,
        local_version: Uuid,
        remote_base_version: Uuid,
    },
    PullRequired {
        source: String,
        local_version: Uuid,
        remote_base_version: Uuid,
    },
}

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
        self.notifier
            .notify(&SchemaChangeEvent::KeyspaceDropped(name.to_string()));
    }

    /// Apply a schema migration atomically against the current catalog.
    ///
    /// A successful migration updates the catalog, recomputes the schema
    /// version UUID, and notifies listeners with the corresponding change
    /// event. Failed migrations leave the catalog and version untouched.
    pub fn apply_migration(
        &mut self,
        migration: SchemaMigration,
    ) -> Result<SchemaMigrationResult, SchemaMigrationError> {
        let previous_version = self.version;
        let (catalog, event) = self.catalog_after_migration(migration)?;
        self.catalog = catalog;
        self.recompute_version();
        self.notifier.notify(&event);

        Ok(SchemaMigrationResult {
            previous_version,
            new_version: self.version,
            catalog_version: self.catalog.version(),
            event,
        })
    }

    /// Build a serializable migration envelope for propagation to peers.
    pub fn prepare_remote_migration(
        &self,
        source: impl Into<String>,
        migration: SchemaMigration,
    ) -> RemoteSchemaMigration {
        RemoteSchemaMigration::new(source, self.version, migration)
    }

    /// Apply a schema migration received from another node.
    ///
    /// The migration is accepted only when its base schema version matches the
    /// receiver's current schema version. This preserves the same optimistic
    /// ordering rule Cassandra relies on before gossip/schema pull resolves
    /// divergent versions.
    pub fn apply_remote_migration(
        &mut self,
        remote: RemoteSchemaMigration,
    ) -> Result<RemoteSchemaMigrationResult, SchemaMigrationError> {
        if remote.base_version != self.version {
            return Err(SchemaMigrationError::VersionMismatch {
                source: remote.source,
                expected: self.version,
                actual: remote.base_version,
            });
        }

        let source = remote.source;
        let base_version = remote.base_version;
        let result = self.apply_migration(remote.migration)?;
        Ok(RemoteSchemaMigrationResult {
            source,
            base_version,
            result,
        })
    }

    /// Decide how a remote migration should be reconciled locally.
    pub fn plan_remote_migration(
        &self,
        remote: &RemoteSchemaMigration,
    ) -> RemoteSchemaMigrationPlan {
        if remote.base_version == self.version {
            return RemoteSchemaMigrationPlan::Apply;
        }

        if self.migration_effect_is_reflected(&remote.migration) {
            RemoteSchemaMigrationPlan::AlreadyApplied {
                source: remote.source.clone(),
                local_version: self.version,
                remote_base_version: remote.base_version,
            }
        } else {
            RemoteSchemaMigrationPlan::PullRequired {
                source: remote.source.clone(),
                local_version: self.version,
                remote_base_version: remote.base_version,
            }
        }
    }

    /// Reconcile a remote schema migration.
    ///
    /// Matching base versions are applied. Stale/divergent migrations that are
    /// already reflected locally are acknowledged without mutation. Other
    /// divergences return `PullRequired` so the caller can fetch a full schema
    /// snapshot or invoke a higher-level merge flow.
    pub fn reconcile_remote_migration(
        &mut self,
        remote: RemoteSchemaMigration,
    ) -> Result<RemoteSchemaMigrationOutcome, SchemaMigrationError> {
        match self.plan_remote_migration(&remote) {
            RemoteSchemaMigrationPlan::Apply => self
                .apply_remote_migration(remote)
                .map(RemoteSchemaMigrationOutcome::Applied),
            RemoteSchemaMigrationPlan::AlreadyApplied {
                source,
                local_version,
                remote_base_version,
            } => Ok(RemoteSchemaMigrationOutcome::AlreadyApplied {
                source,
                local_version,
                remote_base_version,
            }),
            RemoteSchemaMigrationPlan::PullRequired {
                source,
                local_version,
                remote_base_version,
            } => Ok(RemoteSchemaMigrationOutcome::PullRequired {
                source,
                local_version,
                remote_base_version,
            }),
        }
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

    fn migration_effect_is_reflected(&self, migration: &SchemaMigration) -> bool {
        match migration {
            SchemaMigration::CreateKeyspace(ks) | SchemaMigration::AlterKeyspace(ks) => {
                self.catalog.keyspace(&ks.name) == Some(ks)
            }
            SchemaMigration::DropKeyspace(name) => self.catalog.keyspace(name).is_none(),
            SchemaMigration::CreateTable(table) | SchemaMigration::AlterTable(table) => {
                self.catalog.table(&table.keyspace, &table.name) == Some(table)
            }
            SchemaMigration::DropTable { keyspace, table } => {
                self.catalog.table(keyspace, table).is_none()
            }
            SchemaMigration::CreateView(view) => {
                self.catalog.view(&view.keyspace, &view.name) == Some(view)
            }
            SchemaMigration::DropView { keyspace, view } => {
                self.catalog.view(keyspace, view).is_none()
            }
            SchemaMigration::CreateType(udt) => {
                self.catalog.user_type(&udt.keyspace, &udt.name) == Some(udt)
            }
            SchemaMigration::DropType {
                keyspace,
                type_name,
            } => self.catalog.user_type(keyspace, type_name).is_none(),
            SchemaMigration::CreateFunction(function) => {
                self.catalog
                    .function(&function.keyspace, &function.signature())
                    == Some(function)
            }
            SchemaMigration::DropFunction {
                keyspace,
                signature,
            } => self.catalog.function(keyspace, signature).is_none(),
            SchemaMigration::CreateAggregate(aggregate) => {
                self.catalog
                    .aggregate(&aggregate.keyspace, &aggregate.signature())
                    == Some(aggregate)
            }
            SchemaMigration::DropAggregate {
                keyspace,
                signature,
            } => self.catalog.aggregate(keyspace, signature).is_none(),
            SchemaMigration::CreateIndex {
                keyspace,
                table,
                index,
            } => self.catalog.index(keyspace, table, &index.name) == Some(index),
            SchemaMigration::DropIndex {
                keyspace,
                table,
                index,
            } => self.catalog.index(keyspace, table, index).is_none(),
            SchemaMigration::CreateTrigger {
                keyspace,
                table,
                trigger,
            } => {
                self.catalog
                    .table(keyspace, table)
                    .and_then(|table| table.trigger(&trigger.name))
                    == Some(trigger)
            }
            SchemaMigration::DropTrigger {
                keyspace,
                table,
                trigger,
            } => self
                .catalog
                .table(keyspace, table)
                .and_then(|table| table.trigger(trigger))
                .is_none(),
        }
    }

    fn catalog_after_migration(
        &self,
        migration: SchemaMigration,
    ) -> Result<(SchemaCatalog, SchemaChangeEvent), SchemaMigrationError> {
        match migration {
            SchemaMigration::CreateKeyspace(ks) => {
                let event = SchemaChangeEvent::KeyspaceCreated(ks.name.clone());
                Ok((self.catalog.with_keyspace(ks), event))
            }
            SchemaMigration::AlterKeyspace(ks) => {
                let event = SchemaChangeEvent::KeyspaceAltered(ks.name.clone());
                Ok((self.catalog.with_keyspace(ks), event))
            }
            SchemaMigration::DropKeyspace(name) => {
                self.require_keyspace(&name)?;
                let event = SchemaChangeEvent::KeyspaceDropped(name.clone());
                Ok((self.catalog.without_keyspace(&name), event))
            }
            SchemaMigration::CreateTable(table) => {
                let keyspace = table.keyspace.clone();
                let table_name = table.name.clone();
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_table(table))?;
                let event = SchemaChangeEvent::TableCreated {
                    keyspace,
                    table: table_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::AlterTable(table) => {
                let keyspace = table.keyspace.clone();
                let table_name = table.name.clone();
                self.require_table(&keyspace, &table_name)?;
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_table(table))?;
                let event = SchemaChangeEvent::TableAltered {
                    keyspace,
                    table: table_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropTable { keyspace, table } => {
                self.require_table(&keyspace, &table)?;
                let catalog = self.update_keyspace(&keyspace, |ks| ks.without_table(&table))?;
                let event = SchemaChangeEvent::TableDropped { keyspace, table };
                Ok((catalog, event))
            }
            SchemaMigration::CreateView(view) => {
                let keyspace = view.keyspace.clone();
                let view_name = view.name.clone();
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_view(view))?;
                let event = SchemaChangeEvent::ViewCreated {
                    keyspace,
                    view: view_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropView { keyspace, view } => {
                if self.catalog.view(&keyspace, &view).is_none() {
                    return Err(SchemaMigrationError::ViewNotFound { keyspace, view });
                }
                let catalog = self.update_keyspace(&keyspace, |ks| ks.without_view(&view))?;
                let event = SchemaChangeEvent::ViewDropped { keyspace, view };
                Ok((catalog, event))
            }
            SchemaMigration::CreateType(udt) => {
                let keyspace = udt.keyspace.clone();
                let type_name = udt.name.clone();
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_type(udt))?;
                let event = SchemaChangeEvent::TypeCreated {
                    keyspace,
                    type_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropType {
                keyspace,
                type_name,
            } => {
                if self.catalog.user_type(&keyspace, &type_name).is_none() {
                    return Err(SchemaMigrationError::TypeNotFound {
                        keyspace,
                        type_name,
                    });
                }
                let catalog = self.update_keyspace(&keyspace, |ks| ks.without_type(&type_name))?;
                let event = SchemaChangeEvent::TypeDropped {
                    keyspace,
                    type_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::CreateFunction(function) => {
                let keyspace = function.keyspace.clone();
                let signature = function.signature();
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_function(function))?;
                let event = SchemaChangeEvent::FunctionCreated {
                    keyspace,
                    function: signature,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropFunction {
                keyspace,
                signature,
            } => {
                if self.catalog.function(&keyspace, &signature).is_none() {
                    return Err(SchemaMigrationError::FunctionNotFound {
                        keyspace,
                        signature,
                    });
                }
                let catalog =
                    self.update_keyspace(&keyspace, |ks| ks.without_function(&signature))?;
                let event = SchemaChangeEvent::FunctionDropped {
                    keyspace,
                    function: signature,
                };
                Ok((catalog, event))
            }
            SchemaMigration::CreateAggregate(aggregate) => {
                let keyspace = aggregate.keyspace.clone();
                let signature = aggregate.signature();
                let catalog = self.update_keyspace(&keyspace, |ks| ks.with_aggregate(aggregate))?;
                let event = SchemaChangeEvent::AggregateCreated {
                    keyspace,
                    aggregate: signature,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropAggregate {
                keyspace,
                signature,
            } => {
                if self.catalog.aggregate(&keyspace, &signature).is_none() {
                    return Err(SchemaMigrationError::AggregateNotFound {
                        keyspace,
                        signature,
                    });
                }
                let catalog =
                    self.update_keyspace(&keyspace, |ks| ks.without_aggregate(&signature))?;
                let event = SchemaChangeEvent::AggregateDropped {
                    keyspace,
                    aggregate: signature,
                };
                Ok((catalog, event))
            }
            SchemaMigration::CreateIndex {
                keyspace,
                table,
                index,
            } => {
                self.require_table(&keyspace, &table)?;
                let index_name = index.name.clone();
                let catalog =
                    self.update_keyspace(&keyspace, |ks| ks.with_table_index(&table, index))?;
                let event = SchemaChangeEvent::IndexCreated {
                    keyspace,
                    table,
                    index: index_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropIndex {
                keyspace,
                table,
                index,
            } => {
                if self.catalog.index(&keyspace, &table, &index).is_none() {
                    return Err(SchemaMigrationError::IndexNotFound {
                        keyspace,
                        table,
                        index,
                    });
                }
                let catalog =
                    self.update_keyspace(&keyspace, |ks| ks.without_table_index(&table, &index))?;
                let event = SchemaChangeEvent::IndexDropped {
                    keyspace,
                    table,
                    index,
                };
                Ok((catalog, event))
            }
            SchemaMigration::CreateTrigger {
                keyspace,
                table,
                trigger,
            } => {
                let trigger_name = trigger.name.clone();
                let catalog =
                    self.update_table(&keyspace, &table, |table| table.with_trigger(trigger))?;
                let event = SchemaChangeEvent::TriggerCreated {
                    keyspace,
                    table,
                    trigger: trigger_name,
                };
                Ok((catalog, event))
            }
            SchemaMigration::DropTrigger {
                keyspace,
                table,
                trigger,
            } => {
                if self
                    .catalog
                    .table(&keyspace, &table)
                    .and_then(|table| table.trigger(&trigger))
                    .is_none()
                {
                    return Err(SchemaMigrationError::TriggerNotFound {
                        keyspace,
                        table,
                        trigger,
                    });
                }
                let catalog =
                    self.update_table(&keyspace, &table, |table| table.without_trigger(&trigger))?;
                let event = SchemaChangeEvent::TriggerDropped {
                    keyspace,
                    table,
                    trigger,
                };
                Ok((catalog, event))
            }
        }
    }

    fn update_keyspace<F>(
        &self,
        keyspace: &str,
        update: F,
    ) -> Result<SchemaCatalog, SchemaMigrationError>
    where
        F: FnOnce(KeyspaceMetadata) -> KeyspaceMetadata,
    {
        let ks = self
            .catalog
            .keyspace(keyspace)
            .cloned()
            .ok_or_else(|| SchemaMigrationError::KeyspaceNotFound(keyspace.to_string()))?;
        Ok(self.catalog.with_keyspace(update(ks)))
    }

    fn update_table<F>(
        &self,
        keyspace: &str,
        table: &str,
        update: F,
    ) -> Result<SchemaCatalog, SchemaMigrationError>
    where
        F: FnOnce(TableMetadata) -> TableMetadata,
    {
        let table_metadata = self
            .catalog
            .table(keyspace, table)
            .cloned()
            .ok_or_else(|| SchemaMigrationError::TableNotFound {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
            })?;
        self.update_keyspace(keyspace, |ks| ks.with_table(update(table_metadata)))
    }

    fn require_keyspace(&self, keyspace: &str) -> Result<(), SchemaMigrationError> {
        if self.catalog.keyspace(keyspace).is_some() {
            Ok(())
        } else {
            Err(SchemaMigrationError::KeyspaceNotFound(keyspace.to_string()))
        }
    }

    fn require_table(&self, keyspace: &str, table: &str) -> Result<(), SchemaMigrationError> {
        if self.catalog.table(keyspace, table).is_some() {
            Ok(())
        } else {
            Err(SchemaMigrationError::TableNotFound {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnMetadata;
    use crate::index::{IndexKind, IndexMetadata};
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::schema_change::SchemaChangeListener;
    use crate::table::TableMetadataBuilder;
    use cassandra_types::CqlType;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingListener {
        count: AtomicUsize,
    }

    impl SchemaChangeListener for CountingListener {
        fn on_change(&self, _event: &SchemaChangeEvent) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct RecordingListener {
        events: Mutex<Vec<SchemaChangeEvent>>,
    }

    impl SchemaChangeListener for RecordingListener {
        fn on_change(&self, event: &SchemaChangeEvent) {
            self.events.lock().unwrap().push(event.clone());
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

    #[test]
    fn apply_migration_coordinates_nested_schema_changes() {
        let notifier = SchemaChangeNotifier::new();
        let listener = Arc::new(RecordingListener {
            events: Mutex::new(Vec::new()),
        });
        notifier.register(listener.clone());
        let mut ds = DistributedSchema::with_notifier(SchemaCatalog::new(), notifier);
        let initial_version = ds.current_version();

        let keyspace = KeyspaceMetadata::new("ks", KeyspaceParams::default());
        let result = ds
            .apply_migration(SchemaMigration::CreateKeyspace(keyspace))
            .unwrap();
        assert_eq!(
            result.event,
            SchemaChangeEvent::KeyspaceCreated("ks".to_string())
        );
        assert_eq!(result.previous_version, initial_version);
        assert_ne!(result.new_version, initial_version);

        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .add_column(ColumnMetadata::regular("email", CqlType::Varchar))
            .build();
        let result = ds
            .apply_migration(SchemaMigration::CreateTable(table))
            .unwrap();
        assert_eq!(
            result.event,
            SchemaChangeEvent::TableCreated {
                keyspace: "ks".to_string(),
                table: "users".to_string(),
            }
        );
        assert!(ds.snapshot().table("ks", "users").is_some());

        let udt = UserType::new("ks", "address").with_field("street", "text");
        ds.apply_migration(SchemaMigration::CreateType(udt))
            .unwrap();
        assert!(ds.snapshot().user_type("ks", "address").is_some());

        let function = UserFunction::new("ks", "normalize", "text", "java", "return input;")
            .with_arg("input", "varchar");
        let function_signature = function.signature();
        ds.apply_migration(SchemaMigration::CreateFunction(function))
            .unwrap();
        assert!(ds.snapshot().function("ks", &function_signature).is_some());

        let aggregate =
            UserAggregate::new("ks", "concat_all", "text", "concat_state").with_arg_type("text");
        let aggregate_signature = aggregate.signature();
        ds.apply_migration(SchemaMigration::CreateAggregate(aggregate))
            .unwrap();
        assert!(
            ds.snapshot()
                .aggregate("ks", &aggregate_signature)
                .is_some()
        );

        let index = IndexMetadata::new(
            "email_idx_id".to_string(),
            "email_idx".to_string(),
            IndexKind::Keys,
            [("target".to_string(), "email".to_string())].into(),
        );
        ds.apply_migration(SchemaMigration::CreateIndex {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            index,
        })
        .unwrap();
        assert!(ds.snapshot().index("ks", "users", "email_idx").is_some());

        let trigger = TriggerDefinition::new("audit", "com.example.AuditTrigger");
        ds.apply_migration(SchemaMigration::CreateTrigger {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger,
        })
        .unwrap();
        assert!(
            ds.snapshot()
                .table("ks", "users")
                .unwrap()
                .trigger("audit")
                .is_some()
        );

        ds.apply_migration(SchemaMigration::DropIndex {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            index: "email_idx".to_string(),
        })
        .unwrap();
        assert!(ds.snapshot().index("ks", "users", "email_idx").is_none());

        let events = listener.events.lock().unwrap();
        assert_eq!(events.len(), 8);
        assert_eq!(
            events.last(),
            Some(&SchemaChangeEvent::IndexDropped {
                keyspace: "ks".to_string(),
                table: "users".to_string(),
                index: "email_idx".to_string(),
            })
        );
    }

    #[test]
    fn failed_migration_leaves_schema_unchanged() {
        let mut ds = DistributedSchema::new(SchemaCatalog::new());
        let before_version = ds.current_version();
        let before_catalog_version = ds.catalog().version();

        let error = ds
            .apply_migration(SchemaMigration::DropTable {
                keyspace: "missing".to_string(),
                table: "users".to_string(),
            })
            .unwrap_err();

        assert_eq!(
            error,
            SchemaMigrationError::TableNotFound {
                keyspace: "missing".to_string(),
                table: "users".to_string(),
            }
        );
        assert_eq!(ds.current_version(), before_version);
        assert_eq!(ds.catalog().version(), before_catalog_version);
        assert_eq!(ds.snapshot().keyspace_count(), 0);
    }

    #[test]
    fn remote_migration_round_trips_and_applies_from_matching_base_version() {
        let source = DistributedSchema::new(SchemaCatalog::new());
        let mut target = DistributedSchema::new(SchemaCatalog::new());
        let base_version = target.current_version();
        let remote = source.prepare_remote_migration(
            "node1",
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "ks_remote",
                KeyspaceParams::default(),
            )),
        );

        let payload = serde_json::to_vec(&remote).unwrap();
        let decoded: RemoteSchemaMigration = serde_json::from_slice(&payload).unwrap();
        assert_eq!(decoded.source, "node1");
        assert_eq!(decoded.base_version, base_version);

        let result = target.apply_remote_migration(decoded).unwrap();
        assert_eq!(result.source, "node1");
        assert_eq!(result.base_version, base_version);
        assert_eq!(
            result.result.event,
            SchemaChangeEvent::KeyspaceCreated("ks_remote".to_string())
        );
        assert!(target.snapshot().keyspace("ks_remote").is_some());
        assert_ne!(target.current_version(), base_version);
    }

    #[test]
    fn remote_migration_rejects_stale_base_version_without_mutating() {
        let mut target = DistributedSchema::new(SchemaCatalog::new());
        let stale_base_version = target.current_version();
        target
            .apply_migration(SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "existing",
                KeyspaceParams::default(),
            )))
            .unwrap();
        let current_version = target.current_version();
        let catalog_version = target.catalog().version();

        let error = target
            .apply_remote_migration(RemoteSchemaMigration::new(
                "node1",
                stale_base_version,
                SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                    "stale",
                    KeyspaceParams::default(),
                )),
            ))
            .unwrap_err();

        assert_eq!(
            error,
            SchemaMigrationError::VersionMismatch {
                source: "node1".to_string(),
                expected: current_version,
                actual: stale_base_version,
            }
        );
        assert_eq!(target.current_version(), current_version);
        assert_eq!(target.catalog().version(), catalog_version);
        assert!(target.snapshot().keyspace("existing").is_some());
        assert!(target.snapshot().keyspace("stale").is_none());
    }

    #[test]
    fn reconcile_remote_migration_applies_matching_base_version() {
        let mut target = DistributedSchema::new(SchemaCatalog::new());
        let base_version = target.current_version();
        let remote = RemoteSchemaMigration::new(
            "node1",
            base_version,
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "from_remote",
                KeyspaceParams::default(),
            )),
        );

        assert_eq!(
            target.plan_remote_migration(&remote),
            RemoteSchemaMigrationPlan::Apply
        );
        let outcome = target.reconcile_remote_migration(remote).unwrap();

        match outcome {
            RemoteSchemaMigrationOutcome::Applied(result) => {
                assert_eq!(result.source, "node1");
                assert_eq!(result.base_version, base_version);
            }
            other => panic!("expected applied outcome, got {:?}", other),
        }
        assert!(target.snapshot().keyspace("from_remote").is_some());
    }

    #[test]
    fn reconcile_remote_migration_acknowledges_already_reflected_divergence() {
        let mut target = DistributedSchema::new(SchemaCatalog::new());
        let stale_base_version = target.current_version();
        target
            .apply_migration(SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "already_here",
                KeyspaceParams::default(),
            )))
            .unwrap();
        let current_version = target.current_version();
        let catalog_version = target.catalog().version();
        let remote = RemoteSchemaMigration::new(
            "node1",
            stale_base_version,
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "already_here",
                KeyspaceParams::default(),
            )),
        );

        assert_eq!(
            target.plan_remote_migration(&remote),
            RemoteSchemaMigrationPlan::AlreadyApplied {
                source: "node1".to_string(),
                local_version: current_version,
                remote_base_version: stale_base_version,
            }
        );
        let outcome = target.reconcile_remote_migration(remote).unwrap();

        assert_eq!(
            outcome,
            RemoteSchemaMigrationOutcome::AlreadyApplied {
                source: "node1".to_string(),
                local_version: current_version,
                remote_base_version: stale_base_version,
            }
        );
        assert_eq!(target.current_version(), current_version);
        assert_eq!(target.catalog().version(), catalog_version);
    }

    #[test]
    fn reconcile_remote_migration_requests_pull_for_unresolved_divergence() {
        let mut target = DistributedSchema::new(SchemaCatalog::new());
        let stale_base_version = target.current_version();
        target
            .apply_migration(SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "local_only",
                KeyspaceParams::default(),
            )))
            .unwrap();
        let current_version = target.current_version();
        let catalog_version = target.catalog().version();
        let remote = RemoteSchemaMigration::new(
            "node1",
            stale_base_version,
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "remote_only",
                KeyspaceParams::default(),
            )),
        );

        assert_eq!(
            target.plan_remote_migration(&remote),
            RemoteSchemaMigrationPlan::PullRequired {
                source: "node1".to_string(),
                local_version: current_version,
                remote_base_version: stale_base_version,
            }
        );
        let outcome = target.reconcile_remote_migration(remote).unwrap();

        assert_eq!(
            outcome,
            RemoteSchemaMigrationOutcome::PullRequired {
                source: "node1".to_string(),
                local_version: current_version,
                remote_base_version: stale_base_version,
            }
        );
        assert_eq!(target.current_version(), current_version);
        assert_eq!(target.catalog().version(), catalog_version);
        assert!(target.snapshot().keyspace("remote_only").is_none());
    }
}
