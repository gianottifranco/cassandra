// Licensed under Apache License, Version 2.0.

//! Keyspace, table, column, index, UDT, UDA, UDF metadata
//! and schema agreement protocol.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.*`

pub mod catalog;
pub mod column;
pub mod dropped_column;
pub mod index;
pub mod keyspace;
pub mod persistence;
pub mod schema_agreement;
pub mod schema_change;
pub mod schema_constants;
pub mod system_keyspaces;
pub mod system_local_data;
pub mod system_keyspace_manager;
pub mod table;
pub mod table_id;
pub mod trigger;
pub mod user_function;
pub mod user_type;
pub mod view;
pub mod distributed_schema;

pub use catalog::{SchemaCatalog, SchemaSnapshot};
pub use column::{ClusteringOrder, ColumnKind, ColumnMetadata};
pub use dropped_column::DroppedColumn;
pub use index::{IndexKind, IndexMetadata};
pub use keyspace::{KeyspaceKind, KeyspaceMetadata, KeyspaceParams, ReplicationParams};
pub use schema_change::{SchemaChangeEvent, SchemaChangeListener, SchemaChangeNotifier};
pub use system_keyspace_manager::SystemKeyspaceManager;
pub use system_keyspaces::{BootstrapState, SystemColumnSpec, SystemTableDef};
pub use system_local_data::{LocalNodeInfo, PeersEntry};
pub use table::{TableFlag, TableMetadata, TableMetadataBuilder, TableParams};
pub use table_id::TableId;
pub use trigger::TriggerDefinition;
pub use user_function::{UserAggregate, UserFunction};
pub use user_type::UserType;
pub use view::ViewMetadata;
