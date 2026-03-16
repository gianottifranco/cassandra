// Licensed under Apache License, Version 2.0.

//! Keyspace, table, column, index, UDT, UDA, UDF metadata
//! and schema agreement protocol.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.*`

pub mod catalog;
pub mod column;
pub mod index;
pub mod keyspace;
pub mod persistence;
pub mod schema_agreement;
pub mod schema_constants;
pub mod system_keyspaces;
pub mod table;
pub mod table_id;

pub use catalog::{SchemaCatalog, SchemaSnapshot};
pub use column::{ClusteringOrder, ColumnKind, ColumnMetadata};
pub use index::{IndexKind, IndexMetadata};
pub use keyspace::{KeyspaceKind, KeyspaceMetadata, KeyspaceParams, ReplicationParams};
pub use system_keyspaces::{BootstrapState, SystemColumnSpec, SystemTableDef};
pub use table::{TableFlag, TableMetadata, TableMetadataBuilder, TableParams};
pub use table_id::TableId;
