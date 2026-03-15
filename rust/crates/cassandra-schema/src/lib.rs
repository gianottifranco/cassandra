// Licensed under Apache License, Version 2.0.

//! Keyspace, table, column, index, UDT, UDA, UDF metadata
//! and schema agreement protocol.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.*`

pub mod schema_constants;
pub mod table_id;
pub mod column;
pub mod table;
pub mod keyspace;
pub mod catalog;
pub mod persistence;

pub use table_id::TableId;
pub use column::{ColumnMetadata, ColumnKind, ClusteringOrder};
pub use table::{TableMetadata, TableMetadataBuilder, TableParams, TableFlag};
pub use keyspace::{KeyspaceMetadata, KeyspaceParams, ReplicationParams, KeyspaceKind};
pub use catalog::{SchemaCatalog, SchemaSnapshot};
