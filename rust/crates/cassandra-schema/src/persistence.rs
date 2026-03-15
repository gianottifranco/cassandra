// Licensed under Apache License, Version 2.0.

//! JSON-based schema persistence.
//!
//! Saves and loads `SchemaSnapshot` to/from disk for recovery.
//! Feature-gated behind `persistence` for minimal builds.

use std::path::Path;
use crate::catalog::SchemaSnapshot;

/// Error during schema persistence operations.
#[derive(Debug)]
pub enum PersistenceError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Serde(e) => write!(f, "serialization error: {}", e),
        }
    }
}
impl std::error::Error for PersistenceError {}

/// Save a schema snapshot to a JSON file.
pub fn save<P: AsRef<Path>>(path: P, snapshot: &SchemaSnapshot) -> Result<(), PersistenceError> {
    let json = serde_json::to_string_pretty(snapshot).map_err(PersistenceError::Serde)?;
    std::fs::write(path, json).map_err(PersistenceError::Io)
}

/// Load a schema snapshot from a JSON file.
pub fn load<P: AsRef<Path>>(path: P) -> Result<SchemaSnapshot, PersistenceError> {
    let content = std::fs::read_to_string(path).map_err(PersistenceError::Io)?;
    serde_json::from_str(&content).map_err(PersistenceError::Serde)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::table::TableMetadataBuilder;
    use crate::column::ColumnMetadata;
    use cassandra_types::CqlType;

    #[test]
    fn save_and_load_round_trip() {
        let table = TableMetadataBuilder::new("ks", "t1")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular("value", CqlType::Varchar))
            .build();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table);
        let mut snapshot = SchemaSnapshot::empty();
        snapshot.keyspaces.insert("ks".to_string(), ks);
        snapshot.version = 42;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        save(&path, &snapshot).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded, snapshot);
    }

    #[test]
    fn load_missing_file() {
        assert!(load("/nonexistent/schema.json").is_err());
    }
}
