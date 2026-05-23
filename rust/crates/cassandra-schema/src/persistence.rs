// Licensed under Apache License, Version 2.0.

//! JSON-based schema persistence.
//!
//! Saves and loads `SchemaSnapshot` to/from disk for recovery.
//! Feature-gated behind `persistence` for minimal builds.

use crate::catalog::SchemaSnapshot;
use crate::distributed_schema::RemoteSchemaMigration;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

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

/// Persisted status for a schema migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaMigrationJournalStatus {
    Applied,
    AlreadyApplied,
    PullRequired,
    Failed { error: String },
}

/// Append-only journal record for a schema migration coordination decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaMigrationJournalRecord {
    pub sequence: u64,
    pub local_version: Uuid,
    pub resulting_version: Option<Uuid>,
    pub migration: RemoteSchemaMigration,
    pub status: SchemaMigrationJournalStatus,
}

/// Append a schema migration journal record as one JSON line.
pub fn append_migration_record<P: AsRef<Path>>(
    path: P,
    record: &SchemaMigrationJournalRecord,
) -> Result<(), PersistenceError> {
    let json = serde_json::to_string(record).map_err(PersistenceError::Serde)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(PersistenceError::Io)?;
    writeln!(file, "{json}").map_err(PersistenceError::Io)
}

/// Load all schema migration journal records from a JSONL file.
pub fn load_migration_journal<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<SchemaMigrationJournalRecord>, PersistenceError> {
    let path = path.as_ref();
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(PersistenceError::Io(error)),
    };

    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(PersistenceError::Serde))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnMetadata;
    use crate::distributed_schema::{RemoteSchemaMigration, SchemaMigration};
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::table::TableMetadataBuilder;
    use cassandra_types::CqlType;
    use uuid::Uuid;

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

    #[test]
    fn migration_journal_append_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema_migrations.jsonl");
        let local_version = Uuid::new_v4();
        let resulting_version = Uuid::new_v4();
        let record = SchemaMigrationJournalRecord {
            sequence: 7,
            local_version,
            resulting_version: Some(resulting_version),
            migration: RemoteSchemaMigration::new(
                "node1",
                local_version,
                SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                    "ks",
                    KeyspaceParams::default(),
                )),
            ),
            status: SchemaMigrationJournalStatus::Applied,
        };

        append_migration_record(&path, &record).unwrap();
        let loaded = load_migration_journal(&path).unwrap();

        assert_eq!(loaded, vec![record]);
    }

    #[test]
    fn migration_journal_preserves_multiple_statuses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema_migrations.jsonl");
        let version = Uuid::new_v4();
        let applied = SchemaMigrationJournalRecord {
            sequence: 1,
            local_version: version,
            resulting_version: Some(Uuid::new_v4()),
            migration: RemoteSchemaMigration::new(
                "node1",
                version,
                SchemaMigration::DropKeyspace("old".to_string()),
            ),
            status: SchemaMigrationJournalStatus::AlreadyApplied,
        };
        let failed = SchemaMigrationJournalRecord {
            sequence: 2,
            local_version: version,
            resulting_version: None,
            migration: RemoteSchemaMigration::new(
                "node2",
                version,
                SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                    "conflict",
                    KeyspaceParams::default(),
                )),
            ),
            status: SchemaMigrationJournalStatus::Failed {
                error: "conflict".to_string(),
            },
        };

        append_migration_record(&path, &applied).unwrap();
        append_migration_record(&path, &failed).unwrap();

        assert_eq!(
            load_migration_journal(&path).unwrap(),
            vec![applied, failed]
        );
    }

    #[test]
    fn missing_migration_journal_loads_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.jsonl");

        assert!(load_migration_journal(&path).unwrap().is_empty());
    }
}
