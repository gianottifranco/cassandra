// Licensed under Apache License, Version 2.0.

//! Schema agreement and version computation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.Schema`
//! - `org.apache.cassandra.schema.SchemaVersionVerbHandler`
//!
//! Schema versioning uses an MD5 digest over the sorted keyspace/table
//! definitions to produce a deterministic UUID. This matches the Java
//! `SchemaConstants.emptyVersion` computation approach.

use md5::{Digest, Md5};
use uuid::Uuid;

use crate::catalog::SchemaSnapshot;

/// Compute a schema version UUID from a schema snapshot.
///
/// The algorithm: sort all keyspaces and their tables alphabetically,
/// serialize key metadata properties into a deterministic byte stream,
/// compute MD5, and convert to UUID v3-style.
///
/// This is compatible with the Java `Schema.getVersion()` approach.
pub fn compute_schema_version(snapshot: &SchemaSnapshot) -> Uuid {
    let mut hasher = Md5::new();

    for (ks_name, ks) in &snapshot.keyspaces {
        hasher.update(ks_name.as_bytes());
        hasher.update(ks.params.replication.strategy_class.as_bytes());

        for (opt_key, opt_val) in &ks.params.replication.options {
            hasher.update(opt_key.as_bytes());
            hasher.update(opt_val.as_bytes());
        }

        for (table_name, table) in &ks.tables {
            hasher.update(table_name.as_bytes());

            for col in &table.columns {
                hasher.update(col.name.as_bytes());
                hasher.update(format!("{:?}", col.column_type).as_bytes());
                hasher.update(format!("{:?}", col.kind).as_bytes());
            }
        }
    }

    let result = hasher.finalize();
    // Convert MD5 to UUID (same approach as Java's UUID.nameUUIDFromBytes)
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&result[..16]);
    // Set version 3 (name-based MD5)
    bytes[6] = (bytes[6] & 0x0f) | 0x30;
    // Set variant (RFC 4122)
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Compute the empty schema version (no user keyspaces).
pub fn empty_schema_version() -> Uuid {
    let snapshot = SchemaSnapshot::empty();
    compute_schema_version(&snapshot)
}

/// Result of a schema agreement check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaAgreementStatus {
    /// All nodes agree on the schema version.
    Agreed(Uuid),
    /// Nodes disagree; maps schema version → list of node addresses.
    Disagreed {
        local_version: Uuid,
        peer_versions: Vec<(String, Uuid)>,
    },
    /// Unable to determine (e.g., during bootstrap).
    Unknown,
}

/// Check if the local schema version matches all peer schema versions.
pub fn check_schema_agreement(
    local_version: Uuid,
    peer_versions: &[(String, Uuid)],
) -> SchemaAgreementStatus {
    if peer_versions.is_empty() {
        return SchemaAgreementStatus::Agreed(local_version);
    }

    let all_agree = peer_versions.iter().all(|(_, v)| *v == local_version);
    if all_agree {
        SchemaAgreementStatus::Agreed(local_version)
    } else {
        SchemaAgreementStatus::Disagreed {
            local_version,
            peer_versions: peer_versions.to_vec(),
        }
    }
}

/// Build a `SchemaCatalog` by inserting all system keyspaces into an empty catalog.
pub fn bootstrap_system_schema() -> crate::catalog::SchemaCatalog {
    let mut catalog = crate::catalog::SchemaCatalog::new();
    for ks in crate::system_keyspaces::all_system_keyspaces() {
        catalog = catalog.with_keyspace(ks);
    }
    catalog
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_version_is_deterministic() {
        let v1 = empty_schema_version();
        let v2 = empty_schema_version();
        assert_eq!(v1, v2, "empty schema version should be deterministic");
    }

    #[test]
    fn version_changes_with_schema() {
        let empty = SchemaSnapshot::empty();
        let v_empty = compute_schema_version(&empty);

        let catalog = bootstrap_system_schema();
        let v_with_sys = compute_schema_version(&catalog.snapshot());

        assert_ne!(
            v_empty, v_with_sys,
            "version should change when schema changes"
        );
    }

    #[test]
    fn agreement_with_no_peers() {
        let v = empty_schema_version();
        assert_eq!(
            check_schema_agreement(v, &[]),
            SchemaAgreementStatus::Agreed(v)
        );
    }

    #[test]
    fn agreement_with_matching_peers() {
        let v = empty_schema_version();
        let peers = vec![("node1".to_string(), v), ("node2".to_string(), v)];
        assert_eq!(
            check_schema_agreement(v, &peers),
            SchemaAgreementStatus::Agreed(v)
        );
    }

    #[test]
    fn disagreement_detected() {
        let v1 = empty_schema_version();
        let v2 = Uuid::new_v4();
        let peers = vec![("node1".to_string(), v1), ("node2".to_string(), v2)];
        match check_schema_agreement(v1, &peers) {
            SchemaAgreementStatus::Disagreed { local_version, .. } => {
                assert_eq!(local_version, v1);
            }
            _ => panic!("expected disagreement"),
        }
    }

    #[test]
    fn bootstrap_creates_system_keyspaces() {
        let catalog = bootstrap_system_schema();
        let snap = catalog.snapshot();
        assert!(snap.keyspace("system").is_some());
        assert!(snap.keyspace("system_schema").is_some());
        assert!(snap.keyspace("system_auth").is_some());
        assert!(snap.keyspace("system_traces").is_some());
        assert!(snap.keyspace("system_distributed").is_some());
    }

    #[test]
    fn version_is_uuid_v3() {
        let v = empty_schema_version();
        assert_eq!(v.get_version_num(), 3);
    }
}
