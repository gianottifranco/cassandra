// Licensed under Apache License, Version 2.0.

//! Schema constants: system keyspace names, naming rules, and keyspace taxonomy.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.SchemaConstants`

/// Maximum length of a keyspace or table name.
pub const NAME_LENGTH: usize = 48;

/// Maximum filename length (OS limit).
pub const FILENAME_LENGTH: usize = 255;

/// Length of a table UUID as hex string.
pub const TABLE_UUID_AS_HEX_LENGTH: usize = 32;

// ─── Keyspace Names ────────────────────────────────────────────────────────

/// System keyspace: core cluster metadata.
pub const SYSTEM_KEYSPACE: &str = "system";
/// System keyspace: schema definitions.
pub const SCHEMA_KEYSPACE: &str = "system_schema";
/// System keyspace: cluster metadata (TCM).
pub const METADATA_KEYSPACE: &str = "system_cluster_metadata";
/// System keyspace: distributed data (repair, views).
pub const DISTRIBUTED_KEYSPACE: &str = "system_distributed";
/// System keyspace: auth data.
pub const AUTH_KEYSPACE: &str = "system_auth";
/// System keyspace: trace data.
pub const TRACE_KEYSPACE: &str = "system_traces";
/// System keyspace: Accord consensus.
pub const ACCORD_KEYSPACE: &str = "system_accord";

/// Virtual keyspace: schema metadata for virtual tables.
pub const VIRTUAL_SCHEMA_KEYSPACE: &str = "system_virtual_schema";
/// Virtual keyspace: system views.
pub const VIRTUAL_VIEWS_KEYSPACE: &str = "system_views";
/// Virtual keyspace: system metrics.
pub const VIRTUAL_METRICS_KEYSPACE: &str = "system_metrics";

// ─── Keyspace Sets ─────────────────────────────────────────────────────────

/// Local system keyspaces (LocalStrategy replication).
pub const LOCAL_SYSTEM_KEYSPACES: &[&str] = &[
    SYSTEM_KEYSPACE,
    SCHEMA_KEYSPACE,
    ACCORD_KEYSPACE,
];

/// Virtual system keyspaces (no physical storage).
pub const VIRTUAL_SYSTEM_KEYSPACES: &[&str] = &[
    VIRTUAL_SCHEMA_KEYSPACE,
    VIRTUAL_VIEWS_KEYSPACE,
    VIRTUAL_METRICS_KEYSPACE,
];

/// Replicated system keyspaces (real replication strategy).
pub const REPLICATED_SYSTEM_KEYSPACES: &[&str] = &[
    TRACE_KEYSPACE,
    AUTH_KEYSPACE,
    DISTRIBUTED_KEYSPACE,
    METADATA_KEYSPACE,
];

/// All system keyspace names (union of all categories).
pub const SYSTEM_KEYSPACES: &[&str] = &[
    SYSTEM_KEYSPACE,
    SCHEMA_KEYSPACE,
    ACCORD_KEYSPACE,
    DISTRIBUTED_KEYSPACE,
    AUTH_KEYSPACE,
    TRACE_KEYSPACE,
    METADATA_KEYSPACE,
    VIRTUAL_SCHEMA_KEYSPACE,
    VIRTUAL_VIEWS_KEYSPACE,
    VIRTUAL_METRICS_KEYSPACE,
];

// ─── Classification Functions ──────────────────────────────────────────────

/// Returns `true` if this is a local system keyspace (LocalStrategy).
pub fn is_local_system_keyspace(name: &str) -> bool {
    LOCAL_SYSTEM_KEYSPACES.contains(&name) || is_virtual_system_keyspace(name)
}

/// Returns `true` if this is a replicated system keyspace.
pub fn is_replicated_system_keyspace(name: &str) -> bool {
    REPLICATED_SYSTEM_KEYSPACES.contains(&name)
}

/// Returns `true` if this is a virtual system keyspace.
pub fn is_virtual_system_keyspace(name: &str) -> bool {
    VIRTUAL_SYSTEM_KEYSPACES.contains(&name)
}

/// Returns `true` if this is any system/internal keyspace.
pub fn is_system_keyspace(name: &str) -> bool {
    is_local_system_keyspace(name) || is_replicated_system_keyspace(name)
}

/// Returns `true` if this is a non-virtual system keyspace.
pub fn is_non_virtual_system_keyspace(name: &str) -> bool {
    LOCAL_SYSTEM_KEYSPACES.contains(&name) || REPLICATED_SYSTEM_KEYSPACES.contains(&name)
}

/// Returns `true` if the given name is valid per CQL naming rules.
/// Names must be non-empty, at most NAME_LENGTH chars, and consist of
/// alphanumeric chars and underscores.
pub fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= NAME_LENGTH
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_keyspaces() {
        assert!(is_system_keyspace("system"));
        assert!(is_system_keyspace("system_schema"));
        assert!(is_system_keyspace("system_auth"));
        assert!(!is_system_keyspace("my_keyspace"));
    }

    #[test]
    fn local_system_keyspaces() {
        assert!(is_local_system_keyspace("system"));
        assert!(is_local_system_keyspace("system_schema"));
        assert!(is_local_system_keyspace("system_views")); // virtual ⊂ local
        assert!(!is_local_system_keyspace("system_auth")); // replicated
    }

    #[test]
    fn replicated_system_keyspaces() {
        assert!(is_replicated_system_keyspace("system_auth"));
        assert!(is_replicated_system_keyspace("system_traces"));
        assert!(is_replicated_system_keyspace("system_distributed"));
        assert!(!is_replicated_system_keyspace("system"));
    }

    #[test]
    fn virtual_system_keyspaces() {
        assert!(is_virtual_system_keyspace("system_views"));
        assert!(is_virtual_system_keyspace("system_virtual_schema"));
        assert!(is_virtual_system_keyspace("system_metrics"));
        assert!(!is_virtual_system_keyspace("system"));
    }

    #[test]
    fn non_virtual_system_keyspaces() {
        assert!(is_non_virtual_system_keyspace("system"));
        assert!(is_non_virtual_system_keyspace("system_auth"));
        assert!(!is_non_virtual_system_keyspace("system_views"));
    }

    #[test]
    fn valid_names() {
        assert!(is_valid_name("my_keyspace"));
        assert!(is_valid_name("users123"));
        assert!(!is_valid_name(""));
        assert!(!is_valid_name("has space"));
        assert!(!is_valid_name("has-dash"));
        let long = "a".repeat(NAME_LENGTH + 1);
        assert!(!is_valid_name(&long));
    }
}
