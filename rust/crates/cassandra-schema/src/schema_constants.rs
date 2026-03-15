// Licensed under Apache License, Version 2.0.

//! Schema constants: system keyspace names and naming rules.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.SchemaConstants`

/// Maximum length of a keyspace or table name.
pub const NAME_LENGTH: usize = 48;

/// System keyspace: core cluster metadata.
pub const SYSTEM_KEYSPACE: &str = "system";
/// System keyspace: schema definitions.
pub const SCHEMA_KEYSPACE: &str = "system_schema";
/// System keyspace: distributed data (repair, views).
pub const DISTRIBUTED_KEYSPACE: &str = "system_distributed";
/// System keyspace: auth data.
pub const AUTH_KEYSPACE: &str = "system_auth";
/// System keyspace: trace data.
pub const TRACE_KEYSPACE: &str = "system_traces";
/// System keyspace: virtual tables.
pub const VIRTUAL_VIEWS_KEYSPACE: &str = "system_virtual_schema";

/// All system keyspace names.
pub const SYSTEM_KEYSPACES: &[&str] = &[
    SYSTEM_KEYSPACE,
    SCHEMA_KEYSPACE,
    DISTRIBUTED_KEYSPACE,
    AUTH_KEYSPACE,
    TRACE_KEYSPACE,
    VIRTUAL_VIEWS_KEYSPACE,
];

/// Returns `true` if this is a system/internal keyspace.
pub fn is_system_keyspace(name: &str) -> bool {
    SYSTEM_KEYSPACES.contains(&name) || name.starts_with("system")
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
