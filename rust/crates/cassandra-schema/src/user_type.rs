// Licensed under Apache License, Version 2.0.

//! User-defined type (UDT) metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.Types`
//! - `org.apache.cassandra.db.marshal.UserType`

use serde::{Deserialize, Serialize};

/// Metadata for a user-defined type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserType {
    /// Keyspace containing the type.
    pub keyspace: String,
    /// Type name.
    pub name: String,
    /// Ordered field names.
    pub field_names: Vec<String>,
    /// Ordered field types (as CQL type strings).
    pub field_types: Vec<String>,
}

impl UserType {
    /// Create a new user-defined type.
    pub fn new(keyspace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            name: name.into(),
            field_names: Vec::new(),
            field_types: Vec::new(),
        }
    }

    /// Add a field to the type.
    pub fn with_field(mut self, name: impl Into<String>, field_type: impl Into<String>) -> Self {
        self.field_names.push(name.into());
        self.field_types.push(field_type.into());
        self
    }

    /// Number of fields.
    pub fn field_count(&self) -> usize {
        self.field_names.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_user_type() {
        let udt = UserType::new("ks", "address")
            .with_field("street", "text")
            .with_field("city", "text")
            .with_field("zip", "int");

        assert_eq!(udt.name, "address");
        assert_eq!(udt.keyspace, "ks");
        assert_eq!(udt.field_count(), 3);
        assert_eq!(udt.field_names, vec!["street", "city", "zip"]);
        assert_eq!(udt.field_types, vec!["text", "text", "int"]);
    }

    #[test]
    fn serde_round_trip() {
        let udt = UserType::new("ks", "point")
            .with_field("x", "double")
            .with_field("y", "double");
        let json = serde_json::to_string(&udt).unwrap();
        let deserialized: UserType = serde_json::from_str(&json).unwrap();
        assert_eq!(udt, deserialized);
    }
}
