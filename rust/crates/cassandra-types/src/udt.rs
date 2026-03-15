// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! User-Defined Type (UDT) metadata.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.marshal.UserType`

use serde::{Deserialize, Serialize};

use crate::native::CqlType;

/// A User-Defined Type in the CQL type system.
///
/// UDTs belong to a keyspace and have named, typed fields.
/// They can be frozen (single cell) or multi-cell (field-level updates).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserDefinedType {
    /// Keyspace this UDT belongs to.
    pub keyspace: String,
    /// Name of the UDT.
    pub name: String,
    /// Ordered list of field names.
    pub field_names: Vec<String>,
    /// Ordered list of field types (parallel to `field_names`).
    pub field_types: Vec<CqlType>,
    /// Whether this UDT is stored as multiple cells (non-frozen).
    pub is_multi_cell: bool,
}

impl UserDefinedType {
    /// Create a new UDT definition.
    pub fn new(
        keyspace: String,
        name: String,
        field_names: Vec<String>,
        field_types: Vec<CqlType>,
        is_multi_cell: bool,
    ) -> Self {
        assert_eq!(
            field_names.len(),
            field_types.len(),
            "UDT field_names and field_types must have equal length"
        );
        Self {
            keyspace,
            name,
            field_names,
            field_types,
            is_multi_cell,
        }
    }

    /// Number of fields.
    pub fn field_count(&self) -> usize {
        self.field_names.len()
    }

    /// Look up a field type by name.
    pub fn field_type(&self, name: &str) -> Option<&CqlType> {
        self.field_names
            .iter()
            .position(|n| n == name)
            .map(|idx| &self.field_types[idx])
    }

    /// Convert to the `CqlType` representation.
    pub fn to_cql_type(&self) -> CqlType {
        CqlType::Udt {
            keyspace: self.keyspace.clone(),
            name: self.name.clone(),
            field_names: self.field_names.clone(),
            field_types: self.field_types.clone(),
            is_multi_cell: self.is_multi_cell,
        }
    }

    /// Fully-qualified name as `keyspace.name`.
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.keyspace, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_udt() -> UserDefinedType {
        UserDefinedType::new(
            "test_ks".to_string(),
            "address".to_string(),
            vec!["street".to_string(), "city".to_string(), "zip".to_string()],
            vec![CqlType::Varchar, CqlType::Varchar, CqlType::Int],
            false,
        )
    }

    #[test]
    fn field_count() {
        let udt = sample_udt();
        assert_eq!(udt.field_count(), 3);
    }

    #[test]
    fn field_type_lookup() {
        let udt = sample_udt();
        assert_eq!(udt.field_type("street"), Some(&CqlType::Varchar));
        assert_eq!(udt.field_type("zip"), Some(&CqlType::Int));
        assert_eq!(udt.field_type("nonexistent"), None);
    }

    #[test]
    fn full_name() {
        let udt = sample_udt();
        assert_eq!(udt.full_name(), "test_ks.address");
    }

    #[test]
    fn to_cql_type() {
        let udt = sample_udt();
        let cql = udt.to_cql_type();
        assert_eq!(cql.cql_name(), "test_ks.address");
    }

    #[test]
    #[should_panic(expected = "equal length")]
    fn mismatched_fields_panics() {
        UserDefinedType::new(
            "ks".to_string(),
            "bad".to_string(),
            vec!["a".to_string()],
            vec![CqlType::Int, CqlType::Varchar],
            false,
        );
    }
}
