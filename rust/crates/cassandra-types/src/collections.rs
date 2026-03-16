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

//! CQL collection types: list, set, map, tuple.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.marshal.ListType`
//! - `org.apache.cassandra.db.marshal.SetType`
//! - `org.apache.cassandra.db.marshal.MapType`
//! - `org.apache.cassandra.db.marshal.TupleType`

use crate::native::CqlType;

/// A CQL `list<T>` type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListType {
    pub element_type: CqlType,
    pub frozen: bool,
}

impl ListType {
    pub fn new(element_type: CqlType, frozen: bool) -> Self {
        Self {
            element_type,
            frozen,
        }
    }

    pub fn to_cql_type(&self) -> CqlType {
        CqlType::List(Box::new(self.element_type.clone()), self.frozen)
    }
}

/// A CQL `set<T>` type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetType {
    pub element_type: CqlType,
    pub frozen: bool,
}

impl SetType {
    pub fn new(element_type: CqlType, frozen: bool) -> Self {
        Self {
            element_type,
            frozen,
        }
    }

    pub fn to_cql_type(&self) -> CqlType {
        CqlType::Set(Box::new(self.element_type.clone()), self.frozen)
    }
}

/// A CQL `map<K, V>` type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MapType {
    pub key_type: CqlType,
    pub value_type: CqlType,
    pub frozen: bool,
}

impl MapType {
    pub fn new(key_type: CqlType, value_type: CqlType, frozen: bool) -> Self {
        Self {
            key_type,
            value_type,
            frozen,
        }
    }

    pub fn to_cql_type(&self) -> CqlType {
        CqlType::Map(
            Box::new(self.key_type.clone()),
            Box::new(self.value_type.clone()),
            self.frozen,
        )
    }
}

/// A CQL `tuple<T1, T2, ...>` type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TupleType {
    pub field_types: Vec<CqlType>,
}

impl TupleType {
    pub fn new(field_types: Vec<CqlType>) -> Self {
        Self { field_types }
    }

    pub fn to_cql_type(&self) -> CqlType {
        CqlType::Tuple(self.field_types.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_type() {
        let lt = ListType::new(CqlType::Int, false);
        let cql = lt.to_cql_type();
        assert_eq!(cql.cql_name(), "list<int>");
        assert!(cql.is_multi_cell());
    }

    #[test]
    fn frozen_set() {
        let st = SetType::new(CqlType::Varchar, true);
        let cql = st.to_cql_type();
        assert_eq!(cql.cql_name(), "frozen<set<text>>");
        assert!(!cql.is_multi_cell());
    }

    #[test]
    fn map_type() {
        let mt = MapType::new(CqlType::Int, CqlType::Varchar, false);
        let cql = mt.to_cql_type();
        assert_eq!(cql.cql_name(), "map<int, text>");
    }

    #[test]
    fn tuple_type() {
        let tt = TupleType::new(vec![CqlType::Int, CqlType::Varchar, CqlType::Double]);
        let cql = tt.to_cql_type();
        assert_eq!(cql.cql_name(), "tuple<int, text, double>");
    }
}
