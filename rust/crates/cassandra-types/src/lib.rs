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

//! # cassandra-types
//!
//! CQL type system: native types, collections, tuples, UDTs, serialization
//! and comparison.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.marshal` (AbstractType hierarchy)
//! - `org.apache.cassandra.serializers` (type serializers)
//!
//! ## Architecture
//!
//! The CQL type system is modeled as a Rust enum (`CqlType`) rather than a
//! trait-based hierarchy. This enables exhaustive pattern matching, avoids
//! virtual dispatch on hot paths, and keeps the type information inline.

pub mod native;
pub mod collections;
pub mod udt;
pub mod codec;
pub mod comparator;
pub mod partition_key;
pub mod clustering_key;
pub mod vector;

pub use native::CqlType;
pub use collections::{ListType, SetType, MapType, TupleType};
pub use udt::UserDefinedType;
pub use codec::CqlValue;
pub use vector::VectorValue;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_compiles_with_modules() {
        // Verify module re-exports work
        let _ = CqlType::Int;
        let _ = CqlType::Varchar;
        let _ = CqlType::Vector(Box::new(CqlType::Float), 3);
    }
}
