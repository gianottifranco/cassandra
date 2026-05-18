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

//! User-Defined Function (UDF) registry and runtime.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.UDFunction`
//! - `org.apache.cassandra.cql3.functions.JavaBasedUDFunction`
//!
//! ## Design
//! The Java implementation uses a SecurityManager-sandboxed JVM to execute
//! UDF code. Since we can't run Java bytecode natively, we provide:
//! 1. A registry trait for managing function metadata
//! 2. A feature-gated WASM sandbox for scalar Rust-native UDF modules
//! 3. A native Rust function extension point

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use cassandra_schema::canonical_cql_type_name;
use cassandra_types::{CqlType, VectorValue, codec::CqlValue};

/// Metadata for a registered UDF.
#[derive(Debug, Clone)]
pub struct UdfMetadata {
    /// Keyspace the function belongs to.
    pub keyspace: String,
    /// Function name.
    pub name: String,
    /// Argument names and types.
    pub args: Vec<(String, String)>,
    /// Return type name.
    pub return_type: String,
    /// Language (e.g., "java", "wasm", "rust").
    pub language: String,
    /// Source body (for reproducibility / debugging).
    pub body: String,
    /// Whether the function is CALLED ON NULL INPUT.
    pub called_on_null_input: bool,
}

/// A value that can be passed to/from UDFs.
#[derive(Debug, Clone, PartialEq)]
pub enum UdfValue {
    Null,
    Empty,
    Int(i32),
    Smallint(i16),
    Tinyint(i8),
    Long(i64),
    Counter(i64),
    Timestamp(i64),
    Time(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Varint(Vec<u8>),
    Decimal {
        unscaled: Vec<u8>,
        scale: i32,
    },
    Uuid([u8; 16]),
    Timeuuid([u8; 16]),
    Inet(IpAddr),
    Date(u32),
    Duration {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
    List(Vec<UdfValue>),
    Set(Vec<UdfValue>),
    Map(Vec<(UdfValue, UdfValue)>),
    Tuple(Vec<Option<UdfValue>>),
    Udt(Vec<(String, Option<UdfValue>)>),
    Vector(VectorValue),
}

impl UdfValue {
    /// Deserialize a UDF argument from the same CQL bytes Java UDFs receive.
    pub fn deserialize_arg(cql_type: &CqlType, data: Option<&[u8]>) -> Result<Self, String> {
        let Some(data) = data else {
            return Ok(Self::Null);
        };
        CqlValue::deserialize_value(cql_type, data)
            .map(Self::from_cql_value)
            .map_err(|err| err.to_string())
    }

    /// Serialize a UDF return value to CQL bytes. `Null` maps to a null return.
    pub fn serialize_return(&self, cql_type: &CqlType) -> Result<Option<Vec<u8>>, String> {
        if matches!(self, Self::Null) {
            return Ok(None);
        }
        Ok(Some(self.to_cql_value(cql_type)?.serialize_value()))
    }

    pub fn from_cql_value(value: CqlValue) -> Self {
        match value {
            CqlValue::Null => Self::Null,
            CqlValue::Empty => Self::Empty,
            CqlValue::Ascii(s) | CqlValue::Varchar(s) => Self::Text(s),
            CqlValue::Bigint(v) => Self::Long(v),
            CqlValue::Counter(v) => Self::Counter(v),
            CqlValue::Timestamp(v) => Self::Timestamp(v),
            CqlValue::Time(v) => Self::Time(v),
            CqlValue::Blob(bytes) => Self::Blob(bytes),
            CqlValue::Boolean(v) => Self::Boolean(v),
            CqlValue::Decimal { unscaled, scale } => Self::Decimal { unscaled, scale },
            CqlValue::Double(v) => Self::Double(v),
            CqlValue::Float(v) => Self::Float(v),
            CqlValue::Int(v) => Self::Int(v),
            CqlValue::Uuid(bytes) => Self::Uuid(bytes),
            CqlValue::Timeuuid(bytes) => Self::Timeuuid(bytes),
            CqlValue::Inet(addr) => Self::Inet(addr),
            CqlValue::Date(v) => Self::Date(v),
            CqlValue::Smallint(v) => Self::Smallint(v),
            CqlValue::Tinyint(v) => Self::Tinyint(v),
            CqlValue::Varint(bytes) => Self::Varint(bytes),
            CqlValue::Duration {
                months,
                days,
                nanoseconds,
            } => Self::Duration {
                months,
                days,
                nanoseconds,
            },
            CqlValue::List(items) => {
                Self::List(items.into_iter().map(Self::from_cql_value).collect())
            }
            CqlValue::Set(items) => {
                Self::Set(items.into_iter().map(Self::from_cql_value).collect())
            }
            CqlValue::Map(entries) => Self::Map(
                entries
                    .into_iter()
                    .map(|(key, value)| (Self::from_cql_value(key), Self::from_cql_value(value)))
                    .collect(),
            ),
            CqlValue::Tuple(fields) => Self::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.map(Self::from_cql_value))
                    .collect(),
            ),
            CqlValue::Udt(fields) => Self::Udt(
                fields
                    .into_iter()
                    .map(|(name, value)| (name, value.map(Self::from_cql_value)))
                    .collect(),
            ),
            CqlValue::Vector(vector) => Self::Vector(vector),
        }
    }

    pub fn to_cql_value(&self, cql_type: &CqlType) -> Result<CqlValue, String> {
        let value = match self {
            Self::Null => CqlValue::Null,
            Self::Empty => CqlValue::Empty,
            Self::Int(v) => CqlValue::Int(*v),
            Self::Smallint(v) => CqlValue::Smallint(*v),
            Self::Tinyint(v) => CqlValue::Tinyint(*v),
            Self::Long(v) => match cql_type {
                CqlType::Timestamp => CqlValue::Timestamp(*v),
                CqlType::Time => CqlValue::Time(*v),
                _ => CqlValue::Bigint(*v),
            },
            Self::Counter(v) => CqlValue::Counter(*v),
            Self::Timestamp(v) => CqlValue::Timestamp(*v),
            Self::Time(v) => CqlValue::Time(*v),
            Self::Float(v) => CqlValue::Float(*v),
            Self::Double(v) => CqlValue::Double(*v),
            Self::Text(v) => match cql_type {
                CqlType::Ascii => CqlValue::Ascii(v.clone()),
                _ => CqlValue::Varchar(v.clone()),
            },
            Self::Boolean(v) => CqlValue::Boolean(*v),
            Self::Blob(v) => CqlValue::Blob(v.clone()),
            Self::Varint(v) => CqlValue::Varint(v.clone()),
            Self::Decimal { unscaled, scale } => CqlValue::Decimal {
                unscaled: unscaled.clone(),
                scale: *scale,
            },
            Self::Uuid(v) => CqlValue::Uuid(*v),
            Self::Timeuuid(v) => CqlValue::Timeuuid(*v),
            Self::Inet(v) => CqlValue::Inet(*v),
            Self::Date(v) => CqlValue::Date(*v),
            Self::Duration {
                months,
                days,
                nanoseconds,
            } => CqlValue::Duration {
                months: *months,
                days: *days,
                nanoseconds: *nanoseconds,
            },
            Self::List(items) => CqlValue::List(
                items
                    .iter()
                    .map(|item| {
                        item.to_cql_value(collection_value_type(cql_type).unwrap_or(cql_type))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Set(items) => CqlValue::Set(
                items
                    .iter()
                    .map(|item| {
                        item.to_cql_value(collection_value_type(cql_type).unwrap_or(cql_type))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Map(entries) => {
                let (key_type, value_type) = map_types(cql_type).unwrap_or((cql_type, cql_type));
                CqlValue::Map(
                    entries
                        .iter()
                        .map(|(key, value)| {
                            Ok((key.to_cql_value(key_type)?, value.to_cql_value(value_type)?))
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                )
            }
            Self::Tuple(fields) => {
                let field_types = tuple_types(cql_type).unwrap_or_default();
                CqlValue::Tuple(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| {
                            field
                                .as_ref()
                                .map(|value| {
                                    value.to_cql_value(field_types.get(idx).unwrap_or(cql_type))
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Self::Udt(fields) => {
                let field_types = udt_field_types(cql_type);
                CqlValue::Udt(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(idx, (name, value))| {
                            Ok((
                                name.clone(),
                                value
                                    .as_ref()
                                    .map(|v| {
                                        v.to_cql_value(field_types.get(idx).unwrap_or(cql_type))
                                    })
                                    .transpose()?,
                            ))
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                )
            }
            Self::Vector(vector) => CqlValue::Vector(vector.clone()),
        };

        Ok(value)
    }
}

fn collection_value_type(cql_type: &CqlType) -> Option<&CqlType> {
    match cql_type {
        CqlType::List(inner, _) | CqlType::Set(inner, _) => Some(inner),
        _ => None,
    }
}

fn map_types(cql_type: &CqlType) -> Option<(&CqlType, &CqlType)> {
    match cql_type {
        CqlType::Map(key, value, _) => Some((key, value)),
        _ => None,
    }
}

fn tuple_types(cql_type: &CqlType) -> Option<&[CqlType]> {
    match cql_type {
        CqlType::Tuple(fields) => Some(fields.as_slice()),
        _ => None,
    }
}

fn udt_field_types(cql_type: &CqlType) -> &[CqlType] {
    match cql_type {
        CqlType::Udt { field_types, .. } => field_types.as_slice(),
        _ => &[],
    }
}

/// Trait for UDF execution backends.
///
/// Implementations can be:
/// - `NativeUdfExecutor`: Rust closures registered at compile time
/// - `WasmUdfExecutor`: WASM sandbox (future, feature-gated)
pub trait UdfExecutor: Send + Sync {
    /// Execute the function with the given arguments.
    fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String>;

    /// Return the language this executor supports.
    fn language(&self) -> &str;
}

/// Type alias for UDF entries stored in the registry.
type UdfEntry = (UdfMetadata, Arc<dyn UdfExecutor>);

/// Registry for user-defined functions.
pub struct UdfRegistry {
    /// Key: (keyspace, name, arg_types_signature)
    functions: RwLock<HashMap<String, UdfEntry>>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    /// Generate a unique key for a function.
    fn make_key(keyspace: &str, name: &str, arg_types: &[(String, String)]) -> String {
        let args_sig: Vec<String> = arg_types
            .iter()
            .map(|(_, t)| canonical_cql_type_name(t))
            .collect();
        format!("{}.{}({})", keyspace, name, args_sig.join(","))
    }

    /// Register a UDF with its executor.
    pub fn register(
        &self,
        metadata: UdfMetadata,
        executor: Arc<dyn UdfExecutor>,
    ) -> Result<(), String> {
        let key = Self::make_key(&metadata.keyspace, &metadata.name, &metadata.args);
        let mut fns = self.functions.write();
        fns.insert(key, (metadata, executor));
        Ok(())
    }

    /// Look up a UDF by keyspace, name, and argument signature.
    pub fn get(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[(String, String)],
    ) -> Option<(UdfMetadata, Arc<dyn UdfExecutor>)> {
        let key = Self::make_key(keyspace, name, arg_types);
        self.functions.read().get(&key).cloned()
    }

    /// Remove a UDF.
    pub fn unregister(&self, keyspace: &str, name: &str, arg_types: &[(String, String)]) -> bool {
        let key = Self::make_key(keyspace, name, arg_types);
        self.functions.write().remove(&key).is_some()
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.functions.read().is_empty()
    }

    /// List all functions in a keyspace.
    pub fn list_in_keyspace(&self, keyspace: &str) -> Vec<UdfMetadata> {
        self.functions
            .read()
            .values()
            .filter(|(m, _)| m.keyspace == keyspace)
            .map(|(m, _)| m.clone())
            .collect()
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestExecutor;

    impl UdfExecutor for TestExecutor {
        fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String> {
            match args.first() {
                Some(UdfValue::Int(n)) => Ok(UdfValue::Int(n * 2)),
                _ => Ok(UdfValue::Null),
            }
        }

        fn language(&self) -> &str {
            "rust"
        }
    }

    #[test]
    fn register_and_get() {
        let registry = UdfRegistry::new();
        let meta = UdfMetadata {
            keyspace: "ks".into(),
            name: "double_it".into(),
            args: vec![("val".into(), "int".into())],
            return_type: "int".into(),
            language: "rust".into(),
            body: String::new(),
            called_on_null_input: false,
        };
        registry
            .register(meta.clone(), Arc::new(TestExecutor))
            .unwrap();
        assert_eq!(registry.len(), 1);

        let (got_meta, executor) = registry
            .get("ks", "double_it", &[("val".into(), "int".into())])
            .unwrap();
        assert_eq!(got_meta.name, "double_it");

        let result = executor.execute(&[UdfValue::Int(5)]).unwrap();
        assert!(matches!(result, UdfValue::Int(10)));
    }

    #[test]
    fn unregister() {
        let registry = UdfRegistry::new();
        let meta = UdfMetadata {
            keyspace: "ks".into(),
            name: "f".into(),
            args: vec![],
            return_type: "int".into(),
            language: "rust".into(),
            body: String::new(),
            called_on_null_input: false,
        };
        registry.register(meta, Arc::new(TestExecutor)).unwrap();
        assert!(registry.unregister("ks", "f", &[]));
        assert!(registry.is_empty());
    }

    #[test]
    fn list_in_keyspace() {
        let registry = UdfRegistry::new();
        for i in 0..3 {
            let meta = UdfMetadata {
                keyspace: "ks".into(),
                name: format!("f{}", i),
                args: vec![],
                return_type: "int".into(),
                language: "rust".into(),
                body: String::new(),
                called_on_null_input: false,
            };
            registry.register(meta, Arc::new(TestExecutor)).unwrap();
        }
        assert_eq!(registry.list_in_keyspace("ks").len(), 3);
        assert_eq!(registry.list_in_keyspace("other").len(), 0);
    }

    #[test]
    fn deserialize_udf_scalar_arguments_from_cql_bytes() {
        assert_eq!(
            UdfValue::deserialize_arg(&CqlType::Int, Some(&7i32.to_be_bytes())).unwrap(),
            UdfValue::Int(7)
        );
        assert_eq!(
            UdfValue::deserialize_arg(&CqlType::Varchar, Some(b"alice")).unwrap(),
            UdfValue::Text("alice".to_string())
        );
        assert_eq!(
            UdfValue::deserialize_arg(&CqlType::Boolean, Some(&[1])).unwrap(),
            UdfValue::Boolean(true)
        );
        assert_eq!(
            UdfValue::deserialize_arg(&CqlType::Blob, None).unwrap(),
            UdfValue::Null
        );
    }

    #[test]
    fn deserialize_udf_collection_argument_from_cql_bytes() {
        let cql_type = CqlType::List(Box::new(CqlType::Int), false);
        let encoded = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]).serialize_value();

        assert_eq!(
            UdfValue::deserialize_arg(&cql_type, Some(&encoded)).unwrap(),
            UdfValue::List(vec![UdfValue::Int(1), UdfValue::Int(2)])
        );
    }

    #[test]
    fn deserialize_udf_vector_argument_from_cql_bytes() {
        let cql_type = CqlType::Vector(Box::new(CqlType::Float), 3);
        let vector = VectorValue::new(vec![1.0, -2.5, 3.25]);
        let encoded = CqlValue::Vector(vector.clone()).serialize_value();

        assert_eq!(
            UdfValue::deserialize_arg(&cql_type, Some(&encoded)).unwrap(),
            UdfValue::Vector(vector)
        );
    }

    #[test]
    fn serialize_udf_return_to_cql_bytes() {
        assert_eq!(
            UdfValue::Text("done".to_string())
                .serialize_return(&CqlType::Varchar)
                .unwrap(),
            Some(b"done".to_vec())
        );
        assert_eq!(
            UdfValue::Null.serialize_return(&CqlType::Int).unwrap(),
            None
        );
    }
}
