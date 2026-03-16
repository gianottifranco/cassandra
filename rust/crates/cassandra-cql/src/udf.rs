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
//! 2. A feature-gated WASM sandbox stub for future implementation
//! 3. A native Rust function extension point

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

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
#[derive(Debug, Clone)]
pub enum UdfValue {
    Null,
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
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
        let args_sig: Vec<&str> = arg_types.iter().map(|(_, t)| t.as_str()).collect();
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
}
