// Licensed under Apache License, Version 2.0.

//! Core function trait and registry with overload resolution.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.FunctionResolver`
//! - `org.apache.cassandra.cql3.functions.NativeScalarFunction`

use cassandra_types::CqlType;
use dashmap::DashMap;
use std::sync::Arc;

/// Keyspace-qualified function name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionName {
    pub keyspace: Option<String>,
    pub name: String,
}

impl FunctionName {
    pub fn native(name: impl Into<String>) -> Self {
        Self {
            keyspace: Some("system".to_string()),
            name: name.into(),
        }
    }

    pub fn new(keyspace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            keyspace: Some(keyspace.into()),
            name: name.into(),
        }
    }

    /// Canonical key for registry lookup: "keyspace.name" or just "name".
    pub fn canonical(&self) -> String {
        match &self.keyspace {
            Some(ks) => format!("{}.{}", ks, self.name),
            None => self.name.clone(),
        }
    }
}

/// A built-in CQL scalar function.
pub trait CqlFunction: Send + Sync {
    /// Function name.
    fn name(&self) -> &str;

    /// Expected argument types. Empty for variadic functions like token().
    fn arg_types(&self) -> Vec<CqlType>;

    /// Return type.
    fn return_type(&self) -> CqlType;

    /// Execute the function on serialized CQL byte values.
    /// Each argument is `Option<&[u8]>` (None = null).
    /// Returns `Option<Vec<u8>>` (None = null result).
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String>;
}

/// Registry for built-in CQL functions with concurrent access.
pub struct FunctionRegistry {
    /// Functions indexed by (lowercase_name, arg_types).
    functions: DashMap<String, Vec<Arc<dyn CqlFunction>>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: DashMap::new(),
        }
    }

    /// Create a new registry pre-populated with all built-in functions.
    pub fn with_builtins() -> Self {
        let registry = Self::new();
        super::time_uuid::register_all(&registry);
        super::token_cast_blob::register_all(&registry);
        super::math_json::register_all(&registry);
        super::vector_similarity::register_all(&registry);
        super::masking::register_all(&registry);
        registry
    }

    /// Register a function.
    pub fn register(&self, func: Arc<dyn CqlFunction>) {
        let name = func.name().to_lowercase();
        self.functions.entry(name).or_default().push(func);
    }

    /// Resolve a function by name and argument types.
    /// Tries exact match first, then falls back to matching by arg count.
    pub fn resolve(&self, name: &str, arg_types: &[CqlType]) -> Option<Arc<dyn CqlFunction>> {
        let key = name.to_lowercase();
        let overloads = self.functions.get(&key)?;

        // Exact type match
        for func in overloads.iter() {
            let expected = func.arg_types();
            if expected.len() == arg_types.len() {
                let matches = expected
                    .iter()
                    .zip(arg_types.iter())
                    .all(|(e, a)| types_compatible(e, a));
                if matches {
                    return Some(Arc::clone(func));
                }
            }
        }

        // Fallback: match by arg count only (for polymorphic functions)
        for func in overloads.iter() {
            let expected = func.arg_types();
            if expected.is_empty() || expected.len() == arg_types.len() {
                return Some(Arc::clone(func));
            }
        }

        None
    }

    /// Resolve by name only (for zero-arg functions or when types are unknown).
    pub fn resolve_by_name(&self, name: &str) -> Option<Arc<dyn CqlFunction>> {
        let key = name.to_lowercase();
        let overloads = self.functions.get(&key)?;
        overloads.first().cloned()
    }

    /// List all registered function names.
    pub fn function_names(&self) -> Vec<String> {
        self.functions.iter().map(|e| e.key().clone()).collect()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if two CQL types are compatible for overload resolution.
fn types_compatible(expected: &CqlType, actual: &CqlType) -> bool {
    if expected == actual {
        return true;
    }
    // Numeric widening: int → bigint, float → double
    matches!(
        (expected, actual),
        (CqlType::Bigint, CqlType::Int)
            | (CqlType::Double, CqlType::Float)
            | (CqlType::Varchar, CqlType::Ascii)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestFunc {
        name: String,
        args: Vec<CqlType>,
        ret: CqlType,
    }

    impl CqlFunction for TestFunc {
        fn name(&self) -> &str {
            &self.name
        }
        fn arg_types(&self) -> Vec<CqlType> {
            self.args.clone()
        }
        fn return_type(&self) -> CqlType {
            self.ret.clone()
        }
        fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
            Ok(Some(vec![42]))
        }
    }

    #[test]
    fn register_and_resolve() {
        let registry = FunctionRegistry::new();
        registry.register(Arc::new(TestFunc {
            name: "myfunc".into(),
            args: vec![CqlType::Int],
            ret: CqlType::Int,
        }));

        let f = registry.resolve("myfunc", &[CqlType::Int]);
        assert!(f.is_some());
        assert_eq!(f.unwrap().name(), "myfunc");
    }

    #[test]
    fn resolve_case_insensitive() {
        let registry = FunctionRegistry::new();
        registry.register(Arc::new(TestFunc {
            name: "MyFunc".into(),
            args: vec![],
            ret: CqlType::Int,
        }));

        assert!(registry.resolve("myfunc", &[]).is_some());
        assert!(registry.resolve("MYFUNC", &[]).is_some());
    }

    #[test]
    fn overload_resolution() {
        let registry = FunctionRegistry::new();
        registry.register(Arc::new(TestFunc {
            name: "sum".into(),
            args: vec![CqlType::Int],
            ret: CqlType::Int,
        }));
        registry.register(Arc::new(TestFunc {
            name: "sum".into(),
            args: vec![CqlType::Bigint],
            ret: CqlType::Bigint,
        }));

        let f = registry.resolve("sum", &[CqlType::Bigint]);
        assert!(f.is_some());
        assert_eq!(f.unwrap().return_type(), CqlType::Bigint);
    }

    #[test]
    fn resolve_nonexistent() {
        let registry = FunctionRegistry::new();
        assert!(registry.resolve("nope", &[]).is_none());
    }

    #[test]
    fn numeric_widening() {
        let registry = FunctionRegistry::new();
        registry.register(Arc::new(TestFunc {
            name: "f".into(),
            args: vec![CqlType::Bigint],
            ret: CqlType::Bigint,
        }));

        // Int should widen to Bigint
        let f = registry.resolve("f", &[CqlType::Int]);
        assert!(f.is_some());
    }

    #[test]
    fn resolve_by_name_only() {
        let registry = FunctionRegistry::new();
        registry.register(Arc::new(TestFunc {
            name: "now".into(),
            args: vec![],
            ret: CqlType::Timeuuid,
        }));
        assert!(registry.resolve_by_name("now").is_some());
    }
}
