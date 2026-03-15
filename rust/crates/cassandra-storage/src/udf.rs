// Licensed under Apache License, Version 2.0.

//! # User-Defined Functions and Aggregates (Stub)
//!
//! ## Status: DEFERRED — behind `udfs` feature flag
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.cql3.functions.UDFunction`
//! `org.apache.cassandra.cql3.functions.UDAggregate`
//!
//! ## Gap Documentation
//!
//! - **What's missing**: UDF/UDA execution engine, function registration,
//!   CQL `CREATE FUNCTION` / `CREATE AGGREGATE` support, sandboxed
//!   execution environment.
//! - **Why deferred**: Java UDFs run in a sandboxed JVM thread with a
//!   security manager. Translating this to Rust requires either:
//!   1. A WASM sandbox (wasmtime/wasmer) for user code
//!   2. A scripting engine (Lua/Rhai) for simple functions
//!   3. Native Rust plugin loading (unsafe, requires careful design)
//! - **Closure path**: WASM-based UDF sandbox. Users compile functions
//!   to WASM, register via CQL, and the engine runs them in a sandboxed
//!   runtime with memory/time limits. Estimated effort: 4-6 weeks.
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! cassandra-storage = { path = ".", features = ["udfs"] }
//! ```

use serde::{Deserialize, Serialize};

/// UDF definition metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdfDefinition {
    /// Function name.
    pub name: String,
    /// Keyspace.
    pub keyspace: String,
    /// Argument types (CQL type names).
    pub arg_types: Vec<String>,
    /// Return type (CQL type name).
    pub return_type: String,
    /// Language (java, wasm, etc.).
    pub language: String,
    /// Function body (source code or WASM bytecode reference).
    pub body: String,
    /// Whether this function is called on null input.
    pub called_on_null_input: bool,
}

/// UDA definition metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdaDefinition {
    /// Aggregate name.
    pub name: String,
    /// Keyspace.
    pub keyspace: String,
    /// Argument types.
    pub arg_types: Vec<String>,
    /// State function name.
    pub state_func: String,
    /// Final function name (optional).
    pub final_func: Option<String>,
    /// State type.
    pub state_type: String,
    /// Initial state value.
    pub init_cond: Option<String>,
}

/// Trait for UDF execution.
pub trait UserDefinedFunction: Send + Sync + std::fmt::Debug {
    /// Execute the function with the given arguments.
    /// Arguments and return value are serialized CQL values.
    fn execute(&self, args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String>;

    /// Get the function definition.
    fn definition(&self) -> &UdfDefinition;
}

/// Trait for UDA execution.
pub trait UserDefinedAggregate: Send + Sync + std::fmt::Debug {
    /// Initialize the aggregate state.
    fn init(&self) -> Option<Vec<u8>>;

    /// Apply the state function to accumulate a value.
    fn accumulate(
        &self,
        state: Option<Vec<u8>>,
        args: &[Option<Vec<u8>>],
    ) -> Result<Option<Vec<u8>>, String>;

    /// Apply the final function to produce the result.
    fn finalize(&self, state: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, String>;

    /// Get the aggregate definition.
    fn definition(&self) -> &UdaDefinition;
}

/// Stub UDF manager.
#[derive(Debug, Default)]
pub struct UdfManager {
    _udfs: Vec<UdfDefinition>,
    _udas: Vec<UdaDefinition>,
}

impl UdfManager {
    pub fn new() -> Self {
        Self {
            _udfs: Vec::new(),
            _udas: Vec::new(),
        }
    }

    /// Register a UDF. Stub — returns error.
    pub fn register_function(&mut self, _def: UdfDefinition) -> Result<(), String> {
        Err("UDFs are not implemented. Feature is deferred (requires WASM sandbox).".to_string())
    }

    /// Register a UDA. Stub — returns error.
    pub fn register_aggregate(&mut self, _def: UdaDefinition) -> Result<(), String> {
        Err("UDAs are not implemented. Feature is deferred (requires WASM sandbox).".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udf_manager_stub() {
        let mut mgr = UdfManager::new();
        let def = UdfDefinition {
            name: "my_func".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "wasm".to_string(),
            body: "".to_string(),
            called_on_null_input: false,
        };
        assert!(mgr.register_function(def).is_err());
    }

    #[test]
    fn uda_manager_stub() {
        let mut mgr = UdfManager::new();
        let def = UdaDefinition {
            name: "my_agg".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            state_func: "my_state_func".to_string(),
            final_func: None,
            state_type: "int".to_string(),
            init_cond: Some("0".to_string()),
        };
        assert!(mgr.register_aggregate(def).is_err());
    }
}
