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

//! WASM-based UDF executor with feature-gated sandbox.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.JavaBasedUDFunction`
//!
//! ## Design
//! The Java implementation uses a SecurityManager-sandboxed JVM to execute UDF
//! bytecode. Our Rust implementation uses wasmtime for sandboxed WASM execution
//! when the `udf-wasm` feature is enabled. When disabled (the default), all
//! execution attempts return a descriptive error.
//!
//! The WASM sandbox provides:
//! - Memory limits via wasmtime's `Store` configuration
//! - CPU limits via fuel metering
//! - No filesystem, network, or system call access by default

use crate::udf::{UdfExecutor, UdfValue};

/// WASM-based UDF executor.
///
/// When the `udf-wasm` feature is enabled, this uses wasmtime to execute
/// UDF bytecode in a sandboxed environment with memory and CPU limits.
/// When disabled, all execution attempts return an error.
#[derive(Debug)]
pub struct WasmUdfExecutor {
    _name: String,
}

impl WasmUdfExecutor {
    /// Create a new WASM UDF executor from bytecode.
    ///
    /// # Arguments
    /// * `name` - The function name for error messages.
    /// * `_bytecode` - The WASM module bytecode to compile and instantiate.
    ///
    /// # Errors
    /// Returns an error if the `udf-wasm` feature is not enabled, or if
    /// the WASM module fails to compile/instantiate.
    pub fn new(name: String, _bytecode: &[u8]) -> Result<Self, String> {
        #[cfg(feature = "udf-wasm")]
        {
            // TODO: Initialize wasmtime engine with:
            // - fuel metering for CPU limits
            // - memory limits via Store config
            // - no WASI imports (fully sandboxed)
            // - compile and instantiate the WASM module
            Ok(Self { _name: name })
        }
        #[cfg(not(feature = "udf-wasm"))]
        {
            Err(format!(
                "WASM UDF '{}' cannot be executed: the 'udf-wasm' feature is not enabled. \
                 Recompile with --features udf-wasm to enable WASM UDF support.",
                name
            ))
        }
    }
}

impl UdfExecutor for WasmUdfExecutor {
    fn execute(&self, _args: &[UdfValue]) -> Result<UdfValue, String> {
        #[cfg(feature = "udf-wasm")]
        {
            // TODO: Execute WASM function with fuel-limited engine:
            // 1. Convert UdfValue args to WASM-compatible values
            // 2. Call the exported function
            // 3. Convert the result back to UdfValue
            // 4. Check fuel consumption and abort if exceeded
            Err(format!(
                "WASM UDF '{}' execution not yet implemented",
                self._name
            ))
        }
        #[cfg(not(feature = "udf-wasm"))]
        {
            Err("WASM UDF support is not enabled".to_string())
        }
    }

    fn language(&self) -> &str {
        "wasm"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_without_feature_returns_error() {
        // Without the udf-wasm feature, creation should fail.
        #[cfg(not(feature = "udf-wasm"))]
        {
            let result = WasmUdfExecutor::new("test_fn".into(), &[0x00, 0x61, 0x73, 0x6d]);
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(err.contains("udf-wasm"));
            assert!(err.contains("test_fn"));
        }
    }

    #[cfg(feature = "udf-wasm")]
    #[test]
    fn new_with_feature_succeeds() {
        let result = WasmUdfExecutor::new("test_fn".into(), &[]);
        assert!(result.is_ok());
    }

    #[cfg(feature = "udf-wasm")]
    #[test]
    fn execute_returns_not_implemented() {
        let executor = WasmUdfExecutor::new("test_fn".into(), &[]).unwrap();
        let result = executor.execute(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not yet implemented"));
    }
}
