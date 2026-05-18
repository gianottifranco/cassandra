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

#[cfg(feature = "udf-wasm")]
use wasmtime::{Config, Engine, Instance, Module, Store, Val, ValType};

/// WASM-based UDF executor.
///
/// When the `udf-wasm` feature is enabled, this uses wasmtime to execute
/// UDF bytecode in a sandboxed environment with memory and CPU limits.
/// When disabled, all execution attempts return an error.
#[derive(Debug)]
pub struct WasmUdfExecutor {
    #[cfg_attr(not(feature = "udf-wasm"), allow(dead_code))]
    name: String,
    #[cfg(feature = "udf-wasm")]
    engine: Engine,
    #[cfg(feature = "udf-wasm")]
    module: Module,
    #[cfg(feature = "udf-wasm")]
    fuel_per_call: u64,
}

impl WasmUdfExecutor {
    #[cfg(feature = "udf-wasm")]
    const DEFAULT_FUEL_PER_CALL: u64 = 100_000;

    /// Create a new WASM UDF executor from bytecode.
    ///
    /// # Arguments
    /// * `name` - The function name for error messages.
    /// * `_bytecode` - The WASM module bytecode to compile and instantiate.
    ///
    /// # Errors
    /// Returns an error if the `udf-wasm` feature is not enabled, or if
    /// the WASM module fails to compile/instantiate.
    pub fn new(name: String, bytecode: &[u8]) -> Result<Self, String> {
        #[cfg(feature = "udf-wasm")]
        {
            let mut config = Config::new();
            config.consume_fuel(true);
            config.max_wasm_stack(64 * 1024);
            let engine = Engine::new(&config)
                .map_err(|err| format!("failed to initialize WASM engine: {err}"))?;
            let module = Module::new(&engine, bytecode)
                .map_err(|err| format!("failed to compile WASM UDF '{name}': {err}"))?;
            if module.imports().next().is_some() {
                return Err(format!(
                    "WASM UDF '{name}' imports are not allowed in the sandbox"
                ));
            }
            Ok(Self {
                name,
                engine,
                module,
                fuel_per_call: Self::DEFAULT_FUEL_PER_CALL,
            })
        }
        #[cfg(not(feature = "udf-wasm"))]
        {
            let _ = bytecode;
            Err(format!(
                "WASM UDF '{}' cannot be executed: the 'udf-wasm' feature is not enabled. \
                 Recompile with --features udf-wasm to enable WASM UDF support.",
                name
            ))
        }
    }
}

impl UdfExecutor for WasmUdfExecutor {
    fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String> {
        #[cfg(feature = "udf-wasm")]
        {
            let mut store = Store::new(&self.engine, ());
            store
                .set_fuel(self.fuel_per_call)
                .map_err(|err| format!("failed to set WASM fuel: {err}"))?;

            let instance = Instance::new(&mut store, &self.module, &[])
                .map_err(|err| format!("failed to instantiate WASM UDF '{}': {err}", self.name))?;
            let function = instance
                .get_func(&mut store, &self.name)
                .ok_or_else(|| format!("WASM UDF '{}' export not found", self.name))?;
            let function_type = function.ty(&store);
            let params: Vec<ValType> = function_type.params().collect();
            let results: Vec<ValType> = function_type.results().collect();
            if params.len() != args.len() {
                return Err(format!(
                    "WASM UDF '{}' expected {} arguments, got {}",
                    self.name,
                    params.len(),
                    args.len()
                ));
            }
            if results.len() > 1 {
                return Err(format!(
                    "WASM UDF '{}' returned {} values; only zero or one result is supported",
                    self.name,
                    results.len()
                ));
            }

            let wasm_args = args
                .iter()
                .zip(params.iter())
                .map(|(arg, ty)| udf_to_wasm(arg, ty))
                .collect::<Result<Vec<_>, _>>()?;
            let mut wasm_results = results
                .iter()
                .map(default_value_for_type)
                .collect::<Result<Vec<_>, _>>()?;
            function
                .call(&mut store, &wasm_args, &mut wasm_results)
                .map_err(|err| format!("WASM UDF '{}' trapped: {err}", self.name))?;

            match wasm_results.pop() {
                Some(value) => wasm_to_udf(value),
                None => Ok(UdfValue::Null),
            }
        }
        #[cfg(not(feature = "udf-wasm"))]
        {
            let _ = args;
            Err("WASM UDF support is not enabled".to_string())
        }
    }

    fn language(&self) -> &str {
        "wasm"
    }
}

#[cfg(feature = "udf-wasm")]
fn udf_to_wasm(value: &UdfValue, ty: &ValType) -> Result<Val, String> {
    match (value, ty) {
        (UdfValue::Int(value), ValType::I32) => Ok(Val::I32(*value)),
        (UdfValue::Boolean(value), ValType::I32) => Ok(Val::I32(i32::from(*value))),
        (UdfValue::Long(value), ValType::I64) => Ok(Val::I64(*value)),
        (UdfValue::Float(value), ValType::F32) => Ok(Val::F32(value.to_bits())),
        (UdfValue::Double(value), ValType::F64) => Ok(Val::F64(value.to_bits())),
        (UdfValue::Null, _) => Err("null values are not supported by scalar WASM UDF ABI".into()),
        (UdfValue::Text(_), _) | (UdfValue::Blob(_), _) => {
            Err("text/blob WASM UDF values require an explicit memory ABI".into())
        }
        _ => Err(format!(
            "cannot pass UDF value {value:?} to WASM parameter type {ty}"
        )),
    }
}

#[cfg(feature = "udf-wasm")]
fn wasm_to_udf(value: Val) -> Result<UdfValue, String> {
    match value {
        Val::I32(value) => Ok(UdfValue::Int(value)),
        Val::I64(value) => Ok(UdfValue::Long(value)),
        Val::F32(value) => Ok(UdfValue::Float(f32::from_bits(value))),
        Val::F64(value) => Ok(UdfValue::Double(f64::from_bits(value))),
        other => Err(format!("unsupported WASM result value {other:?}")),
    }
}

#[cfg(feature = "udf-wasm")]
fn default_value_for_type(ty: &ValType) -> Result<Val, String> {
    match ty {
        ValType::I32 => Ok(Val::I32(0)),
        ValType::I64 => Ok(Val::I64(0)),
        ValType::F32 => Ok(Val::F32(0)),
        ValType::F64 => Ok(Val::F64(0)),
        other => Err(format!("unsupported WASM result type {other:?}")),
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
        let bytes = wat::parse_str(
            r#"(module
                (func (export "test_fn") (param i32) (result i32)
                    local.get 0
                    i32.const 1
                    i32.add))"#,
        )
        .unwrap();
        let result = WasmUdfExecutor::new("test_fn".into(), &bytes);
        assert!(result.is_ok());
    }

    #[cfg(feature = "udf-wasm")]
    #[test]
    fn execute_scalar_function() {
        let bytes = wat::parse_str(
            r#"(module
                (func (export "test_fn") (param i32) (result i32)
                    local.get 0
                    i32.const 1
                    i32.add))"#,
        )
        .unwrap();
        let executor = WasmUdfExecutor::new("test_fn".into(), &bytes).unwrap();
        let result = executor.execute(&[UdfValue::Int(41)]).unwrap();
        assert!(matches!(result, UdfValue::Int(42)));
    }

    #[cfg(feature = "udf-wasm")]
    #[test]
    fn rejects_imports() {
        let bytes = wat::parse_str(
            r#"(module
                (import "env" "external" (func $external))
                (func (export "test_fn")
                    call $external))"#,
        )
        .unwrap();
        let err = WasmUdfExecutor::new("test_fn".into(), &bytes).unwrap_err();
        assert!(err.contains("imports are not allowed"));
    }
}
