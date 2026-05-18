// Licensed under Apache License, Version 2.0.

//! # User-Defined Functions and Aggregates
//!
//! ## Status: feature-gated native evaluator
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.cql3.functions.UDFunction`
//! `org.apache.cassandra.cql3.functions.UDAggregate`
//!
//! This module provides a deterministic native evaluator for simple UDF/UDA
//! bodies used by the Rust storage stack. Arbitrary user code still belongs in
//! a sandboxed runtime such as WASM, but registration and execution no longer
//! fail unconditionally.
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! cassandra-storage = { path = ".", features = ["udfs"] }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

#[derive(Debug, Clone)]
struct NativeUdf {
    definition: UdfDefinition,
}

impl UserDefinedFunction for NativeUdf {
    fn execute(&self, args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
        if !self.definition.called_on_null_input && args.iter().any(Option::is_none) {
            return Ok(None);
        }
        execute_native_body(
            &self.definition.body,
            &self.definition.return_type,
            &self.definition.arg_types,
            args,
        )
    }

    fn definition(&self) -> &UdfDefinition {
        &self.definition
    }
}

/// UDF/UDA manager with an in-process native function registry.
#[derive(Debug, Default)]
pub struct UdfManager {
    udfs: HashMap<String, Arc<dyn UserDefinedFunction>>,
    udas: HashMap<String, UdaDefinition>,
}

impl UdfManager {
    pub fn new() -> Self {
        Self {
            udfs: HashMap::new(),
            udas: HashMap::new(),
        }
    }

    /// Register a UDF in the native registry.
    pub fn register_function(&mut self, def: UdfDefinition) -> Result<(), String> {
        validate_native_body(&def)?;
        self.udfs.insert(
            function_key(&def.keyspace, &def.name, &def.arg_types),
            Arc::new(NativeUdf { definition: def }),
        );
        Ok(())
    }

    /// Register a UDA. Its state/final functions are resolved at execution time.
    pub fn register_aggregate(&mut self, def: UdaDefinition) -> Result<(), String> {
        self.udas
            .insert(function_key(&def.keyspace, &def.name, &def.arg_types), def);
        Ok(())
    }

    pub fn execute_function(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[String],
        args: &[Option<Vec<u8>>],
    ) -> Result<Option<Vec<u8>>, String> {
        let key = function_key(keyspace, name, arg_types);
        self.udfs
            .get(&key)
            .ok_or_else(|| format!("UDF not found: {key}"))?
            .execute(args)
    }

    pub fn execute_aggregate(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[String],
        rows: &[Vec<Option<Vec<u8>>>],
    ) -> Result<Option<Vec<u8>>, String> {
        let key = function_key(keyspace, name, arg_types);
        let aggregate = self
            .udas
            .get(&key)
            .ok_or_else(|| format!("UDA not found: {key}"))?;
        let mut state = aggregate
            .init_cond
            .as_ref()
            .map(|value| encode_literal(&aggregate.state_type, value))
            .transpose()?;

        for row in rows {
            let mut state_args = Vec::with_capacity(row.len() + 1);
            state_args.push(state);
            state_args.extend(row.iter().cloned());
            state = self.execute_function(
                keyspace,
                &aggregate.state_func,
                &state_function_arg_types(aggregate),
                &state_args,
            )?;
        }

        if let Some(final_func) = &aggregate.final_func {
            self.execute_function(
                keyspace,
                final_func,
                std::slice::from_ref(&aggregate.state_type),
                &[state],
            )
        } else {
            Ok(state)
        }
    }
}

fn function_key(keyspace: &str, name: &str, arg_types: &[String]) -> String {
    format!("{}.{}({})", keyspace, name, arg_types.join(","))
}

fn state_function_arg_types(aggregate: &UdaDefinition) -> Vec<String> {
    let mut arg_types = Vec::with_capacity(aggregate.arg_types.len() + 1);
    arg_types.push(aggregate.state_type.clone());
    arg_types.extend(aggregate.arg_types.iter().cloned());
    arg_types
}

fn validate_native_body(def: &UdfDefinition) -> Result<(), String> {
    let language = def.language.to_ascii_lowercase();
    if language != "rust" && language != "native" && language != "java" {
        return Err(format!("Unsupported UDF language: {}", def.language));
    }
    let body = canonical_body(&def.body, &def.return_type, &def.arg_types);
    match body.as_str() {
        "identity"
        | "concat"
        | "lower"
        | "upper"
        | "trim"
        | "java_trim"
        | "trim_start"
        | "trim_end"
        | "add_i32"
        | "add_i64"
        | "add_f32"
        | "add_f64"
        | "sub_i32"
        | "sub_i64"
        | "sub_f32"
        | "sub_f64"
        | "mul_i32"
        | "mul_i64"
        | "mul_f32"
        | "mul_f64"
        | "div_i32"
        | "div_i64"
        | "div_f32"
        | "div_f64"
        | "rem_i32"
        | "rem_i64"
        | "rem_f32"
        | "rem_f64"
        | "neg_i32"
        | "neg_i64"
        | "neg_f32"
        | "neg_f64"
        | "inc_exact_i32"
        | "inc_exact_i64"
        | "dec_exact_i32"
        | "dec_exact_i64"
        | "neg_exact_i32"
        | "neg_exact_i64"
        | "add_exact_i32_args"
        | "add_exact_i64_args"
        | "sub_exact_i32_args"
        | "sub_exact_i64_args"
        | "mul_exact_i32_args"
        | "mul_exact_i64_args"
        | "floor_div_i32_args"
        | "floor_div_i64_args"
        | "floor_mod_i32_args"
        | "floor_mod_i64_args"
        | "to_i32_exact"
        | "abs_i32"
        | "abs_i64"
        | "abs_f32"
        | "abs_f64"
        | "max_i32_args"
        | "max_i64_args"
        | "max_f32_args"
        | "max_f64_args"
        | "min_i32_args"
        | "min_i64_args"
        | "min_f32_args"
        | "min_f64_args"
        | "sqrt_f64"
        | "floor_f64"
        | "ceil_f64"
        | "next_up_f32"
        | "next_down_f32"
        | "ulp_f32"
        | "rint_f64"
        | "next_up_f64"
        | "next_down_f64"
        | "ulp_f64"
        | "log_f64"
        | "log10_f64"
        | "exp_f64"
        | "expm1_f64"
        | "log1p_f64"
        | "sin_f64"
        | "cos_f64"
        | "tan_f64"
        | "sinh_f64"
        | "cosh_f64"
        | "tanh_f64"
        | "asin_f64"
        | "acos_f64"
        | "atan_f64"
        | "cbrt_f64"
        | "to_radians_f64"
        | "to_degrees_f64"
        | "signum_f32"
        | "signum_f64"
        | "round_f32_to_i32"
        | "round_f64_to_i64"
        | "get_exponent_f32_to_i32"
        | "get_exponent_f64_to_i32"
        | "mul_full_i32_args"
        | "mul_high_i64_args"
        | "next_after_f32_args"
        | "next_after_f64_args"
        | "fma_f32_args"
        | "fma_f64_args"
        | "pow_f64_args"
        | "hypot_f64_args"
        | "atan2_f64_args"
        | "copysign_f32_args"
        | "copysign_f64_args"
        | "is_nan_f32"
        | "is_nan_f64"
        | "is_infinite_f32"
        | "is_infinite_f64"
        | "is_finite_f32"
        | "is_finite_f64"
        | "float_nan_const"
        | "float_pos_inf_const"
        | "float_neg_inf_const"
        | "float_max_value_const"
        | "float_min_value_const"
        | "float_min_normal_const"
        | "double_nan_const"
        | "double_pos_inf_const"
        | "double_neg_inf_const"
        | "double_max_value_const"
        | "double_min_value_const"
        | "double_min_normal_const"
        | "float_sum_args"
        | "float_max_args"
        | "float_min_args"
        | "double_sum_args"
        | "double_max_args"
        | "double_min_args"
        | "f32_to_i32_bits"
        | "f32_to_raw_i32_bits"
        | "i32_bits_to_f32"
        | "f64_to_i64_bits"
        | "f64_to_raw_i64_bits"
        | "i64_bits_to_f64"
        | "not_bool"
        | "bool_and"
        | "bool_or"
        | "bool_xor"
        | "to_string_i8"
        | "to_string_i16"
        | "to_string_i32"
        | "to_string_i64"
        | "to_string_f32"
        | "to_string_f64"
        | "to_string_bool"
        | "to_string_i32_radix_args"
        | "to_string_i64_radix_args"
        | "to_unsigned_int_i8"
        | "to_unsigned_long_i8"
        | "to_unsigned_int_i16"
        | "to_unsigned_long_i16"
        | "to_unsigned_long_i32"
        | "to_unsigned_string_i32"
        | "to_unsigned_string_i64"
        | "to_unsigned_string_i32_radix_args"
        | "to_unsigned_string_i64_radix_args"
        | "to_binary_string_i32"
        | "to_binary_string_i64"
        | "to_octal_string_i32"
        | "to_octal_string_i64"
        | "to_hex_string_i32"
        | "to_hex_string_i64"
        | "bit_count_i32"
        | "bit_count_i64"
        | "leading_zeros_i32"
        | "leading_zeros_i64"
        | "trailing_zeros_i32"
        | "trailing_zeros_i64"
        | "highest_one_bit_i32"
        | "highest_one_bit_i64"
        | "lowest_one_bit_i32"
        | "lowest_one_bit_i64"
        | "reverse_bits_i32"
        | "reverse_bits_i64"
        | "reverse_bytes_i32"
        | "reverse_bytes_i64"
        | "rotate_left_i32"
        | "rotate_left_i64"
        | "rotate_right_i32"
        | "rotate_right_i64"
        | "int_sum_args"
        | "int_max_args"
        | "int_min_args"
        | "int_divide_unsigned_args"
        | "int_remainder_unsigned_args"
        | "int_signum"
        | "long_sum_args"
        | "long_max_args"
        | "long_min_args"
        | "long_divide_unsigned_args"
        | "long_remainder_unsigned_args"
        | "long_signum"
        | "java_compare_i8"
        | "java_compare_i16"
        | "java_compare_i32"
        | "java_compare_i64"
        | "java_compare_bool"
        | "java_compare_f32"
        | "java_compare_f64"
        | "java_compare_unsigned_i32"
        | "java_compare_unsigned_i64"
        | "boxed_equals_f32_args"
        | "boxed_not_equals_f32_args"
        | "boxed_equals_f64_args"
        | "boxed_not_equals_f64_args"
        | "java_hash_i8"
        | "java_hash_i16"
        | "java_hash_i32"
        | "java_hash_i64"
        | "java_hash_bool"
        | "java_hash_f32"
        | "java_hash_f64"
        | "java_hash_text"
        | "objects_is_null"
        | "objects_non_null"
        | "objects_require_non_null"
        | "objects_require_non_null_else_args"
        | "objects_to_string_default_text_args"
        | "parse_bool_text"
        | "parse_i8_text"
        | "parse_i16_text"
        | "parse_i32_text"
        | "parse_i64_text"
        | "parse_unsigned_i32_text"
        | "parse_unsigned_i64_text"
        | "parse_unsigned_i32_text_radix_args"
        | "parse_unsigned_i64_text_radix_args"
        | "decode_i8_text"
        | "decode_i16_text"
        | "decode_i32_text"
        | "decode_i64_text"
        | "parse_f32_text"
        | "parse_f64_text"
        | "text_length"
        | "text_is_empty"
        | "text_is_blank" => Ok(()),
        body if body.starts_with("constant:")
            || body.starts_with("add_i32_literal:")
            || body.starts_with("add_i64_literal:")
            || body.starts_with("add_f32_literal:")
            || body.starts_with("add_f64_literal:")
            || body.starts_with("sub_i32_literal:")
            || body.starts_with("sub_i64_literal:")
            || body.starts_with("sub_f32_literal:")
            || body.starts_with("sub_f64_literal:")
            || body.starts_with("mul_i32_literal:")
            || body.starts_with("mul_i64_literal:")
            || body.starts_with("mul_f32_literal:")
            || body.starts_with("mul_f64_literal:")
            || body.starts_with("div_i32_literal:")
            || body.starts_with("div_i64_literal:")
            || body.starts_with("div_f32_literal:")
            || body.starts_with("div_f64_literal:")
            || body.starts_with("rem_i32_literal:")
            || body.starts_with("rem_i64_literal:")
            || body.starts_with("rem_f32_literal:")
            || body.starts_with("rem_f64_literal:")
            || body.starts_with("concat_prefix:")
            || body.starts_with("concat_suffix:")
            || body.starts_with("cmp_i32_literal:")
            || body.starts_with("cmp_i64_literal:")
            || body.starts_with("cmp_f32_literal:")
            || body.starts_with("cmp_f64_literal:")
            || body.starts_with("cmp_i32_args:")
            || body.starts_with("cmp_i64_args:")
            || body.starts_with("cmp_f32_args:")
            || body.starts_with("cmp_f64_args:")
            || body.starts_with("cmp_bool_args:")
            || body.starts_with("text_eq_literal:")
            || body.starts_with("text_ne_literal:")
            || body.starts_with("text_equals_ignore_case_literal:")
            || body.starts_with("text_compare_to_literal:")
            || body.starts_with("text_compare_to_ignore_case_literal:")
            || body.starts_with("text_eq_args")
            || body.starts_with("text_ne_args")
            || body.starts_with("text_equals_ignore_case_args")
            || body.starts_with("text_compare_to_args")
            || body.starts_with("text_compare_to_ignore_case_args")
            || body.starts_with("bool_eq_literal:")
            || body.starts_with("bool_ne_literal:")
            || body.starts_with("text_contains_literal:")
            || body.starts_with("text_starts_with_literal:")
            || body.starts_with("text_starts_with_literal_from:")
            || body.starts_with("text_ends_with_literal:")
            || body.starts_with("text_contains_args")
            || body.starts_with("text_starts_with_args")
            || body.starts_with("text_starts_with_from_args")
            || body.starts_with("text_ends_with_args")
            || body.starts_with("text_char_at_literal:")
            || body.starts_with("text_char_at_arg")
            || body.starts_with("text_code_point_at_literal:")
            || body.starts_with("text_code_point_at_arg")
            || body.starts_with("text_code_point_before_literal:")
            || body.starts_with("text_code_point_before_arg")
            || body.starts_with("text_code_point_count_literal:")
            || body.starts_with("text_code_point_count_args")
            || body.starts_with("text_offset_by_code_points_literal:")
            || body.starts_with("text_offset_by_code_points_args")
            || body.starts_with("objects_require_non_null_else_text_literal:")
            || body.starts_with("objects_to_string_default_text_literal:")
            || body.starts_with("text_substring_from:")
            || body.starts_with("text_substring_range:")
            || body.starts_with("text_substring_from_arg")
            || body.starts_with("text_substring_range_args")
            || body.starts_with("text_repeat_arg")
            || body.starts_with("text_index_of_literal:")
            || body.starts_with("text_last_index_of_literal:")
            || body.starts_with("text_index_of_literal_from:")
            || body.starts_with("text_last_index_of_literal_from:")
            || body.starts_with("text_index_of_args")
            || body.starts_with("text_last_index_of_args")
            || body.starts_with("text_index_of_from_args")
            || body.starts_with("text_last_index_of_from_args")
            || body.starts_with("text_replace_literals_len:")
            || body.starts_with("text_replace_literals:")
            || body.starts_with("text_replace_args")
            || body.starts_with("parse_i8_text_radix:")
            || body.starts_with("parse_i16_text_radix:")
            || body.starts_with("parse_i32_text_radix:")
            || body.starts_with("parse_i64_text_radix:")
            || body.starts_with("parse_unsigned_i32_text_radix:")
            || body.starts_with("parse_unsigned_i64_text_radix:")
            || body.starts_with("boxed_numeric_conversion:")
            || body.starts_with("to_string_i32_radix:")
            || body.starts_with("to_string_i64_radix:")
            || body.starts_with("to_unsigned_string_i32_radix:")
            || body.starts_with("to_unsigned_string_i64_radix:") =>
        {
            Ok(())
        }
        other => Err(format!("Unsupported native UDF body: {other}")),
    }
}

fn execute_native_body(
    body: &str,
    return_type: &str,
    arg_types: &[String],
    args: &[Option<Vec<u8>>],
) -> Result<Option<Vec<u8>>, String> {
    let body = canonical_body(body, return_type, arg_types);
    match body.as_str() {
        "identity" => Ok(args.first().cloned().unwrap_or(None)),
        "concat" => {
            let mut out = Vec::new();
            for arg in args.iter().flatten() {
                out.extend(arg);
            }
            Ok(Some(out))
        }
        "lower" => map_utf8(args.first().cloned().unwrap_or(None), |s| s.to_lowercase()),
        "upper" => map_utf8(args.first().cloned().unwrap_or(None), |s| s.to_uppercase()),
        "java_trim" => map_utf8(args.first().cloned().unwrap_or(None), |s| {
            java_text_trim(&s)
        }),
        "trim" => map_utf8(args.first().cloned().unwrap_or(None), |s| {
            s.trim().to_string()
        }),
        "trim_start" => map_utf8(args.first().cloned().unwrap_or(None), |s| {
            s.trim_start().to_string()
        }),
        "trim_end" => map_utf8(args.first().cloned().unwrap_or(None), |s| {
            s.trim_end().to_string()
        }),
        "add_i32" => Ok(Some(sum_i32(args)?.to_be_bytes().to_vec())),
        "add_i64" => Ok(Some(sum_i64(args)?.to_be_bytes().to_vec())),
        "add_f32" => Ok(Some(sum_f32(args)?.to_be_bytes().to_vec())),
        "add_f64" => Ok(Some(sum_f64(args)?.to_be_bytes().to_vec())),
        "sub_i32" => Ok(Some(sub_i32(args)?.to_be_bytes().to_vec())),
        "sub_i64" => Ok(Some(sub_i64(args)?.to_be_bytes().to_vec())),
        "sub_f32" => Ok(Some(sub_f32(args)?.to_be_bytes().to_vec())),
        "sub_f64" => Ok(Some(sub_f64(args)?.to_be_bytes().to_vec())),
        "mul_i32" => Ok(Some(mul_i32(args)?.to_be_bytes().to_vec())),
        "mul_i64" => Ok(Some(mul_i64(args)?.to_be_bytes().to_vec())),
        "mul_f32" => Ok(Some(mul_f32(args)?.to_be_bytes().to_vec())),
        "mul_f64" => Ok(Some(mul_f64(args)?.to_be_bytes().to_vec())),
        "div_i32" => Ok(Some(div_i32(args)?.to_be_bytes().to_vec())),
        "div_i64" => Ok(Some(div_i64(args)?.to_be_bytes().to_vec())),
        "div_f32" => Ok(Some(div_f32(args)?.to_be_bytes().to_vec())),
        "div_f64" => Ok(Some(div_f64(args)?.to_be_bytes().to_vec())),
        "rem_i32" => Ok(Some(rem_i32(args)?.to_be_bytes().to_vec())),
        "rem_i64" => Ok(Some(rem_i64(args)?.to_be_bytes().to_vec())),
        "rem_f32" => Ok(Some(rem_f32(args)?.to_be_bytes().to_vec())),
        "rem_f64" => Ok(Some(rem_f64(args)?.to_be_bytes().to_vec())),
        "neg_i32" => Ok(Some(neg_i32(args)?.to_be_bytes().to_vec())),
        "neg_i64" => Ok(Some(neg_i64(args)?.to_be_bytes().to_vec())),
        "neg_f32" => Ok(Some(neg_f32(args)?.to_be_bytes().to_vec())),
        "neg_f64" => Ok(Some(neg_f64(args)?.to_be_bytes().to_vec())),
        "inc_exact_i32" => Ok(Some(inc_exact_i32(args)?.to_be_bytes().to_vec())),
        "inc_exact_i64" => Ok(Some(inc_exact_i64(args)?.to_be_bytes().to_vec())),
        "dec_exact_i32" => Ok(Some(dec_exact_i32(args)?.to_be_bytes().to_vec())),
        "dec_exact_i64" => Ok(Some(dec_exact_i64(args)?.to_be_bytes().to_vec())),
        "neg_exact_i32" => Ok(Some(neg_exact_i32(args)?.to_be_bytes().to_vec())),
        "neg_exact_i64" => Ok(Some(neg_exact_i64(args)?.to_be_bytes().to_vec())),
        "add_exact_i32_args" => Ok(Some(add_exact_i32_args(args)?.to_be_bytes().to_vec())),
        "add_exact_i64_args" => Ok(Some(add_exact_i64_args(args)?.to_be_bytes().to_vec())),
        "sub_exact_i32_args" => Ok(Some(sub_exact_i32_args(args)?.to_be_bytes().to_vec())),
        "sub_exact_i64_args" => Ok(Some(sub_exact_i64_args(args)?.to_be_bytes().to_vec())),
        "mul_exact_i32_args" => Ok(Some(mul_exact_i32_args(args)?.to_be_bytes().to_vec())),
        "mul_exact_i64_args" => Ok(Some(mul_exact_i64_args(args)?.to_be_bytes().to_vec())),
        "floor_div_i32_args" => Ok(Some(floor_div_i32_args(args)?.to_be_bytes().to_vec())),
        "floor_div_i64_args" => Ok(Some(floor_div_i64_args(args)?.to_be_bytes().to_vec())),
        "floor_mod_i32_args" => Ok(Some(floor_mod_i32_args(args)?.to_be_bytes().to_vec())),
        "floor_mod_i64_args" => Ok(Some(floor_mod_i64_args(args)?.to_be_bytes().to_vec())),
        "to_i32_exact" => Ok(Some(to_i32_exact(args)?.to_be_bytes().to_vec())),
        "abs_i32" => Ok(Some(abs_i32(args)?.to_be_bytes().to_vec())),
        "abs_i64" => Ok(Some(abs_i64(args)?.to_be_bytes().to_vec())),
        "abs_f32" => Ok(Some(abs_f32(args)?.to_be_bytes().to_vec())),
        "abs_f64" => Ok(Some(abs_f64(args)?.to_be_bytes().to_vec())),
        "max_i32_args" => Ok(Some(max_i32_args(args)?.to_be_bytes().to_vec())),
        "max_i64_args" => Ok(Some(max_i64_args(args)?.to_be_bytes().to_vec())),
        "max_f32_args" => Ok(Some(max_f32_args(args)?.to_be_bytes().to_vec())),
        "max_f64_args" => Ok(Some(max_f64_args(args)?.to_be_bytes().to_vec())),
        "min_i32_args" => Ok(Some(min_i32_args(args)?.to_be_bytes().to_vec())),
        "min_i64_args" => Ok(Some(min_i64_args(args)?.to_be_bytes().to_vec())),
        "min_f32_args" => Ok(Some(min_f32_args(args)?.to_be_bytes().to_vec())),
        "min_f64_args" => Ok(Some(min_f64_args(args)?.to_be_bytes().to_vec())),
        "sqrt_f64" => Ok(Some(sqrt_f64(args)?.to_be_bytes().to_vec())),
        "floor_f64" => Ok(Some(floor_f64(args)?.to_be_bytes().to_vec())),
        "ceil_f64" => Ok(Some(ceil_f64(args)?.to_be_bytes().to_vec())),
        "next_up_f32" => Ok(Some(next_up_f32(args)?.to_be_bytes().to_vec())),
        "next_down_f32" => Ok(Some(next_down_f32(args)?.to_be_bytes().to_vec())),
        "ulp_f32" => Ok(Some(ulp_f32(args)?.to_be_bytes().to_vec())),
        "rint_f64" => Ok(Some(rint_f64(args)?.to_be_bytes().to_vec())),
        "next_up_f64" => Ok(Some(next_up_f64(args)?.to_be_bytes().to_vec())),
        "next_down_f64" => Ok(Some(next_down_f64(args)?.to_be_bytes().to_vec())),
        "ulp_f64" => Ok(Some(ulp_f64(args)?.to_be_bytes().to_vec())),
        "log_f64" => Ok(Some(log_f64(args)?.to_be_bytes().to_vec())),
        "log10_f64" => Ok(Some(log10_f64(args)?.to_be_bytes().to_vec())),
        "exp_f64" => Ok(Some(exp_f64(args)?.to_be_bytes().to_vec())),
        "expm1_f64" => Ok(Some(expm1_f64(args)?.to_be_bytes().to_vec())),
        "log1p_f64" => Ok(Some(log1p_f64(args)?.to_be_bytes().to_vec())),
        "sin_f64" => Ok(Some(sin_f64(args)?.to_be_bytes().to_vec())),
        "cos_f64" => Ok(Some(cos_f64(args)?.to_be_bytes().to_vec())),
        "tan_f64" => Ok(Some(tan_f64(args)?.to_be_bytes().to_vec())),
        "sinh_f64" => Ok(Some(sinh_f64(args)?.to_be_bytes().to_vec())),
        "cosh_f64" => Ok(Some(cosh_f64(args)?.to_be_bytes().to_vec())),
        "tanh_f64" => Ok(Some(tanh_f64(args)?.to_be_bytes().to_vec())),
        "asin_f64" => Ok(Some(asin_f64(args)?.to_be_bytes().to_vec())),
        "acos_f64" => Ok(Some(acos_f64(args)?.to_be_bytes().to_vec())),
        "atan_f64" => Ok(Some(atan_f64(args)?.to_be_bytes().to_vec())),
        "cbrt_f64" => Ok(Some(cbrt_f64(args)?.to_be_bytes().to_vec())),
        "to_radians_f64" => Ok(Some(to_radians_f64(args)?.to_be_bytes().to_vec())),
        "to_degrees_f64" => Ok(Some(to_degrees_f64(args)?.to_be_bytes().to_vec())),
        "signum_f32" => Ok(Some(signum_f32(args)?.to_be_bytes().to_vec())),
        "signum_f64" => Ok(Some(signum_f64(args)?.to_be_bytes().to_vec())),
        "round_f32_to_i32" => Ok(Some(round_f32_to_i32(args)?.to_be_bytes().to_vec())),
        "round_f64_to_i64" => Ok(Some(round_f64_to_i64(args)?.to_be_bytes().to_vec())),
        "get_exponent_f32_to_i32" => {
            Ok(Some(get_exponent_f32_to_i32(args)?.to_be_bytes().to_vec()))
        }
        "get_exponent_f64_to_i32" => {
            Ok(Some(get_exponent_f64_to_i32(args)?.to_be_bytes().to_vec()))
        }
        "mul_full_i32_args" => Ok(Some(mul_full_i32_args(args)?.to_be_bytes().to_vec())),
        "mul_high_i64_args" => Ok(Some(mul_high_i64_args(args)?.to_be_bytes().to_vec())),
        "next_after_f32_args" => Ok(Some(next_after_f32_args(args)?.to_be_bytes().to_vec())),
        "next_after_f64_args" => Ok(Some(next_after_f64_args(args)?.to_be_bytes().to_vec())),
        "fma_f32_args" => Ok(Some(fma_f32_args(args)?.to_be_bytes().to_vec())),
        "fma_f64_args" => Ok(Some(fma_f64_args(args)?.to_be_bytes().to_vec())),
        "pow_f64_args" => Ok(Some(pow_f64_args(args)?.to_be_bytes().to_vec())),
        "hypot_f64_args" => Ok(Some(hypot_f64_args(args)?.to_be_bytes().to_vec())),
        "atan2_f64_args" => Ok(Some(atan2_f64_args(args)?.to_be_bytes().to_vec())),
        "copysign_f32_args" => Ok(Some(copysign_f32_args(args)?.to_be_bytes().to_vec())),
        "copysign_f64_args" => Ok(Some(copysign_f64_args(args)?.to_be_bytes().to_vec())),
        "is_nan_f32" => Ok(Some(boolean_bytes(is_nan_f32(args)?))),
        "is_nan_f64" => Ok(Some(boolean_bytes(is_nan_f64(args)?))),
        "is_infinite_f32" => Ok(Some(boolean_bytes(is_infinite_f32(args)?))),
        "is_infinite_f64" => Ok(Some(boolean_bytes(is_infinite_f64(args)?))),
        "is_finite_f32" => Ok(Some(boolean_bytes(is_finite_f32(args)?))),
        "is_finite_f64" => Ok(Some(boolean_bytes(is_finite_f64(args)?))),
        "float_nan_const" => Ok(Some(f32::NAN.to_be_bytes().to_vec())),
        "float_pos_inf_const" => Ok(Some(f32::INFINITY.to_be_bytes().to_vec())),
        "float_neg_inf_const" => Ok(Some(f32::NEG_INFINITY.to_be_bytes().to_vec())),
        "float_max_value_const" => Ok(Some(f32::MAX.to_be_bytes().to_vec())),
        "float_min_value_const" => Ok(Some(f32::from_bits(1).to_be_bytes().to_vec())),
        "float_min_normal_const" => Ok(Some(f32::MIN_POSITIVE.to_be_bytes().to_vec())),
        "double_nan_const" => Ok(Some(f64::NAN.to_be_bytes().to_vec())),
        "double_pos_inf_const" => Ok(Some(f64::INFINITY.to_be_bytes().to_vec())),
        "double_neg_inf_const" => Ok(Some(f64::NEG_INFINITY.to_be_bytes().to_vec())),
        "double_max_value_const" => Ok(Some(f64::MAX.to_be_bytes().to_vec())),
        "double_min_value_const" => Ok(Some(f64::from_bits(1).to_be_bytes().to_vec())),
        "double_min_normal_const" => Ok(Some(f64::MIN_POSITIVE.to_be_bytes().to_vec())),
        "float_sum_args" => Ok(Some(float_sum_args(args)?.to_be_bytes().to_vec())),
        "float_max_args" => Ok(Some(float_max_args(args)?.to_be_bytes().to_vec())),
        "float_min_args" => Ok(Some(float_min_args(args)?.to_be_bytes().to_vec())),
        "double_sum_args" => Ok(Some(double_sum_args(args)?.to_be_bytes().to_vec())),
        "double_max_args" => Ok(Some(double_max_args(args)?.to_be_bytes().to_vec())),
        "double_min_args" => Ok(Some(double_min_args(args)?.to_be_bytes().to_vec())),
        "f32_to_i32_bits" => Ok(Some(f32_to_i32_bits(args)?.to_be_bytes().to_vec())),
        "f32_to_raw_i32_bits" => Ok(Some(f32_to_raw_i32_bits(args)?.to_be_bytes().to_vec())),
        "i32_bits_to_f32" => Ok(Some(i32_bits_to_f32(args)?.to_be_bytes().to_vec())),
        "f64_to_i64_bits" => Ok(Some(f64_to_i64_bits(args)?.to_be_bytes().to_vec())),
        "f64_to_raw_i64_bits" => Ok(Some(f64_to_raw_i64_bits(args)?.to_be_bytes().to_vec())),
        "i64_bits_to_f64" => Ok(Some(i64_bits_to_f64(args)?.to_be_bytes().to_vec())),
        "not_bool" => Ok(Some(boolean_bytes(not_bool(args)?))),
        "bool_and" => Ok(Some(boolean_bytes(boolean_binary(args, |lhs, rhs| {
            lhs && rhs
        })?))),
        "bool_or" => Ok(Some(boolean_bytes(boolean_binary(args, |lhs, rhs| {
            lhs || rhs
        })?))),
        "bool_xor" => Ok(Some(boolean_bytes(boolean_binary(args, |lhs, rhs| {
            lhs ^ rhs
        })?))),
        "to_string_i8" => to_string_i8(args),
        "to_string_i16" => to_string_i16(args),
        "to_string_i32" => to_string_i32(args),
        "to_string_i64" => to_string_i64(args),
        "to_string_f32" => to_string_f32(args),
        "to_string_f64" => to_string_f64(args),
        "to_string_bool" => to_string_bool(args),
        "to_string_i32_radix_args" => to_string_i32_radix_args(args),
        "to_string_i64_radix_args" => to_string_i64_radix_args(args),
        "to_unsigned_int_i8" => Ok(Some(to_unsigned_int_i8(args)?.to_be_bytes().to_vec())),
        "to_unsigned_long_i8" => Ok(Some(to_unsigned_long_i8(args)?.to_be_bytes().to_vec())),
        "to_unsigned_int_i16" => Ok(Some(to_unsigned_int_i16(args)?.to_be_bytes().to_vec())),
        "to_unsigned_long_i16" => Ok(Some(to_unsigned_long_i16(args)?.to_be_bytes().to_vec())),
        "to_unsigned_long_i32" => Ok(Some(to_unsigned_long_i32(args)?.to_be_bytes().to_vec())),
        "to_unsigned_string_i32" => to_unsigned_string_i32(args),
        "to_unsigned_string_i64" => to_unsigned_string_i64(args),
        "to_unsigned_string_i32_radix_args" => to_unsigned_string_i32_radix_args(args),
        "to_unsigned_string_i64_radix_args" => to_unsigned_string_i64_radix_args(args),
        "to_binary_string_i32" => to_binary_string_i32(args),
        "to_binary_string_i64" => to_binary_string_i64(args),
        "to_octal_string_i32" => to_octal_string_i32(args),
        "to_octal_string_i64" => to_octal_string_i64(args),
        "to_hex_string_i32" => to_hex_string_i32(args),
        "to_hex_string_i64" => to_hex_string_i64(args),
        "bit_count_i32" => Ok(Some(bit_count_i32(args)?.to_be_bytes().to_vec())),
        "bit_count_i64" => Ok(Some(bit_count_i64(args)?.to_be_bytes().to_vec())),
        "leading_zeros_i32" => Ok(Some(leading_zeros_i32(args)?.to_be_bytes().to_vec())),
        "leading_zeros_i64" => Ok(Some(leading_zeros_i64(args)?.to_be_bytes().to_vec())),
        "trailing_zeros_i32" => Ok(Some(trailing_zeros_i32(args)?.to_be_bytes().to_vec())),
        "trailing_zeros_i64" => Ok(Some(trailing_zeros_i64(args)?.to_be_bytes().to_vec())),
        "highest_one_bit_i32" => Ok(Some(highest_one_bit_i32(args)?.to_be_bytes().to_vec())),
        "highest_one_bit_i64" => Ok(Some(highest_one_bit_i64(args)?.to_be_bytes().to_vec())),
        "lowest_one_bit_i32" => Ok(Some(lowest_one_bit_i32(args)?.to_be_bytes().to_vec())),
        "lowest_one_bit_i64" => Ok(Some(lowest_one_bit_i64(args)?.to_be_bytes().to_vec())),
        "reverse_bits_i32" => Ok(Some(reverse_bits_i32(args)?.to_be_bytes().to_vec())),
        "reverse_bits_i64" => Ok(Some(reverse_bits_i64(args)?.to_be_bytes().to_vec())),
        "reverse_bytes_i32" => Ok(Some(reverse_bytes_i32(args)?.to_be_bytes().to_vec())),
        "reverse_bytes_i64" => Ok(Some(reverse_bytes_i64(args)?.to_be_bytes().to_vec())),
        "rotate_left_i32" => Ok(Some(rotate_left_i32(args)?.to_be_bytes().to_vec())),
        "rotate_left_i64" => Ok(Some(rotate_left_i64(args)?.to_be_bytes().to_vec())),
        "rotate_right_i32" => Ok(Some(rotate_right_i32(args)?.to_be_bytes().to_vec())),
        "rotate_right_i64" => Ok(Some(rotate_right_i64(args)?.to_be_bytes().to_vec())),
        "int_sum_args" => Ok(Some(int_sum_args(args)?.to_be_bytes().to_vec())),
        "int_max_args" => Ok(Some(int_max_args(args)?.to_be_bytes().to_vec())),
        "int_min_args" => Ok(Some(int_min_args(args)?.to_be_bytes().to_vec())),
        "int_divide_unsigned_args" => {
            Ok(Some(int_divide_unsigned_args(args)?.to_be_bytes().to_vec()))
        }
        "int_remainder_unsigned_args" => Ok(Some(
            int_remainder_unsigned_args(args)?.to_be_bytes().to_vec(),
        )),
        "int_signum" => Ok(Some(int_signum(args)?.to_be_bytes().to_vec())),
        "long_sum_args" => Ok(Some(long_sum_args(args)?.to_be_bytes().to_vec())),
        "long_max_args" => Ok(Some(long_max_args(args)?.to_be_bytes().to_vec())),
        "long_min_args" => Ok(Some(long_min_args(args)?.to_be_bytes().to_vec())),
        "long_divide_unsigned_args" => Ok(Some(
            long_divide_unsigned_args(args)?.to_be_bytes().to_vec(),
        )),
        "long_remainder_unsigned_args" => Ok(Some(
            long_remainder_unsigned_args(args)?.to_be_bytes().to_vec(),
        )),
        "long_signum" => Ok(Some(long_signum(args)?.to_be_bytes().to_vec())),
        "java_compare_i8" => Ok(Some(java_compare_i8(args)?.to_be_bytes().to_vec())),
        "java_compare_i16" => Ok(Some(java_compare_i16(args)?.to_be_bytes().to_vec())),
        "java_compare_i32" => Ok(Some(java_compare_i32(args)?.to_be_bytes().to_vec())),
        "java_compare_i64" => Ok(Some(java_compare_i64(args)?.to_be_bytes().to_vec())),
        "java_compare_bool" => Ok(Some(java_compare_bool(args)?.to_be_bytes().to_vec())),
        "java_compare_f32" => Ok(Some(java_compare_f32(args)?.to_be_bytes().to_vec())),
        "java_compare_f64" => Ok(Some(java_compare_f64(args)?.to_be_bytes().to_vec())),
        "java_compare_unsigned_i32" => Ok(Some(
            java_compare_unsigned_i32(args)?.to_be_bytes().to_vec(),
        )),
        "java_compare_unsigned_i64" => Ok(Some(
            java_compare_unsigned_i64(args)?.to_be_bytes().to_vec(),
        )),
        "boxed_equals_f32_args" => Ok(Some(boolean_bytes(boxed_equals_f32_args(args)?))),
        "boxed_not_equals_f32_args" => Ok(Some(boolean_bytes(!boxed_equals_f32_args(args)?))),
        "boxed_equals_f64_args" => Ok(Some(boolean_bytes(boxed_equals_f64_args(args)?))),
        "boxed_not_equals_f64_args" => Ok(Some(boolean_bytes(!boxed_equals_f64_args(args)?))),
        "java_hash_i8" => Ok(Some(java_hash_i8(args)?.to_be_bytes().to_vec())),
        "java_hash_i16" => Ok(Some(java_hash_i16(args)?.to_be_bytes().to_vec())),
        "java_hash_i32" => Ok(Some(java_hash_i32(args)?.to_be_bytes().to_vec())),
        "java_hash_i64" => Ok(Some(java_hash_i64(args)?.to_be_bytes().to_vec())),
        "java_hash_bool" => Ok(Some(java_hash_bool(args)?.to_be_bytes().to_vec())),
        "java_hash_f32" => Ok(Some(java_hash_f32(args)?.to_be_bytes().to_vec())),
        "java_hash_f64" => Ok(Some(java_hash_f64(args)?.to_be_bytes().to_vec())),
        "java_hash_text" => Ok(Some(java_hash_text(args)?.to_be_bytes().to_vec())),
        "objects_is_null" => Ok(Some(boolean_bytes(object_is_null(args)))),
        "objects_non_null" => Ok(Some(boolean_bytes(!object_is_null(args)))),
        "objects_require_non_null" => objects_require_non_null(args),
        "objects_require_non_null_else_args" => objects_require_non_null_else_args(args),
        body if body.starts_with("objects_require_non_null_else_text_literal:") => {
            objects_require_non_null_else_text_literal(
                args,
                body.trim_start_matches("objects_require_non_null_else_text_literal:"),
            )
        }
        "objects_to_string_default_text_args" => objects_to_string_default_text_args(args),
        body if body.starts_with("objects_to_string_default_text_literal:") => {
            objects_to_string_default_text_literal(
                args,
                body.trim_start_matches("objects_to_string_default_text_literal:"),
            )
        }
        "parse_bool_text" => Ok(Some(boolean_bytes(parse_bool_text(args)?))),
        "parse_i8_text" => Ok(Some(parse_i8_text(args)?.to_be_bytes().to_vec())),
        "parse_i16_text" => Ok(Some(parse_i16_text(args)?.to_be_bytes().to_vec())),
        "parse_i32_text" => Ok(Some(parse_i32_text(args)?.to_be_bytes().to_vec())),
        "parse_i64_text" => Ok(Some(parse_i64_text(args)?.to_be_bytes().to_vec())),
        "parse_unsigned_i32_text" => {
            Ok(Some(parse_unsigned_i32_text(args)?.to_be_bytes().to_vec()))
        }
        "parse_unsigned_i64_text" => {
            Ok(Some(parse_unsigned_i64_text(args)?.to_be_bytes().to_vec()))
        }
        "parse_unsigned_i32_text_radix_args" => Ok(Some(
            parse_unsigned_i32_text_radix_args(args)?
                .to_be_bytes()
                .to_vec(),
        )),
        "parse_unsigned_i64_text_radix_args" => Ok(Some(
            parse_unsigned_i64_text_radix_args(args)?
                .to_be_bytes()
                .to_vec(),
        )),
        "decode_i8_text" => Ok(Some(decode_i8_text(args)?.to_be_bytes().to_vec())),
        "decode_i16_text" => Ok(Some(decode_i16_text(args)?.to_be_bytes().to_vec())),
        "decode_i32_text" => Ok(Some(decode_i32_text(args)?.to_be_bytes().to_vec())),
        "decode_i64_text" => Ok(Some(decode_i64_text(args)?.to_be_bytes().to_vec())),
        "parse_f32_text" => Ok(Some(parse_f32_text(args)?.to_be_bytes().to_vec())),
        "parse_f64_text" => Ok(Some(parse_f64_text(args)?.to_be_bytes().to_vec())),
        "text_length" => Ok(Some(text_length(args)?.to_be_bytes().to_vec())),
        "text_is_empty" => Ok(Some(boolean_bytes(text_is_empty(args)?))),
        "text_is_blank" => Ok(Some(boolean_bytes(text_is_blank(args)?))),
        body if body.starts_with("add_i32_literal:") => {
            let literal = body.trim_start_matches("add_i32_literal:");
            Ok(Some(
                add_i32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("add_i64_literal:") => {
            let literal = body.trim_start_matches("add_i64_literal:");
            Ok(Some(
                add_i64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("add_f32_literal:") => {
            let literal = body.trim_start_matches("add_f32_literal:");
            Ok(Some(
                add_f32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("add_f64_literal:") => {
            let literal = body.trim_start_matches("add_f64_literal:");
            Ok(Some(
                add_f64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("sub_i32_literal:") => {
            let literal = body.trim_start_matches("sub_i32_literal:");
            Ok(Some(
                sub_i32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("sub_i64_literal:") => {
            let literal = body.trim_start_matches("sub_i64_literal:");
            Ok(Some(
                sub_i64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("sub_f32_literal:") => {
            let literal = body.trim_start_matches("sub_f32_literal:");
            Ok(Some(
                sub_f32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("sub_f64_literal:") => {
            let literal = body.trim_start_matches("sub_f64_literal:");
            Ok(Some(
                sub_f64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("mul_i32_literal:") => {
            let literal = body.trim_start_matches("mul_i32_literal:");
            Ok(Some(
                mul_i32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("mul_i64_literal:") => {
            let literal = body.trim_start_matches("mul_i64_literal:");
            Ok(Some(
                mul_i64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("mul_f32_literal:") => {
            let literal = body.trim_start_matches("mul_f32_literal:");
            Ok(Some(
                mul_f32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("mul_f64_literal:") => {
            let literal = body.trim_start_matches("mul_f64_literal:");
            Ok(Some(
                mul_f64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("div_i32_literal:") => {
            let literal = body.trim_start_matches("div_i32_literal:");
            Ok(Some(
                div_i32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("div_i64_literal:") => {
            let literal = body.trim_start_matches("div_i64_literal:");
            Ok(Some(
                div_i64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("div_f32_literal:") => {
            let literal = body.trim_start_matches("div_f32_literal:");
            Ok(Some(
                div_f32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("div_f64_literal:") => {
            let literal = body.trim_start_matches("div_f64_literal:");
            Ok(Some(
                div_f64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("rem_i32_literal:") => {
            let literal = body.trim_start_matches("rem_i32_literal:");
            Ok(Some(
                rem_i32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("rem_i64_literal:") => {
            let literal = body.trim_start_matches("rem_i64_literal:");
            Ok(Some(
                rem_i64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("rem_f32_literal:") => {
            let literal = body.trim_start_matches("rem_f32_literal:");
            Ok(Some(
                rem_f32_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("rem_f64_literal:") => {
            let literal = body.trim_start_matches("rem_f64_literal:");
            Ok(Some(
                rem_f64_literal(args, literal.parse().map_err(|e| format!("{e}"))?)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("concat_prefix:") => {
            let literal = body.trim_start_matches("concat_prefix:");
            concat_literal(args.first().cloned().unwrap_or(None), literal, true)
        }
        body if body.starts_with("concat_suffix:") => {
            let literal = body.trim_start_matches("concat_suffix:");
            concat_literal(args.first().cloned().unwrap_or(None), literal, false)
        }
        body if body.starts_with("cmp_i32_literal:") => {
            let (op, literal) =
                parse_comparison_alias(body.trim_start_matches("cmp_i32_literal:"))?;
            Ok(Some(boolean_bytes(compare_i32_literal(
                args,
                op,
                literal.parse().map_err(|e| format!("{e}"))?,
            )?)))
        }
        body if body.starts_with("cmp_i64_literal:") => {
            let (op, literal) =
                parse_comparison_alias(body.trim_start_matches("cmp_i64_literal:"))?;
            Ok(Some(boolean_bytes(compare_i64_literal(
                args,
                op,
                literal.parse().map_err(|e| format!("{e}"))?,
            )?)))
        }
        body if body.starts_with("cmp_f32_literal:") => {
            let (op, literal) =
                parse_comparison_alias(body.trim_start_matches("cmp_f32_literal:"))?;
            Ok(Some(boolean_bytes(compare_f32_literal(
                args,
                op,
                literal.parse().map_err(|e| format!("{e}"))?,
            )?)))
        }
        body if body.starts_with("cmp_f64_literal:") => {
            let (op, literal) =
                parse_comparison_alias(body.trim_start_matches("cmp_f64_literal:"))?;
            Ok(Some(boolean_bytes(compare_f64_literal(
                args,
                op,
                literal.parse().map_err(|e| format!("{e}"))?,
            )?)))
        }
        body if body.starts_with("cmp_i32_args:") => Ok(Some(boolean_bytes(compare_i32_args(
            args,
            body.trim_start_matches("cmp_i32_args:"),
        )?))),
        body if body.starts_with("cmp_i64_args:") => Ok(Some(boolean_bytes(compare_i64_args(
            args,
            body.trim_start_matches("cmp_i64_args:"),
        )?))),
        body if body.starts_with("cmp_f32_args:") => Ok(Some(boolean_bytes(compare_f32_args(
            args,
            body.trim_start_matches("cmp_f32_args:"),
        )?))),
        body if body.starts_with("cmp_f64_args:") => Ok(Some(boolean_bytes(compare_f64_args(
            args,
            body.trim_start_matches("cmp_f64_args:"),
        )?))),
        body if body.starts_with("cmp_bool_args:") => Ok(Some(boolean_bytes(compare_bool_args(
            args,
            body.trim_start_matches("cmp_bool_args:"),
        )?))),
        body if body.starts_with("text_eq_literal:") => Ok(Some(boolean_bytes(
            text_equals_literal(args, body.trim_start_matches("text_eq_literal:"))?,
        ))),
        body if body.starts_with("text_ne_literal:") => Ok(Some(boolean_bytes(
            !text_equals_literal(args, body.trim_start_matches("text_ne_literal:"))?,
        ))),
        body if body.starts_with("text_equals_ignore_case_literal:") => {
            Ok(Some(boolean_bytes(text_predicate_literal(
                args,
                body.trim_start_matches("text_equals_ignore_case_literal:"),
                text_equals_ignore_case,
            )?)))
        }
        "text_eq_args" => Ok(Some(boolean_bytes(text_equals_args(args)?))),
        "text_ne_args" => Ok(Some(boolean_bytes(!text_equals_args(args)?))),
        "text_equals_ignore_case_args" => Ok(Some(boolean_bytes(text_predicate_args(
            args,
            text_equals_ignore_case,
        )?))),
        body if body.starts_with("text_compare_to_literal:") => Ok(Some(
            text_compare_to_literal(args, body.trim_start_matches("text_compare_to_literal:"))?
                .to_be_bytes()
                .to_vec(),
        )),
        "text_compare_to_args" => Ok(Some(text_compare_to_args(args)?.to_be_bytes().to_vec())),
        body if body.starts_with("text_compare_to_ignore_case_literal:") => Ok(Some(
            text_compare_to_ignore_case_literal(
                args,
                body.trim_start_matches("text_compare_to_ignore_case_literal:"),
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        "text_compare_to_ignore_case_args" => Ok(Some(
            text_compare_to_ignore_case_args(args)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("bool_eq_literal:") => {
            Ok(Some(boolean_bytes(bool_equals_literal(
                args,
                parse_java_boolean_literal(body.trim_start_matches("bool_eq_literal:"))
                    .ok_or_else(|| "Invalid boolean literal".to_string())?,
            )?)))
        }
        body if body.starts_with("bool_ne_literal:") => {
            Ok(Some(boolean_bytes(!bool_equals_literal(
                args,
                parse_java_boolean_literal(body.trim_start_matches("bool_ne_literal:"))
                    .ok_or_else(|| "Invalid boolean literal".to_string())?,
            )?)))
        }
        body if body.starts_with("text_contains_literal:") => {
            Ok(Some(boolean_bytes(text_predicate_literal(
                args,
                body.trim_start_matches("text_contains_literal:"),
                |value, needle| value.contains(needle),
            )?)))
        }
        body if body.starts_with("text_starts_with_literal:") => {
            Ok(Some(boolean_bytes(text_predicate_literal(
                args,
                body.trim_start_matches("text_starts_with_literal:"),
                |value, prefix| value.starts_with(prefix),
            )?)))
        }
        body if body.starts_with("text_starts_with_literal_from:") => {
            let (prefix, from_index) =
                parse_index_from_alias(body.trim_start_matches("text_starts_with_literal_from:"))?;
            Ok(Some(boolean_bytes(text_starts_with_literal_from(
                args, prefix, from_index,
            )?)))
        }
        body if body.starts_with("text_ends_with_literal:") => {
            Ok(Some(boolean_bytes(text_predicate_literal(
                args,
                body.trim_start_matches("text_ends_with_literal:"),
                |value, suffix| value.ends_with(suffix),
            )?)))
        }
        "text_contains_args" => Ok(Some(boolean_bytes(text_predicate_args(
            args,
            |value, needle| value.contains(needle),
        )?))),
        "text_starts_with_args" => Ok(Some(boolean_bytes(text_predicate_args(
            args,
            |value, prefix| value.starts_with(prefix),
        )?))),
        "text_starts_with_from_args" => Ok(Some(boolean_bytes(text_starts_with_from_args(args)?))),
        "text_ends_with_args" => Ok(Some(boolean_bytes(text_predicate_args(
            args,
            |value, suffix| value.ends_with(suffix),
        )?))),
        body if body.starts_with("text_char_at_literal:") => Ok(Some(
            text_char_at_literal(
                args,
                body.trim_start_matches("text_char_at_literal:")
                    .parse()
                    .map_err(|e| format!("{e}"))?,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        "text_char_at_arg" => Ok(Some(text_char_at_arg(args)?.to_be_bytes().to_vec())),
        body if body.starts_with("text_code_point_at_literal:") => Ok(Some(
            text_code_point_at_literal(
                args,
                body.trim_start_matches("text_code_point_at_literal:")
                    .parse()
                    .map_err(|e| format!("{e}"))?,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        "text_code_point_at_arg" => Ok(Some(text_code_point_at_arg(args)?.to_be_bytes().to_vec())),
        body if body.starts_with("text_code_point_before_literal:") => Ok(Some(
            text_code_point_before_literal(
                args,
                body.trim_start_matches("text_code_point_before_literal:")
                    .parse()
                    .map_err(|e| format!("{e}"))?,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        "text_code_point_before_arg" => Ok(Some(
            text_code_point_before_arg(args)?.to_be_bytes().to_vec(),
        )),
        body if body.starts_with("text_code_point_count_literal:") => {
            let (begin, end) =
                parse_range_alias(body.trim_start_matches("text_code_point_count_literal:"))?;
            Ok(Some(
                text_code_point_count_literal(args, begin, end)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        "text_code_point_count_args" => Ok(Some(
            text_code_point_count_args(args)?.to_be_bytes().to_vec(),
        )),
        body if body.starts_with("text_offset_by_code_points_literal:") => {
            let (index, offset) =
                parse_range_alias(body.trim_start_matches("text_offset_by_code_points_literal:"))?;
            Ok(Some(
                text_offset_by_code_points_literal(args, index, offset)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        "text_offset_by_code_points_args" => Ok(Some(
            text_offset_by_code_points_args(args)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("text_substring_from:") => {
            let begin = body
                .trim_start_matches("text_substring_from:")
                .parse()
                .map_err(|e| format!("{e}"))?;
            text_substring_from(args, begin)
        }
        body if body.starts_with("text_substring_range:") => {
            let (begin, end) = parse_range_alias(body.trim_start_matches("text_substring_range:"))?;
            text_substring_range(args, begin, end)
        }
        "text_substring_from_arg" => text_substring_from_arg(args),
        "text_substring_range_args" => text_substring_range_args(args),
        "text_repeat_arg" => text_repeat_arg(args),
        body if body.starts_with("text_index_of_literal:") => Ok(Some(
            text_index_literal(
                args,
                body.trim_start_matches("text_index_of_literal:"),
                false,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        body if body.starts_with("text_last_index_of_literal:") => Ok(Some(
            text_index_literal(
                args,
                body.trim_start_matches("text_last_index_of_literal:"),
                true,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        body if body.starts_with("text_index_of_literal_from:") => {
            let (literal, from_index) =
                parse_index_from_alias(body.trim_start_matches("text_index_of_literal_from:"))?;
            Ok(Some(
                text_index_literal_from(args, literal, from_index, false)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        body if body.starts_with("text_last_index_of_literal_from:") => {
            let (literal, from_index) = parse_index_from_alias(
                body.trim_start_matches("text_last_index_of_literal_from:"),
            )?;
            Ok(Some(
                text_index_literal_from(args, literal, from_index, true)?
                    .to_be_bytes()
                    .to_vec(),
            ))
        }
        "text_index_of_args" => Ok(Some(text_index_args(args, false)?.to_be_bytes().to_vec())),
        "text_last_index_of_args" => Ok(Some(text_index_args(args, true)?.to_be_bytes().to_vec())),
        "text_index_of_from_args" => Ok(Some(
            text_index_from_args(args, false)?.to_be_bytes().to_vec(),
        )),
        "text_last_index_of_from_args" => Ok(Some(
            text_index_from_args(args, true)?.to_be_bytes().to_vec(),
        )),
        body if body.starts_with("text_replace_literals_len:") => {
            let (target, replacement) =
                parse_replace_len_alias(body.trim_start_matches("text_replace_literals_len:"))?;
            text_replace_literals(args, target, replacement)
        }
        body if body.starts_with("text_replace_literals:") => {
            let (target, replacement) =
                parse_replace_alias(body.trim_start_matches("text_replace_literals:"))?;
            text_replace_literals(args, target, replacement)
        }
        "text_replace_args" => text_replace_args(args),
        body if body.starts_with("parse_i8_text_radix:") => Ok(Some(
            parse_i8_text_radix(args, parse_radix_alias(body, "parse_i8_text_radix:")?)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("parse_i16_text_radix:") => Ok(Some(
            parse_i16_text_radix(args, parse_radix_alias(body, "parse_i16_text_radix:")?)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("parse_i32_text_radix:") => Ok(Some(
            parse_i32_text_radix(args, parse_radix_alias(body, "parse_i32_text_radix:")?)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("parse_i64_text_radix:") => Ok(Some(
            parse_i64_text_radix(args, parse_radix_alias(body, "parse_i64_text_radix:")?)?
                .to_be_bytes()
                .to_vec(),
        )),
        body if body.starts_with("parse_unsigned_i32_text_radix:") => Ok(Some(
            parse_unsigned_i32_text_radix(
                args,
                parse_radix_alias(body, "parse_unsigned_i32_text_radix:")?,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        body if body.starts_with("parse_unsigned_i64_text_radix:") => Ok(Some(
            parse_unsigned_i64_text_radix(
                args,
                parse_radix_alias(body, "parse_unsigned_i64_text_radix:")?,
            )?
            .to_be_bytes()
            .to_vec(),
        )),
        body if body.starts_with("boxed_numeric_conversion:") => {
            boxed_numeric_conversion(args, body.trim_start_matches("boxed_numeric_conversion:"))
        }
        body if body.starts_with("to_string_i32_radix:") => to_string_i32_radix(
            args,
            parse_string_radix_alias(body, "to_string_i32_radix:")?,
        ),
        body if body.starts_with("to_string_i64_radix:") => to_string_i64_radix(
            args,
            parse_string_radix_alias(body, "to_string_i64_radix:")?,
        ),
        body if body.starts_with("to_unsigned_string_i32_radix:") => to_unsigned_string_i32_radix(
            args,
            parse_string_radix_alias(body, "to_unsigned_string_i32_radix:")?,
        ),
        body if body.starts_with("to_unsigned_string_i64_radix:") => to_unsigned_string_i64_radix(
            args,
            parse_string_radix_alias(body, "to_unsigned_string_i64_radix:")?,
        ),
        body if body.starts_with("constant:") => {
            let literal = body.trim_start_matches("constant:");
            encode_literal(return_type, literal).map(Some)
        }
        other => Err(format!("Unsupported native UDF body: {other}")),
    }
}

fn normalized_body(body: &str) -> String {
    body.trim().to_ascii_lowercase()
}

fn canonical_body(body: &str, return_type: &str, arg_types: &[String]) -> String {
    let normalized = normalized_body(body);
    java_body_alias(body, return_type, arg_types).unwrap_or(normalized)
}

fn java_body_alias(body: &str, return_type: &str, arg_types: &[String]) -> Option<String> {
    let raw_expression = body
        .trim()
        .strip_prefix("return ")
        .and_then(|value| value.strip_suffix(';'))?
        .trim();
    let raw_expression =
        strip_ascii_prefix_ignore_case(raw_expression, "java.lang.").unwrap_or(raw_expression);
    let canonical_math_raw_expression;
    let math_raw_expression = if let Some(suffix) =
        strip_ascii_prefix_ignore_case(raw_expression, "strictmath.")
            .or_else(|| strip_ascii_prefix_ignore_case(raw_expression, "java.lang.strictmath."))
            .or_else(|| strip_ascii_prefix_ignore_case(raw_expression, "java.lang.math."))
    {
        canonical_math_raw_expression = format!("Math.{suffix}");
        canonical_math_raw_expression.as_str()
    } else {
        raw_expression
    };
    let expression = raw_expression.to_ascii_lowercase();
    let expression = expression.as_str();
    let canonical_math_expression;
    let math_expression = if let Some(suffix) = expression
        .strip_prefix("strictmath.")
        .or_else(|| expression.strip_prefix("java.lang.strictmath."))
        .or_else(|| expression.strip_prefix("java.lang.math."))
    {
        canonical_math_expression = format!("math.{suffix}");
        canonical_math_expression.as_str()
    } else {
        expression
    };

    if let Some(alias) = java_math_constant_alias(math_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_constant_alias(expression, return_type) {
        return Some(alias);
    }

    if is_binary_identifier_expression(expression, '+') {
        return match return_type.to_ascii_lowercase().as_str() {
            "int" => Some("add_i32".to_string()),
            "bigint" | "counter" => Some("add_i64".to_string()),
            "float" => Some("add_f32".to_string()),
            "double" => Some("add_f64".to_string()),
            "text" | "varchar" | "ascii" => Some("concat".to_string()),
            _ => None,
        };
    }
    if let Some(alias) = java_literal_binary_alias(raw_expression, '+', return_type) {
        return Some(alias);
    }

    if is_binary_identifier_expression(expression, '-') {
        match return_type.to_ascii_lowercase().as_str() {
            "int" => return Some("sub_i32".to_string()),
            "bigint" | "counter" => return Some("sub_i64".to_string()),
            "float" => return Some("sub_f32".to_string()),
            "double" => return Some("sub_f64".to_string()),
            _ => return None,
        }
    }
    if let Some(alias) = java_literal_binary_alias(raw_expression, '-', return_type) {
        return Some(alias);
    }

    for operator in ['*', '/', '%'] {
        if let Some(alias) = java_identifier_binary_alias(expression, operator, return_type) {
            return Some(alias);
        }
        if let Some(alias) = java_literal_binary_alias(raw_expression, operator, return_type) {
            return Some(alias);
        }
    }

    if let Some(alias) = java_unary_alias(expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_abs_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_abs_alias(math_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_signum_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_signum_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_round_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_round_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_get_exponent_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_get_exponent_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_exact_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_exact_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_exact_binary_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_exact_binary_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_wide_multiply_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_wide_multiply_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_floor_div_mod_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_floor_div_mod_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_min_max_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_min_max_alias(math_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_copysign_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_copysign_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_next_after_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_next_after_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_fma_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_fma_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_pow_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_pow_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f64_binary_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f64_binary_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f32_unary_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f32_unary_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f64_unary_literal_alias(math_raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_math_f64_unary_alias(math_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_float_predicate_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_float_predicate_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_float_constant_alias(expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_float_bit_conversion_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_float_bit_conversion_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_float_static_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_float_static_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_value_of_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_value_of_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_numeric_parse_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_numeric_parse_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        if let Some(alias) = java_primitive_to_string_literal_alias(raw_expression, return_type) {
            return Some(alias);
        }
        if let Some(alias) = java_objects_to_string_default_alias(raw_expression, arg_types) {
            return Some(alias);
        }
    }
    if let Some(alias) = java_primitive_to_string_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_unsigned_conversion_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_unsigned_conversion_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_bit_op_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_bit_op_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_bit_rotate_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_bit_rotate_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_integer_static_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_primitive_compare_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_primitive_compare_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_compare_to_literal_alias(raw_expression, return_type)
    {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_compare_to_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_primitive_hash_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_primitive_hash_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_hash_code_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_hash_code_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_character_predicate_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_character_numeric_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boolean_parse_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boolean_parse_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boolean_binary_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boolean_binary_alias(expression, return_type, arg_types) {
        return Some(alias);
    }

    if let Some(alias) = java_objects_hash_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_objects_null_check_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_objects_null_check_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_objects_require_non_null_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) =
        java_objects_require_non_null_else_alias(raw_expression, return_type, arg_types)
    {
        return Some(alias);
    }
    if let Some(alias) = java_objects_equals_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_objects_equals_alias(raw_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_equals_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_equals_alias(raw_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_comparison_literal_alias(raw_expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_text_method_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_text_literal_receiver_method_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_no_arg_text_method_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }

    if expression.ends_with(".tolowercase()") {
        return Some("lower".to_string());
    }
    if expression.ends_with(".touppercase()") {
        return Some("upper".to_string());
    }
    if let Some(alias) = java_boxed_primitive_to_string_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_primitive_to_string_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_boolean_value_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_boolean_value_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_numeric_conversion_literal_alias(raw_expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_boxed_numeric_conversion_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_no_arg_text_method_alias(expression, return_type, arg_types) {
        return Some(alias);
    }

    if expression
        .chars()
        .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Some("identity".to_string());
    }

    None
}

fn java_math_constant_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double" {
        return None;
    }
    match expression {
        "math.pi" => Some(format!("constant:{}", std::f64::consts::PI)),
        "math.e" => Some(format!("constant:{}", std::f64::consts::E)),
        _ => None,
    }
}

fn java_boxed_primitive_constant_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    let (expected_return_type, value) = match expression {
        "byte.max_value" => ("tinyint", i8::MAX.to_string()),
        "byte.min_value" => ("tinyint", i8::MIN.to_string()),
        "byte.size" => ("int", i8::BITS.to_string()),
        "byte.bytes" => ("int", std::mem::size_of::<i8>().to_string()),
        "short.max_value" => ("smallint", i16::MAX.to_string()),
        "short.min_value" => ("smallint", i16::MIN.to_string()),
        "short.size" => ("int", i16::BITS.to_string()),
        "short.bytes" => ("int", std::mem::size_of::<i16>().to_string()),
        "integer.max_value" => ("int", i32::MAX.to_string()),
        "integer.min_value" => ("int", i32::MIN.to_string()),
        "integer.size" => ("int", i32::BITS.to_string()),
        "integer.bytes" => ("int", std::mem::size_of::<i32>().to_string()),
        "long.max_value" => ("bigint", i64::MAX.to_string()),
        "long.min_value" => ("bigint", i64::MIN.to_string()),
        "long.size" => ("int", i64::BITS.to_string()),
        "long.bytes" => ("int", std::mem::size_of::<i64>().to_string()),
        "float.size" => ("int", "32".to_string()),
        "float.bytes" => ("int", std::mem::size_of::<f32>().to_string()),
        "double.size" => ("int", "64".to_string()),
        "double.bytes" => ("int", std::mem::size_of::<f64>().to_string()),
        "boolean.true" => ("boolean", "true".to_string()),
        "boolean.false" => ("boolean", "false".to_string()),
        _ => return None,
    };
    if return_type == expected_return_type
        || (expected_return_type == "bigint" && return_type == "counter")
    {
        Some(format!("constant:{value}"))
    } else {
        None
    }
}

fn java_literal_binary_alias(
    expression: &str,
    operator: char,
    return_type: &str,
) -> Option<String> {
    let (lhs, rhs) = expression.split_once(operator)?;
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    let return_type = return_type.to_ascii_lowercase();

    if is_java_identifier(lhs) {
        if let Some(number) = parse_java_integer_literal(rhs) {
            return match (operator, return_type.as_str()) {
                ('+', "int") => Some(format!("add_i32_literal:{number}")),
                ('+', "bigint" | "counter") => Some(format!("add_i64_literal:{number}")),
                ('-', "int") => Some(format!("sub_i32_literal:{number}")),
                ('-', "bigint" | "counter") => Some(format!("sub_i64_literal:{number}")),
                ('*', "int") => Some(format!("mul_i32_literal:{number}")),
                ('*', "bigint" | "counter") => Some(format!("mul_i64_literal:{number}")),
                ('/', "int") => Some(format!("div_i32_literal:{number}")),
                ('/', "bigint" | "counter") => Some(format!("div_i64_literal:{number}")),
                ('%', "int") => Some(format!("rem_i32_literal:{number}")),
                ('%', "bigint" | "counter") => Some(format!("rem_i64_literal:{number}")),
                _ => None,
            };
        }
        if let Some(number) = parse_java_float_literal(rhs) {
            return match (operator, return_type.as_str()) {
                ('+', "float") => Some(format!("add_f32_literal:{number}")),
                ('+', "double") => Some(format!("add_f64_literal:{number}")),
                ('-', "float") => Some(format!("sub_f32_literal:{number}")),
                ('-', "double") => Some(format!("sub_f64_literal:{number}")),
                ('*', "float") => Some(format!("mul_f32_literal:{number}")),
                ('*', "double") => Some(format!("mul_f64_literal:{number}")),
                ('/', "float") => Some(format!("div_f32_literal:{number}")),
                ('/', "double") => Some(format!("div_f64_literal:{number}")),
                ('%', "float") => Some(format!("rem_f32_literal:{number}")),
                ('%', "double") => Some(format!("rem_f64_literal:{number}")),
                _ => None,
            };
        }
        if operator == '+' && matches!(return_type.as_str(), "text" | "varchar" | "ascii") {
            return parse_java_string_literal(rhs)
                .map(|literal| format!("concat_suffix:{literal}"));
        }
    }

    if operator == '+'
        && matches!(return_type.as_str(), "text" | "varchar" | "ascii")
        && is_java_identifier(rhs)
    {
        return parse_java_string_literal(lhs).map(|literal| format!("concat_prefix:{literal}"));
    }

    None
}

fn java_equals_alias(expression: &str, return_type: &str, arg_types: &[String]) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let expression = expression.trim();
    let (negated, expression) = expression
        .strip_prefix('!')
        .map_or((false, expression), |value| (true, value.trim()));
    let normalized = expression.to_ascii_lowercase();
    let (method_index, marker, text_only) = [(".equals(", false), (".contentequals(", true)]
        .into_iter()
        .find_map(|(marker, text_only)| {
            normalized
                .find(marker)
                .map(|method_index| (method_index, marker, text_only))
        })?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let first_arg_type = arg_types
        .first()
        .map(|arg_type| arg_type.to_ascii_lowercase());
    if text_only
        && !matches!(
            first_arg_type.as_deref(),
            Some("text" | "varchar" | "ascii")
        )
    {
        return None;
    }
    let rhs_start = method_index + marker.len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    if let Some(literal) = parse_java_string_literal(rhs) {
        return Some(format!(
            "{}:{literal}",
            if negated {
                "text_ne_literal"
            } else {
                "text_eq_literal"
            }
        ));
    }
    if arg_types.len() >= 2
        && matches!(
            first_arg_type.as_deref(),
            Some("text" | "varchar" | "ascii")
        )
        && is_java_identifier(rhs)
    {
        return Some(
            if negated {
                "text_ne_args"
            } else {
                "text_eq_args"
            }
            .to_string(),
        );
    }
    if arg_types.len() >= 2 && is_java_identifier(rhs) {
        return match first_arg_type.as_deref() {
            Some("tinyint" | "smallint" | "int") => Some(format!(
                "cmp_i32_args:{}",
                if negated { "ne" } else { "eq" }
            )),
            Some("bigint" | "counter") => Some(format!(
                "cmp_i64_args:{}",
                if negated { "ne" } else { "eq" }
            )),
            Some("boolean") => Some(format!(
                "cmp_bool_args:{}",
                if negated { "ne" } else { "eq" }
            )),
            Some("float") => Some(
                if negated {
                    "boxed_not_equals_f32_args"
                } else {
                    "boxed_equals_f32_args"
                }
                .to_string(),
            ),
            Some("double") => Some(
                if negated {
                    "boxed_not_equals_f64_args"
                } else {
                    "boxed_equals_f64_args"
                }
                .to_string(),
            ),
            _ => None,
        };
    }
    None
}

fn java_equals_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let expression = expression.trim();
    let (negated, expression) = expression
        .strip_prefix('!')
        .map_or((false, expression), |value| (true, value.trim()));
    let lower = expression.to_ascii_lowercase();
    let method_index = lower.find(".equals(")?;
    let lhs = expression[..method_index].trim();
    let rhs_start = method_index + ".equals(".len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    let equals = if let (Some(lhs), Some(rhs)) = (
        java_boxed_compare_value_of_literal(lhs),
        java_boxed_compare_value_of_literal(rhs),
    ) {
        lhs.0 == rhs.0 && java_boxed_compare_values_equal(lhs.1, rhs.1)
    } else {
        java_object_literal_key(lhs)? == java_object_literal_key(rhs)?
    };
    Some(format!(
        "constant:{}",
        if negated { !equals } else { equals }
    ))
}

fn java_boxed_compare_values_equal(lhs: JavaBoxedCompareValue, rhs: JavaBoxedCompareValue) -> bool {
    match (lhs, rhs) {
        (JavaBoxedCompareValue::I64(lhs), JavaBoxedCompareValue::I64(rhs)) => lhs == rhs,
        (JavaBoxedCompareValue::Bool(lhs), JavaBoxedCompareValue::Bool(rhs)) => lhs == rhs,
        (JavaBoxedCompareValue::F32(lhs), JavaBoxedCompareValue::F32(rhs)) => {
            java_f32_to_i32_bits(lhs) == java_f32_to_i32_bits(rhs)
        }
        (JavaBoxedCompareValue::F64(lhs), JavaBoxedCompareValue::F64(rhs)) => {
            java_f64_to_i64_bits(lhs) == java_f64_to_i64_bits(rhs)
        }
        _ => false,
    }
}

fn java_objects_null_check_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" || arg_types.is_empty() {
        return None;
    }
    let (value, alias) = if let Some(value) = expression.strip_prefix("objects.isnull(") {
        (value, "objects_is_null")
    } else if let Some(value) = expression.strip_prefix("java.util.objects.isnull(") {
        (value, "objects_is_null")
    } else if let Some(value) = expression.strip_prefix("objects.nonnull(") {
        (value, "objects_non_null")
    } else if let Some(value) = expression.strip_prefix("java.util.objects.nonnull(") {
        (value, "objects_non_null")
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    is_java_identifier(value).then(|| alias.to_string())
}

fn java_objects_null_check_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let (value, is_null_check) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "objects.isnull(") {
            (value, true)
        } else if let Some(value) =
            strip_ascii_prefix_ignore_case(expression, "java.util.objects.isnull(")
        {
            (value, true)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "objects.nonnull(") {
            (value, false)
        } else if let Some(value) =
            strip_ascii_prefix_ignore_case(expression, "java.util.objects.nonnull(")
        {
            (value, false)
        } else {
            return None;
        };
    let value = value.strip_suffix(')')?.trim();
    let is_null = if value.eq_ignore_ascii_case("null") {
        true
    } else if parse_java_string_literal(value).is_some()
        || parse_java_boolean_literal(value).is_some()
        || parse_java_integer_literal(value).is_some()
        || parse_java_float_literal(value).is_some()
    {
        false
    } else {
        return None;
    };
    Some(format!(
        "constant:{}",
        if is_null_check { is_null } else { !is_null }
    ))
}

fn java_objects_hash_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "objects.hash(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "java.util.objects.hash("))?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    let hash = params.iter().try_fold(1i32, |hash, param| {
        java_literal_hash_code(param.trim()).map(|value| hash.wrapping_mul(31).wrapping_add(value))
    })?;
    Some(format!("constant:{hash}"))
}

fn java_objects_require_non_null_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let Some(first_arg_type) = arg_types.first() else {
        return None;
    };
    if !cql_return_type_matches_arg(return_type, first_arg_type) {
        return None;
    }
    let value = expression
        .strip_prefix("objects.requirenonnull(")
        .or_else(|| expression.strip_prefix("java.util.objects.requirenonnull("))?;
    let params = value.strip_suffix(')')?.trim();
    let params = split_java_args(params)?;
    let value = params.first()?;
    if params.len() > 2 || !is_java_identifier(value) {
        return None;
    }
    match params.get(1).copied() {
        None => Some("objects_require_non_null".to_string()),
        Some(message)
            if is_java_identifier(message) || parse_java_string_literal(message).is_some() =>
        {
            Some("objects_require_non_null".to_string())
        }
        Some(_) => None,
    }
}

fn java_objects_require_non_null_else_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !cql_return_type_matches_arg(return_type, arg_types.first()?) {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "objects.requirenonnullelse(").or_else(
        || strip_ascii_prefix_ignore_case(expression, "java.util.objects.requirenonnullelse("),
    )?;
    let params = value.strip_suffix(')')?.trim();
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let value = params[0].trim();
    let fallback = params[1].trim();
    if !is_java_identifier(value) {
        return None;
    }
    if is_java_identifier(fallback) && cql_return_type_matches_arg(return_type, arg_types.get(1)?) {
        Some("objects_require_non_null_else_args".to_string())
    } else if matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        parse_java_string_literal(fallback)
            .map(|literal| format!("objects_require_non_null_else_text_literal:{literal}"))
    } else {
        None
    }
}

fn cql_return_type_matches_arg(return_type: &str, arg_type: &str) -> bool {
    let return_type = return_type.to_ascii_lowercase();
    let arg_type = arg_type.to_ascii_lowercase();
    return_type == arg_type
        || matches!(
            (return_type.as_str(), arg_type.as_str()),
            ("text" | "varchar" | "ascii", "text" | "varchar" | "ascii")
        )
}

fn strip_ascii_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.get(..prefix.len())?.eq_ignore_ascii_case(prefix) {
        value.get(prefix.len()..)
    } else {
        None
    }
}

fn strip_java_lang_prefix(value: &str) -> &str {
    strip_ascii_prefix_ignore_case(value.trim(), "java.lang.").unwrap_or_else(|| value.trim())
}

fn java_objects_equals_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let expression = expression.trim();
    let (negated, expression) = expression
        .strip_prefix('!')
        .map_or((false, expression), |value| (true, value.trim()));
    let value = if let Some(value) = strip_ascii_prefix_ignore_case(expression, "objects.equals(") {
        value
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "java.util.objects.equals(")
    {
        value
    } else {
        return None;
    };
    let params = value.strip_suffix(')')?.trim();
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let lhs = params[0].trim();
    let rhs = params[1].trim();
    if matches!(
        arg_types
            .first()
            .map(|arg_type| arg_type.to_ascii_lowercase())
            .as_deref(),
        Some("text" | "varchar" | "ascii")
    ) {
        if is_java_identifier(lhs) {
            if let Some(literal) = parse_java_string_literal(rhs) {
                return Some(format!(
                    "{}:{literal}",
                    if negated {
                        "text_ne_literal"
                    } else {
                        "text_eq_literal"
                    }
                ));
            }
        } else if is_java_identifier(rhs) {
            if let Some(literal) = parse_java_string_literal(lhs) {
                return Some(format!(
                    "{}:{literal}",
                    if negated {
                        "text_ne_literal"
                    } else {
                        "text_eq_literal"
                    }
                ));
            }
        }
    }
    if !is_java_identifier(lhs) || !is_java_identifier(rhs) || arg_types.len() < 2 {
        return None;
    }
    match arg_types
        .first()
        .map(|arg_type| arg_type.to_ascii_lowercase())
        .as_deref()
    {
        Some("text" | "varchar" | "ascii") => Some(
            if negated {
                "text_ne_args"
            } else {
                "text_eq_args"
            }
            .to_string(),
        ),
        Some("tinyint" | "smallint" | "int") => Some(format!(
            "cmp_i32_args:{}",
            if negated { "ne" } else { "eq" }
        )),
        Some("bigint" | "counter") => Some(format!(
            "cmp_i64_args:{}",
            if negated { "ne" } else { "eq" }
        )),
        Some("boolean") => Some(format!(
            "cmp_bool_args:{}",
            if negated { "ne" } else { "eq" }
        )),
        Some("float") => Some(
            if negated {
                "boxed_not_equals_f32_args"
            } else {
                "boxed_equals_f32_args"
            }
            .to_string(),
        ),
        Some("double") => Some(
            if negated {
                "boxed_not_equals_f64_args"
            } else {
                "boxed_equals_f64_args"
            }
            .to_string(),
        ),
        _ => None,
    }
}

fn java_objects_equals_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let expression = expression.trim();
    let (negated, expression) = expression
        .strip_prefix('!')
        .map_or((false, expression), |value| (true, value.trim()));
    let value = strip_ascii_prefix_ignore_case(expression, "objects.equals(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "java.util.objects.equals("))?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let lhs = java_object_literal_key(params[0].trim())?;
    let rhs = java_object_literal_key(params[1].trim())?;
    let equals = lhs == rhs;
    Some(format!(
        "constant:{}",
        if negated { !equals } else { equals }
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JavaObjectLiteralKey {
    Null,
    Text(String),
    Boolean(bool),
    Integer(i64),
    Float32(u32),
    Float64(u64),
}

fn java_object_literal_key(value: &str) -> Option<JavaObjectLiteralKey> {
    if value.eq_ignore_ascii_case("null") {
        return Some(JavaObjectLiteralKey::Null);
    }
    if let Some(value) = parse_java_string_literal(value) {
        return Some(JavaObjectLiteralKey::Text(value));
    }
    if let Some(value) = parse_java_boolean_literal(value) {
        return Some(JavaObjectLiteralKey::Boolean(value));
    }
    if let Some(value) = parse_java_integer_literal(value) {
        return Some(JavaObjectLiteralKey::Integer(value));
    }
    if let Some(value) = parse_java_float_literal(value) {
        if is_java_f32_literal(value) {
            return Some(JavaObjectLiteralKey::Float32(
                java_f32_to_i32_bits(value as f32) as u32,
            ));
        }
        return Some(JavaObjectLiteralKey::Float64(
            java_f64_to_i64_bits(value) as u64
        ));
    }
    None
}

fn is_java_f32_literal(value: &str) -> bool {
    let value = strip_java_lang_prefix(value);
    value.ends_with('f') || value.ends_with('F') || value.to_ascii_lowercase().starts_with("float.")
}

fn java_comparison_literal_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }

    for (operator, alias_op) in [
        (">=", "gte"),
        ("<=", "lte"),
        ("==", "eq"),
        ("!=", "ne"),
        (">", "gt"),
        ("<", "lt"),
    ] {
        let Some((lhs, rhs)) = expression.split_once(operator) else {
            continue;
        };
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if !is_java_identifier(lhs) {
            return None;
        }

        if matches!(operator, "==" | "!=") {
            if let Some(literal) = parse_java_string_literal(rhs) {
                let prefix = if operator == "==" {
                    "text_eq_literal"
                } else {
                    "text_ne_literal"
                };
                return Some(format!("{prefix}:{literal}"));
            }
            if let Some(literal) = parse_java_boolean_literal(rhs) {
                let prefix = if operator == "==" {
                    "bool_eq_literal"
                } else {
                    "bool_ne_literal"
                };
                return Some(format!("{prefix}:{literal}"));
            }
        }

        if let Some(number) = parse_java_integer_literal(rhs) {
            return Some(format!(
                "{}:{alias_op}:{number}",
                comparison_numeric_alias_prefix(arg_types, false)
            ));
        }

        if let Some(number) = parse_java_float_literal(rhs) {
            return Some(format!(
                "{}:{alias_op}:{number}",
                comparison_numeric_alias_prefix(arg_types, true)
            ));
        }

        if is_java_identifier(rhs) {
            if let Some(prefix) = comparison_args_alias_prefix(arg_types, operator) {
                return Some(format!("{prefix}:{alias_op}"));
            }
        }
    }

    None
}

fn comparison_numeric_alias_prefix(arg_types: &[String], literal_is_float: bool) -> &'static str {
    match arg_types.first().map(|value| value.to_ascii_lowercase()) {
        Some(value) if value == "float" => "cmp_f32_literal",
        Some(value) if value == "double" => "cmp_f64_literal",
        Some(value) if matches!(value.as_str(), "int" | "smallint" | "tinyint") => {
            "cmp_i32_literal"
        }
        Some(value) if matches!(value.as_str(), "bigint" | "counter") => "cmp_i64_literal",
        _ if literal_is_float => "cmp_f64_literal",
        _ => "cmp_i64_literal",
    }
}

fn comparison_args_alias_prefix(arg_types: &[String], operator: &str) -> Option<&'static str> {
    if arg_types.len() < 2 {
        return None;
    }
    match arg_types.first().map(|value| value.to_ascii_lowercase()) {
        Some(value) if value == "float" => Some("cmp_f32_args"),
        Some(value) if value == "double" => Some("cmp_f64_args"),
        Some(value) if matches!(value.as_str(), "int" | "smallint" | "tinyint") => {
            Some("cmp_i32_args")
        }
        Some(value) if matches!(value.as_str(), "bigint" | "counter") => Some("cmp_i64_args"),
        Some(value) if value == "boolean" && matches!(operator, "==" | "!=") => {
            Some("cmp_bool_args")
        }
        _ => None,
    }
}

fn java_text_method_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    if let Some((prefix, from_index)) = parse_java_text_starts_with_literal_from(expression) {
        return match return_type.as_str() {
            "boolean" => Some(format!(
                "text_starts_with_literal_from:{prefix}:{from_index}"
            )),
            _ => None,
        };
    }

    if let Some((method, literal, from_index)) = parse_java_text_index_literal_from(expression) {
        return match (method.as_str(), return_type.as_str()) {
            ("indexof", "int") => {
                Some(format!("text_index_of_literal_from:{literal}:{from_index}"))
            }
            ("lastindexof", "int") => Some(format!(
                "text_last_index_of_literal_from:{literal}:{from_index}"
            )),
            _ => None,
        };
    }
    if let Some(index) = parse_java_text_char_at_literal(expression) {
        return match return_type.as_str() {
            "int" => Some(format!("text_char_at_literal:{index}")),
            _ => None,
        };
    }
    if let Some(index) = parse_java_text_code_point_at_literal(expression) {
        return match return_type.as_str() {
            "int" => Some(format!("text_code_point_at_literal:{index}")),
            _ => None,
        };
    }
    if let Some(index) = parse_java_text_code_point_before_literal(expression) {
        return match return_type.as_str() {
            "int" => Some(format!("text_code_point_before_literal:{index}")),
            _ => None,
        };
    }
    if let Some((begin, end)) = parse_java_text_code_point_count_literal(expression) {
        return match return_type.as_str() {
            "int" => Some(format!("text_code_point_count_literal:{begin}:{end}")),
            _ => None,
        };
    }
    if let Some((index, offset)) = parse_java_text_offset_by_code_points_literal(expression) {
        return match return_type.as_str() {
            "int" => Some(format!(
                "text_offset_by_code_points_literal:{index}:{offset}"
            )),
            _ => None,
        };
    }
    if let Some((method, literal)) = parse_java_text_method_literal(expression) {
        return match (method.as_str(), return_type.as_str()) {
            ("contains", "boolean") => Some(format!("text_contains_literal:{literal}")),
            ("startswith", "boolean") => Some(format!("text_starts_with_literal:{literal}")),
            ("endswith", "boolean") => Some(format!("text_ends_with_literal:{literal}")),
            ("equalsignorecase", "boolean") => {
                Some(format!("text_equals_ignore_case_literal:{literal}"))
            }
            ("compareto", "int") => Some(format!("text_compare_to_literal:{literal}")),
            ("comparetoignorecase", "int") => {
                Some(format!("text_compare_to_ignore_case_literal:{literal}"))
            }
            ("indexof", "int") => Some(format!("text_index_of_literal:{literal}")),
            ("lastindexof", "int") => Some(format!("text_last_index_of_literal:{literal}")),
            ("concat", "text" | "varchar" | "ascii") => Some(format!("concat_suffix:{literal}")),
            _ => None,
        };
    }

    if parse_java_text_starts_with_from_args(expression).is_some() {
        return match return_type.as_str() {
            "boolean" => Some("text_starts_with_from_args".to_string()),
            _ => None,
        };
    }

    if let Some(method) = parse_java_text_index_from_args(expression) {
        return match (method.as_str(), return_type.as_str()) {
            ("indexof", "int") => Some("text_index_of_from_args".to_string()),
            ("lastindexof", "int") => Some("text_last_index_of_from_args".to_string()),
            _ => None,
        };
    }

    if let Some(method) = parse_java_text_method_arg(expression) {
        return match (method.as_str(), return_type.as_str()) {
            ("contains", "boolean") => Some("text_contains_args".to_string()),
            ("startswith", "boolean") => Some("text_starts_with_args".to_string()),
            ("endswith", "boolean") => Some("text_ends_with_args".to_string()),
            ("equalsignorecase", "boolean") => Some("text_equals_ignore_case_args".to_string()),
            ("compareto", "int") => Some("text_compare_to_args".to_string()),
            ("comparetoignorecase", "int") => Some("text_compare_to_ignore_case_args".to_string()),
            ("indexof", "int") => Some("text_index_of_args".to_string()),
            ("lastindexof", "int") => Some("text_last_index_of_args".to_string()),
            ("concat", "text" | "varchar" | "ascii") => Some("concat".to_string()),
            _ => None,
        };
    }
    if parse_java_text_char_at_arg(expression).is_some() {
        return match return_type.as_str() {
            "int" => Some("text_char_at_arg".to_string()),
            _ => None,
        };
    }
    if parse_java_text_code_point_at_arg(expression).is_some() {
        return match return_type.as_str() {
            "int" => Some("text_code_point_at_arg".to_string()),
            _ => None,
        };
    }
    if parse_java_text_code_point_before_arg(expression).is_some() {
        return match return_type.as_str() {
            "int" => Some("text_code_point_before_arg".to_string()),
            _ => None,
        };
    }
    if parse_java_text_code_point_count_args(expression).is_some() {
        return match return_type.as_str() {
            "int" => Some("text_code_point_count_args".to_string()),
            _ => None,
        };
    }
    if parse_java_text_offset_by_code_points_args(expression).is_some() {
        return match return_type.as_str() {
            "int" => Some("text_offset_by_code_points_args".to_string()),
            _ => None,
        };
    }

    if return_type == "text" || return_type == "varchar" || return_type == "ascii" {
        if let Some((begin, end)) = parse_java_substring_literal(expression) {
            return match end {
                Some(end) => Some(format!("text_substring_range:{begin}:{end}")),
                None => Some(format!("text_substring_from:{begin}")),
            };
        }
        if let Some(has_end) = parse_java_substring_args(expression) {
            return Some(
                if has_end {
                    "text_substring_range_args"
                } else {
                    "text_substring_from_arg"
                }
                .to_string(),
            );
        }
        if parse_java_repeat_arg(expression).is_some() {
            return Some("text_repeat_arg".to_string());
        }
        if let Some((target, replacement)) = parse_java_replace_literals(expression) {
            return Some(format!(
                "text_replace_literals_len:{}:{target}{replacement}",
                target.len()
            ));
        }
        if parse_java_replace_args(expression).is_some() {
            return Some("text_replace_args".to_string());
        }
    }

    None
}

fn java_text_literal_receiver_method_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    let normalized = expression.to_ascii_lowercase();
    for method in [
        "contains",
        "contentequals",
        "startswith",
        "endswith",
        "equalsignorecase",
        "compareto",
        "comparetoignorecase",
        "indexof",
        "lastindexof",
        "concat",
        "charat",
        "codepointat",
        "codepointbefore",
        "codepointcount",
        "offsetbycodepoints",
        "substring",
        "replace",
        "repeat",
    ] {
        let marker = format!(".{method}(");
        let Some(method_index) = normalized.find(&marker) else {
            continue;
        };
        let receiver = parse_java_string_literal(expression[..method_index].trim())?;
        let params_start = method_index + marker.len();
        let params = split_java_args(expression[params_start..].strip_suffix(')')?.trim())?;
        let folded = match method {
            "contains" if return_type == "boolean" && params.len() == 1 => receiver
                .contains(&parse_java_string_literal(params[0].trim())?)
                .to_string(),
            "contentequals" if return_type == "boolean" && params.len() == 1 => {
                (receiver == parse_java_string_literal(params[0].trim())?).to_string()
            }
            "startswith" if return_type == "boolean" && params.len() == 1 => receiver
                .starts_with(&parse_java_string_literal(params[0].trim())?)
                .to_string(),
            "startswith" if return_type == "boolean" && params.len() == 2 => {
                let prefix = parse_java_string_literal(params[0].trim())?;
                let from_index = params[1].trim().parse::<i32>().ok()?;
                java_text_starts_with_from(&receiver, &prefix, from_index).to_string()
            }
            "endswith" if return_type == "boolean" && params.len() == 1 => receiver
                .ends_with(&parse_java_string_literal(params[0].trim())?)
                .to_string(),
            "equalsignorecase" if return_type == "boolean" && params.len() == 1 => {
                text_equals_ignore_case(&receiver, &parse_java_string_literal(params[0].trim())?)
                    .to_string()
            }
            "compareto" if return_type == "int" && params.len() == 1 => {
                java_text_compare_to(&receiver, &parse_java_string_literal(params[0].trim())?)
                    .to_string()
            }
            "comparetoignorecase" if return_type == "int" && params.len() == 1 => {
                java_text_compare_to_ignore_case(
                    &receiver,
                    &parse_java_string_literal(params[0].trim())?,
                )
                .to_string()
            }
            "indexof" if return_type == "int" && params.len() == 1 => java_text_index(
                &receiver,
                &parse_java_string_literal(params[0].trim())?,
                false,
            )
            .to_string(),
            "lastindexof" if return_type == "int" && params.len() == 1 => java_text_index(
                &receiver,
                &parse_java_string_literal(params[0].trim())?,
                true,
            )
            .to_string(),
            "indexof" if return_type == "int" && params.len() == 2 => {
                let literal = parse_java_string_literal(params[0].trim())?;
                let from_index = params[1].trim().parse::<i32>().ok()?;
                java_text_index_from(&receiver, &literal, from_index, false).to_string()
            }
            "lastindexof" if return_type == "int" && params.len() == 2 => {
                let literal = parse_java_string_literal(params[0].trim())?;
                let from_index = params[1].trim().parse::<i32>().ok()?;
                java_text_index_from(&receiver, &literal, from_index, true).to_string()
            }
            "concat"
                if matches!(return_type.as_str(), "text" | "varchar" | "ascii")
                    && params.len() == 1 =>
            {
                format!(
                    "{}{}",
                    receiver,
                    parse_java_string_literal(params[0].trim())?
                )
            }
            "charat" if return_type == "int" && params.len() == 1 => {
                let index = params[0].trim().parse::<i32>().ok()?;
                java_text_char_at(&receiver, index).ok()?.to_string()
            }
            "codepointat" if return_type == "int" && params.len() == 1 => {
                let index = params[0].trim().parse::<i32>().ok()?;
                java_text_code_point_at(&receiver, index).ok()?.to_string()
            }
            "codepointbefore" if return_type == "int" && params.len() == 1 => {
                let index = params[0].trim().parse::<i32>().ok()?;
                java_text_code_point_before(&receiver, index)
                    .ok()?
                    .to_string()
            }
            "codepointcount" if return_type == "int" && params.len() == 2 => {
                let begin = params[0].trim().parse::<i32>().ok()?;
                let end = params[1].trim().parse::<i32>().ok()?;
                java_text_code_point_count(&receiver, begin, end)
                    .ok()?
                    .to_string()
            }
            "offsetbycodepoints" if return_type == "int" && params.len() == 2 => {
                let index = params[0].trim().parse::<i32>().ok()?;
                let offset = params[1].trim().parse::<i32>().ok()?;
                java_text_offset_by_code_points(&receiver, index, offset)
                    .ok()?
                    .to_string()
            }
            "substring"
                if matches!(return_type.as_str(), "text" | "varchar" | "ascii")
                    && (1..=2).contains(&params.len()) =>
            {
                let begin = params[0].trim().parse::<isize>().ok()?;
                let end = params
                    .get(1)
                    .and_then(|value| value.trim().parse::<isize>().ok())
                    .unwrap_or_else(|| receiver.encode_utf16().count() as isize);
                java_substring_chars(&receiver, begin, end).ok()?
            }
            "replace"
                if matches!(return_type.as_str(), "text" | "varchar" | "ascii")
                    && params.len() == 2 =>
            {
                let target = parse_java_text_or_char_literal(params[0].trim())?;
                let replacement = parse_java_text_or_char_literal(params[1].trim())?;
                receiver.replace(&target, &replacement)
            }
            "repeat"
                if matches!(return_type.as_str(), "text" | "varchar" | "ascii")
                    && params.len() == 1 =>
            {
                let count = params[0].trim().parse::<usize>().ok()?;
                receiver.repeat(count)
            }
            _ => return None,
        };
        return Some(format!("constant:{folded}"));
    }
    None
}

fn parse_java_text_starts_with_literal_from(expression: &str) -> Option<(String, i32)> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".startswith(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let prefix = parse_java_string_literal(params[0])?;
    let from_index = params[1].parse().ok()?;
    Some((prefix, from_index))
}

fn parse_java_text_starts_with_from_args(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".startswith(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 || !is_java_identifier(params[0]) || !is_java_identifier(params[1]) {
        return None;
    }
    Some(())
}

fn parse_java_text_char_at_literal(expression: &str) -> Option<i32> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".charat(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    if index.contains(',') {
        return None;
    }
    index.parse().ok()
}

fn parse_java_text_char_at_arg(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".charat(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    is_java_identifier(index).then_some(())
}

fn parse_java_text_code_point_at_literal(expression: &str) -> Option<i32> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointat(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    if index.contains(',') {
        return None;
    }
    index.parse().ok()
}

fn parse_java_text_code_point_at_arg(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointat(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    is_java_identifier(index).then_some(())
}

fn parse_java_text_code_point_before_literal(expression: &str) -> Option<i32> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointbefore(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    if index.contains(',') {
        return None;
    }
    index.parse().ok()
}

fn parse_java_text_code_point_before_arg(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointbefore(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let index = expression[params_start..].strip_suffix(')')?.trim();
    is_java_identifier(index).then_some(())
}

fn parse_java_text_code_point_count_literal(expression: &str) -> Option<(i32, i32)> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointcount(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let begin = params[0].parse().ok()?;
    let end = params[1].parse().ok()?;
    Some((begin, end))
}

fn parse_java_text_code_point_count_args(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".codepointcount(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    (is_java_identifier(params[0]) && is_java_identifier(params[1])).then_some(())
}

fn parse_java_text_offset_by_code_points_literal(expression: &str) -> Option<(i32, i32)> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".offsetbycodepoints(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let index = params[0].parse().ok()?;
    let offset = params[1].parse().ok()?;
    Some((index, offset))
}

fn parse_java_text_offset_by_code_points_args(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".offsetbycodepoints(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    (is_java_identifier(params[0]) && is_java_identifier(params[1])).then_some(())
}

fn parse_java_text_method_arg(expression: &str) -> Option<String> {
    let normalized = expression.to_ascii_lowercase();
    for method in [
        "contains",
        "startswith",
        "endswith",
        "equalsignorecase",
        "compareto",
        "comparetoignorecase",
        "indexof",
        "lastindexof",
        "concat",
    ] {
        let marker = format!(".{method}(");
        let Some(method_index) = normalized.find(&marker) else {
            continue;
        };
        let lhs = expression[..method_index].trim();
        if !is_java_identifier(lhs) {
            return None;
        }
        let rhs_start = method_index + marker.len();
        let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
        if is_java_identifier(rhs) {
            return Some(method.to_string());
        }
    }
    None
}

fn parse_java_text_method_literal(expression: &str) -> Option<(String, String)> {
    let normalized = expression.to_ascii_lowercase();
    for method in [
        "contains",
        "startswith",
        "endswith",
        "equalsignorecase",
        "compareto",
        "comparetoignorecase",
        "indexof",
        "lastindexof",
        "concat",
    ] {
        let marker = format!(".{method}(");
        let Some(method_index) = normalized.find(&marker) else {
            continue;
        };
        let lhs = expression[..method_index].trim();
        if !is_java_identifier(lhs) {
            return None;
        }
        let rhs_start = method_index + marker.len();
        let literal = parse_java_string_literal(expression[rhs_start..].strip_suffix(')')?.trim())?;
        return Some((method.to_string(), literal));
    }
    None
}

fn parse_java_text_index_literal_from(expression: &str) -> Option<(String, String, i32)> {
    let normalized = expression.to_ascii_lowercase();
    for method in ["indexof", "lastindexof"] {
        let marker = format!(".{method}(");
        let Some(method_index) = normalized.find(&marker) else {
            continue;
        };
        let lhs = expression[..method_index].trim();
        if !is_java_identifier(lhs) {
            return None;
        }
        let params_start = method_index + marker.len();
        let params = expression[params_start..].strip_suffix(')')?;
        let params = split_java_args(params)?;
        if params.len() != 2 {
            return None;
        }
        let literal = parse_java_string_literal(params[0])?;
        let from_index = params[1].parse().ok()?;
        return Some((method.to_string(), literal, from_index));
    }
    None
}

fn parse_java_text_index_from_args(expression: &str) -> Option<String> {
    let normalized = expression.to_ascii_lowercase();
    for method in ["indexof", "lastindexof"] {
        let marker = format!(".{method}(");
        let Some(method_index) = normalized.find(&marker) else {
            continue;
        };
        let lhs = expression[..method_index].trim();
        if !is_java_identifier(lhs) {
            return None;
        }
        let params_start = method_index + marker.len();
        let params = expression[params_start..].strip_suffix(')')?;
        let params = split_java_args(params)?;
        if params.len() != 2 || !is_java_identifier(params[0]) || !is_java_identifier(params[1]) {
            return None;
        }
        return Some(method.to_string());
    }
    None
}

fn parse_java_substring_literal(expression: &str) -> Option<(i32, Option<i32>)> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".substring(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() > 2 {
        return None;
    }
    let begin = params.first()?.parse().ok()?;
    let end = params.get(1).map(|value| value.parse()).transpose().ok()?;
    Some((begin, end))
}

fn parse_java_substring_args(expression: &str) -> Option<bool> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".substring(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    let begin = params.first()?;
    if !is_java_identifier(begin) {
        return None;
    }
    if params.len() > 2 {
        return None;
    }
    match params.get(1).copied() {
        Some(end) if is_java_identifier(end) => Some(true),
        Some(_) => None,
        None => Some(false),
    }
}

fn parse_java_repeat_arg(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".repeat(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let rhs_start = method_index + marker.len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    is_java_identifier(rhs).then_some(())
}

fn parse_java_replace_literals(expression: &str) -> Option<(String, String)> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".replace(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let target = parse_java_text_or_char_literal(params[0])?;
    let replacement = parse_java_text_or_char_literal(params[1])?;
    Some((target, replacement))
}

fn parse_java_replace_args(expression: &str) -> Option<()> {
    let normalized = expression.to_ascii_lowercase();
    let marker = ".replace(";
    let method_index = normalized.find(marker)?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let params_start = method_index + marker.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    if is_java_identifier(params[0]) && is_java_identifier(params[1]) {
        return Some(());
    }
    None
}

fn parse_java_integer_literal(value: &str) -> Option<i64> {
    let value = strip_java_lang_prefix(value);
    match value.to_ascii_lowercase().as_str() {
        "byte.max_value" => return Some(i8::MAX as i64),
        "byte.min_value" => return Some(i8::MIN as i64),
        "short.max_value" => return Some(i16::MAX as i64),
        "short.min_value" => return Some(i16::MIN as i64),
        "integer.max_value" => return Some(i32::MAX as i64),
        "integer.min_value" => return Some(i32::MIN as i64),
        "long.max_value" => return Some(i64::MAX),
        "long.min_value" => return Some(i64::MIN),
        _ => {}
    }
    value.trim_end_matches(['l', 'L']).parse::<i64>().ok()
}

fn parse_java_float_literal(value: &str) -> Option<f64> {
    let value = strip_java_lang_prefix(value);
    match value.to_ascii_lowercase().as_str() {
        "float.nan" | "double.nan" => return Some(f64::NAN),
        "float.positive_infinity" | "double.positive_infinity" => return Some(f64::INFINITY),
        "float.negative_infinity" | "double.negative_infinity" => {
            return Some(f64::NEG_INFINITY);
        }
        "float.max_value" => return Some(f32::MAX as f64),
        "float.min_value" => return Some(f32::from_bits(1) as f64),
        "float.min_normal" => return Some(f32::MIN_POSITIVE as f64),
        "double.max_value" => return Some(f64::MAX),
        "double.min_value" => return Some(f64::from_bits(1)),
        "double.min_normal" => return Some(f64::MIN_POSITIVE),
        _ => {}
    }
    value
        .trim_end_matches(['f', 'F', 'd', 'D'])
        .parse::<f64>()
        .ok()
}

fn parse_java_boolean_literal(value: &str) -> Option<bool> {
    match strip_java_lang_prefix(value).to_ascii_lowercase().as_str() {
        "true" | "boolean.true" => Some(true),
        "false" | "boolean.false" => Some(false),
        _ => None,
    }
}

fn parse_java_string_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        parse_java_escaped_literal_inner(&value[1..value.len() - 1])
    } else {
        None
    }
}

fn parse_java_text_or_char_literal(value: &str) -> Option<String> {
    parse_java_string_literal(value).or_else(|| parse_java_char_literal(value).map(String::from))
}

fn split_java_args(value: &str) -> Option<Vec<&str>> {
    let mut args = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (idx, ch) in value.char_indices() {
        if let Some(current_quote) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == current_quote {
                quote = None;
            }
        } else if ch == '"' || ch == '\'' {
            quote = Some(ch);
        } else if ch == ',' {
            args.push(value[start..idx].trim());
            start = idx + ch.len_utf8();
        }
    }
    if quote.is_some() || escaped {
        return None;
    }
    args.push(value[start..].trim());
    Some(args)
}

fn parse_java_char_literal(value: &str) -> Option<char> {
    let value = value.trim();
    if value.len() < 3 || !value.starts_with('\'') || !value.ends_with('\'') {
        return None;
    }
    let decoded = parse_java_escaped_literal_inner(&value[1..value.len() - 1])?;
    let mut chars = decoded.chars();
    let ch = chars.next()?;
    chars.next().is_none().then_some(ch)
}

fn parse_java_escaped_literal_inner(value: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let escaped = chars.next()?;
        match escaped {
            'b' => out.push('\u{0008}'),
            't' => out.push('\t'),
            'n' => out.push('\n'),
            'f' => out.push('\u{000C}'),
            'r' => out.push('\r'),
            '"' => out.push('"'),
            '\'' => out.push('\''),
            '\\' => out.push('\\'),
            'u' => {
                while matches!(chars.peek(), Some('u')) {
                    chars.next();
                }
                let mut codepoint = 0u32;
                for _ in 0..4 {
                    codepoint = (codepoint << 4) | chars.next()?.to_digit(16)?;
                }
                out.push(char::from_u32(codepoint)?);
            }
            '0'..='7' => {
                let mut value = escaped.to_digit(8)?;
                let max_extra_digits = if escaped <= '3' { 2 } else { 1 };
                for _ in 0..max_extra_digits {
                    let Some(next) = chars.peek().copied() else {
                        break;
                    };
                    let Some(digit) = next.to_digit(8) else {
                        break;
                    };
                    chars.next();
                    value = (value << 3) | digit;
                }
                out.push(char::from_u32(value)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

fn is_binary_identifier_expression(expression: &str, operator: char) -> bool {
    let Some((lhs, rhs)) = expression.split_once(operator) else {
        return false;
    };
    is_java_identifier(lhs.trim()) && is_java_identifier(rhs.trim())
}

fn java_identifier_binary_alias(
    expression: &str,
    operator: char,
    return_type: &str,
) -> Option<String> {
    if !is_binary_identifier_expression(expression, operator) {
        return None;
    }
    match (operator, return_type.to_ascii_lowercase().as_str()) {
        ('*', "int") => Some("mul_i32".to_string()),
        ('*', "bigint" | "counter") => Some("mul_i64".to_string()),
        ('*', "float") => Some("mul_f32".to_string()),
        ('*', "double") => Some("mul_f64".to_string()),
        ('/', "int") => Some("div_i32".to_string()),
        ('/', "bigint" | "counter") => Some("div_i64".to_string()),
        ('/', "float") => Some("div_f32".to_string()),
        ('/', "double") => Some("div_f64".to_string()),
        ('%', "int") => Some("rem_i32".to_string()),
        ('%', "bigint" | "counter") => Some("rem_i64".to_string()),
        ('%', "float") => Some("rem_f32".to_string()),
        ('%', "double") => Some("rem_f64".to_string()),
        _ => None,
    }
}

fn java_unary_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    if let Some(value) = expression.strip_prefix('-') {
        if !is_java_identifier(value.trim()) {
            return None;
        }
        return match return_type.as_str() {
            "int" => Some("neg_i32".to_string()),
            "bigint" | "counter" => Some("neg_i64".to_string()),
            "float" => Some("neg_f32".to_string()),
            "double" => Some("neg_f64".to_string()),
            _ => None,
        };
    }
    if let Some(value) = expression.strip_prefix('!') {
        if return_type == "boolean" && is_java_identifier(value.trim()) {
            return Some("not_bool".to_string());
        }
    }
    None
}

fn java_math_abs_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = expression
        .strip_prefix("math.abs(")
        .and_then(|value| value.strip_suffix(')'))?
        .trim();
    if !is_java_identifier(value) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "int" => Some("abs_i32".to_string()),
        "bigint" | "counter" => Some("abs_i64".to_string()),
        "float" => Some("abs_f32".to_string()),
        "double" => Some("abs_f64".to_string()),
        _ => None,
    }
}

fn java_math_abs_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.abs(")?
        .strip_suffix(')')?
        .trim();
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" => i32::try_from(parse_java_integer_literal(value)?)
            .ok()?
            .saturating_abs()
            .to_string(),
        "bigint" | "counter" => parse_java_integer_literal(value)?
            .saturating_abs()
            .to_string(),
        "float" => java_f32_to_string((parse_java_float_literal(value)? as f32).abs()),
        "double" => java_f64_to_string(parse_java_float_literal(value)?.abs()),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_signum_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let value = expression
        .strip_prefix("math.signum(")
        .and_then(|value| value.strip_suffix(')'))?
        .trim();
    if !is_java_identifier(value) {
        return None;
    }
    match (
        return_type.to_ascii_lowercase().as_str(),
        arg_types.first().map(|value| value.to_ascii_lowercase()),
    ) {
        ("float", Some(arg_type)) if arg_type == "float" => Some("signum_f32".to_string()),
        ("double", Some(arg_type)) if arg_type == "double" => Some("signum_f64".to_string()),
        _ => None,
    }
}

fn java_math_signum_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.signum(")?
        .strip_suffix(')')?
        .trim();
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "float" => java_f32_to_string((parse_java_float_literal(value)? as f32).signum()),
        "double" => java_f64_to_string(parse_java_float_literal(value)?.signum()),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_round_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let value = expression
        .strip_prefix("math.round(")
        .and_then(|value| value.strip_suffix(')'))?
        .trim();
    if !is_java_identifier(value) {
        return None;
    }
    match (
        return_type.to_ascii_lowercase().as_str(),
        arg_types.first().map(|value| value.to_ascii_lowercase()),
    ) {
        ("int", Some(arg_type)) if arg_type == "float" => Some("round_f32_to_i32".to_string()),
        ("bigint", Some(arg_type)) if arg_type == "double" => Some("round_f64_to_i64".to_string()),
        _ => None,
    }
}

fn java_math_round_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.round(")?
        .strip_suffix(')')?
        .trim();
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" => java_round_f32(parse_java_float_literal(value)? as f32).to_string(),
        "bigint" | "counter" => java_round_f64(parse_java_float_literal(value)?).to_string(),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_get_exponent_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "int" {
        return None;
    }
    let value = expression
        .strip_prefix("math.getexponent(")
        .and_then(|value| value.strip_suffix(')'))?
        .trim();
    if !is_java_identifier(value) {
        return None;
    }
    match arg_types.first().map(|value| value.to_ascii_lowercase()) {
        Some(arg_type) if arg_type == "float" => Some("get_exponent_f32_to_i32".to_string()),
        Some(arg_type) if arg_type == "double" => Some("get_exponent_f64_to_i32".to_string()),
        _ => None,
    }
}

fn java_math_get_exponent_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "math.getexponent(")?
        .strip_suffix(')')?
        .trim();
    let parsed = parse_java_float_literal(value)?;
    let folded = if value.ends_with(['f', 'F']) {
        java_get_exponent_f32(parsed as f32)
    } else {
        java_get_exponent_f64(parsed)
    };
    Some(format!("constant:{folded}"))
}

fn java_math_exact_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.incrementexact(") {
        ("inc_exact", value)
    } else if let Some(value) = expression.strip_prefix("math.decrementexact(") {
        ("dec_exact", value)
    } else if let Some(value) = expression.strip_prefix("math.negateexact(") {
        ("neg_exact", value)
    } else if let Some(value) = expression.strip_prefix("math.tointexact(") {
        ("to_i32_exact", value)
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    match (
        return_type.to_ascii_lowercase().as_str(),
        arg_types.first().map(|value| value.to_ascii_lowercase()),
    ) {
        ("int", Some(arg_type)) if operation == "to_i32_exact" && arg_type == "bigint" => {
            Some(operation.to_string())
        }
        ("int", Some(arg_type)) if arg_type == "int" => Some(format!("{operation}_i32")),
        ("bigint", Some(arg_type)) if arg_type == "bigint" => Some(format!("{operation}_i64")),
        _ => None,
    }
}

fn java_math_exact_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (operation, value) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "math.incrementexact(")
    {
        ("inc", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.decrementexact(") {
        ("dec", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.negateexact(") {
        ("neg", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.tointexact(") {
        ("to_i32", value)
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" if operation == "to_i32" => i32::try_from(parse_java_integer_literal(value)?)
            .ok()?
            .to_string(),
        "int" => {
            let value = i32::try_from(parse_java_integer_literal(value)?).ok()?;
            match operation {
                "inc" => value.checked_add(1),
                "dec" => value.checked_sub(1),
                "neg" => value.checked_neg(),
                _ => None,
            }?
            .to_string()
        }
        "bigint" if operation != "to_i32" => {
            let value = parse_java_integer_literal(value)?;
            match operation {
                "inc" => value.checked_add(1),
                "dec" => value.checked_sub(1),
                "neg" => value.checked_neg(),
                _ => None,
            }?
            .to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_exact_binary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (operation, value) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "math.addexact(")
    {
        ("add", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.subtractexact(") {
        ("sub", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.multiplyexact(") {
        ("mul", value)
    } else {
        return None;
    };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            match operation {
                "add" => lhs.checked_add(rhs),
                "sub" => lhs.checked_sub(rhs),
                "mul" => lhs.checked_mul(rhs),
                _ => None,
            }?
            .to_string()
        }
        "bigint" | "counter" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            match operation {
                "add" => lhs.checked_add(rhs),
                "sub" => lhs.checked_sub(rhs),
                "mul" => lhs.checked_mul(rhs),
                _ => None,
            }?
            .to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_exact_binary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.addexact(") {
        ("add_exact", value)
    } else if let Some(value) = expression.strip_prefix("math.subtractexact(") {
        ("sub_exact", value)
    } else if let Some(value) = expression.strip_prefix("math.multiplyexact(") {
        ("mul_exact", value)
    } else {
        return None;
    };
    let (lhs, rhs) = value.strip_suffix(')')?.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "int"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("int")) =>
        {
            Some(format!("{operation}_i32_args"))
        }
        "bigint"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("bigint")) =>
        {
            Some(format!("{operation}_i64_args"))
        }
        _ => None,
    }
}

fn java_math_wide_multiply_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 || !return_type.eq_ignore_ascii_case("bigint") {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.multiplyfull(") {
        ("mul_full_i32_args", value)
    } else if let Some(value) = expression.strip_prefix("math.multiplyhigh(") {
        ("mul_high_i64_args", value)
    } else {
        return None;
    };
    let (lhs, rhs) = value.strip_suffix(')')?.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match operation {
        "mul_full_i32_args"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("int")) =>
        {
            Some(operation.to_string())
        }
        "mul_high_i64_args"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("bigint")) =>
        {
            Some(operation.to_string())
        }
        _ => None,
    }
}

fn java_math_wide_multiply_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("bigint") {
        return None;
    }
    let (operation, value) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "math.multiplyfull(")
    {
        ("mul_full", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.multiplyhigh(") {
        ("mul_high", value)
    } else {
        return None;
    };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match operation {
        "mul_full" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as i64;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()? as i64;
            lhs * rhs
        }
        "mul_high" => {
            let lhs = parse_java_integer_literal(params[0].trim())? as i128;
            let rhs = parse_java_integer_literal(params[1].trim())? as i128;
            ((lhs * rhs) >> 64) as i64
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_floor_div_mod_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.floordiv(") {
        ("floor_div", value)
    } else if let Some(value) = expression.strip_prefix("math.floormod(") {
        ("floor_mod", value)
    } else {
        return None;
    };
    let (lhs, rhs) = value.strip_suffix(')')?.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "int"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("int")) =>
        {
            Some(format!("{operation}_i32_args"))
        }
        "bigint"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("bigint")) =>
        {
            Some(format!("{operation}_i64_args"))
        }
        _ => None,
    }
}

fn java_math_floor_div_mod_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (operation, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.floordiv(") {
            ("floor_div", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.floormod(") {
            ("floor_mod", value)
        } else {
            return None;
        };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            match operation {
                "floor_div" => java_floor_div_i32(lhs, rhs),
                "floor_mod" => java_floor_mod_i32(lhs, rhs),
                _ => return None,
            }
            .ok()?
            .to_string()
        }
        "bigint" | "counter" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            match operation {
                "floor_div" => java_floor_div_i64(lhs, rhs),
                "floor_mod" => java_floor_mod_i64(lhs, rhs),
                _ => return None,
            }
            .ok()?
            .to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_min_max_alias(expression: &str, return_type: &str) -> Option<String> {
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.max(") {
        ("max", value)
    } else if let Some(value) = expression.strip_prefix("math.min(") {
        ("min", value)
    } else {
        return None;
    };
    let (lhs, rhs) = value.strip_suffix(')')?.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "int" => Some(format!("{operation}_i32_args")),
        "bigint" | "counter" => Some(format!("{operation}_i64_args")),
        "float" => Some(format!("{operation}_f32_args")),
        "double" => Some(format!("{operation}_f64_args")),
        _ => None,
    }
}

fn java_math_min_max_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (operation, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.max(") {
            ("max", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.min(") {
            ("min", value)
        } else {
            return None;
        };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "int" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            match operation {
                "max" => lhs.max(rhs),
                "min" => lhs.min(rhs),
                _ => return None,
            }
            .to_string()
        }
        "bigint" | "counter" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            match operation {
                "max" => lhs.max(rhs),
                "min" => lhs.min(rhs),
                _ => return None,
            }
            .to_string()
        }
        "float" => {
            let lhs = parse_java_float_literal(params[0].trim())? as f32;
            let rhs = parse_java_float_literal(params[1].trim())? as f32;
            match operation {
                "max" => java_max_f32(lhs, rhs),
                "min" => java_min_f32(lhs, rhs),
                _ => return None,
            }
            .to_string()
        }
        "double" => {
            let lhs = parse_java_float_literal(params[0].trim())?;
            let rhs = parse_java_float_literal(params[1].trim())?;
            match operation {
                "max" => java_max_f64(lhs, rhs),
                "min" => java_min_f64(lhs, rhs),
                _ => return None,
            }
            .to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_copysign_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (lhs, rhs) = expression
        .strip_prefix("math.copysign(")
        .and_then(|value| value.strip_suffix(')'))?
        .split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "float"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("float")) =>
        {
            Some("copysign_f32_args".to_string())
        }
        "double"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("double")) =>
        {
            Some("copysign_f64_args".to_string())
        }
        _ => None,
    }
}

fn java_math_copysign_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.copysign(")?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "float" => {
            let magnitude = parse_java_float_literal(params[0].trim())? as f32;
            let sign = parse_java_float_literal(params[1].trim())? as f32;
            magnitude.copysign(sign).to_string()
        }
        "double" => {
            let magnitude = parse_java_float_literal(params[0].trim())?;
            let sign = parse_java_float_literal(params[1].trim())?;
            magnitude.copysign(sign).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_next_after_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (lhs, rhs) = expression
        .strip_prefix("math.nextafter(")
        .and_then(|value| value.strip_suffix(')'))?
        .split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "float"
            if arg_types[0].eq_ignore_ascii_case("float")
                && arg_types[1].eq_ignore_ascii_case("double") =>
        {
            Some("next_after_f32_args".to_string())
        }
        "double"
            if arg_types
                .iter()
                .take(2)
                .all(|value| value.eq_ignore_ascii_case("double")) =>
        {
            Some("next_after_f64_args".to_string())
        }
        _ => None,
    }
}

fn java_math_next_after_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.nextafter(")?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "float" => {
            let start = parse_java_float_literal(params[0].trim())? as f32;
            let direction = parse_java_float_literal(params[1].trim())?;
            java_next_after_f32(start, direction).to_string()
        }
        "double" => {
            let start = parse_java_float_literal(params[0].trim())?;
            let direction = parse_java_float_literal(params[1].trim())?;
            java_next_after_f64(start, direction).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_fma_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 3 {
        return None;
    }
    let value = expression
        .strip_prefix("math.fma(")
        .and_then(|value| value.strip_suffix(')'))?;
    let mut params = value.split(',');
    let first = params.next()?.trim();
    let second = params.next()?.trim();
    let third = params.next()?.trim();
    if params.next().is_some()
        || !is_java_identifier(first)
        || !is_java_identifier(second)
        || !is_java_identifier(third)
    {
        return None;
    }
    match return_type.to_ascii_lowercase().as_str() {
        "float"
            if arg_types
                .iter()
                .take(3)
                .all(|value| value.eq_ignore_ascii_case("float")) =>
        {
            Some("fma_f32_args".to_string())
        }
        "double"
            if arg_types
                .iter()
                .take(3)
                .all(|value| value.eq_ignore_ascii_case("double")) =>
        {
            Some("fma_f64_args".to_string())
        }
        _ => None,
    }
}

fn java_math_fma_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let value = strip_ascii_prefix_ignore_case(expression, "math.fma(")?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 3 {
        return None;
    }
    let folded = match return_type.to_ascii_lowercase().as_str() {
        "float" => {
            let multiplicand = parse_java_float_literal(params[0].trim())? as f32;
            let multiplier = parse_java_float_literal(params[1].trim())? as f32;
            let addend = parse_java_float_literal(params[2].trim())? as f32;
            multiplicand.mul_add(multiplier, addend).to_string()
        }
        "double" => {
            let multiplicand = parse_java_float_literal(params[0].trim())?;
            let multiplier = parse_java_float_literal(params[1].trim())?;
            let addend = parse_java_float_literal(params[2].trim())?;
            multiplicand.mul_add(multiplier, addend).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_pow_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double"
        || arg_types.len() < 2
        || !arg_types
            .iter()
            .take(2)
            .all(|value| value.eq_ignore_ascii_case("double"))
    {
        return None;
    }
    let (lhs, rhs) = expression
        .strip_prefix("math.pow(")
        .and_then(|value| value.strip_suffix(')'))?
        .split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some("pow_f64_args".to_string())
}

fn java_math_pow_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double" {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "math.pow(")?;
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let lhs = parse_java_float_literal(params[0].trim())?;
    let rhs = parse_java_float_literal(params[1].trim())?;
    Some(format!("constant:{}", lhs.powf(rhs)))
}

fn java_math_f64_binary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double"
        || arg_types.len() < 2
        || !arg_types
            .iter()
            .take(2)
            .all(|value| value.eq_ignore_ascii_case("double"))
    {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.hypot(") {
        ("hypot", value)
    } else if let Some(value) = expression.strip_prefix("math.atan2(") {
        ("atan2", value)
    } else {
        return None;
    };
    let (lhs, rhs) = value.strip_suffix(')')?.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some(format!("{operation}_f64_args"))
}

fn java_math_f64_binary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double" {
        return None;
    }
    let (operation, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.hypot(") {
            ("hypot", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.atan2(") {
            ("atan2", value)
        } else {
            return None;
        };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let lhs = parse_java_float_literal(params[0].trim())?;
    let rhs = parse_java_float_literal(params[1].trim())?;
    let folded = match operation {
        "hypot" => lhs.hypot(rhs),
        "atan2" => lhs.atan2(rhs),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_f32_unary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "float"
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(value) if value == "float"
        )
    {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.nextup(") {
        ("next_up", value)
    } else if let Some(value) = expression.strip_prefix("math.nextdown(") {
        ("next_down", value)
    } else if let Some(value) = expression.strip_prefix("math.ulp(") {
        ("ulp", value)
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(format!("{operation}_f32"))
}

fn java_math_f32_unary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "float" {
        return None;
    }
    let (operation, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.nextup(") {
            ("next_up", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.nextdown(") {
            ("next_down", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.ulp(") {
            ("ulp", value)
        } else {
            return None;
        };
    let value = parse_java_float_literal(value.strip_suffix(')')?.trim())? as f32;
    let folded = match operation {
        "next_up" => value.next_up(),
        "next_down" => value.next_down(),
        "ulp" => java_ulp_f32(value),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_math_f64_unary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double"
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(value) if value == "double"
        )
    {
        return None;
    }
    let (operation, value) = if let Some(value) = expression.strip_prefix("math.sqrt(") {
        ("sqrt", value)
    } else if let Some(value) = expression.strip_prefix("math.floor(") {
        ("floor", value)
    } else if let Some(value) = expression.strip_prefix("math.ceil(") {
        ("ceil", value)
    } else if let Some(value) = expression.strip_prefix("math.rint(") {
        ("rint", value)
    } else if let Some(value) = expression.strip_prefix("math.nextup(") {
        ("next_up", value)
    } else if let Some(value) = expression.strip_prefix("math.nextdown(") {
        ("next_down", value)
    } else if let Some(value) = expression.strip_prefix("math.ulp(") {
        ("ulp", value)
    } else if let Some(value) = expression.strip_prefix("math.log10(") {
        ("log10", value)
    } else if let Some(value) = expression.strip_prefix("math.log(") {
        ("log", value)
    } else if let Some(value) = expression.strip_prefix("math.exp(") {
        ("exp", value)
    } else if let Some(value) = expression.strip_prefix("math.expm1(") {
        ("expm1", value)
    } else if let Some(value) = expression.strip_prefix("math.log1p(") {
        ("log1p", value)
    } else if let Some(value) = expression.strip_prefix("math.sin(") {
        ("sin", value)
    } else if let Some(value) = expression.strip_prefix("math.cos(") {
        ("cos", value)
    } else if let Some(value) = expression.strip_prefix("math.tan(") {
        ("tan", value)
    } else if let Some(value) = expression.strip_prefix("math.sinh(") {
        ("sinh", value)
    } else if let Some(value) = expression.strip_prefix("math.cosh(") {
        ("cosh", value)
    } else if let Some(value) = expression.strip_prefix("math.tanh(") {
        ("tanh", value)
    } else if let Some(value) = expression.strip_prefix("math.asin(") {
        ("asin", value)
    } else if let Some(value) = expression.strip_prefix("math.acos(") {
        ("acos", value)
    } else if let Some(value) = expression.strip_prefix("math.atan(") {
        ("atan", value)
    } else if let Some(value) = expression.strip_prefix("math.cbrt(") {
        ("cbrt", value)
    } else if let Some(value) = expression.strip_prefix("math.toradians(") {
        ("to_radians", value)
    } else if let Some(value) = expression.strip_prefix("math.todegrees(") {
        ("to_degrees", value)
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(format!("{operation}_f64"))
}

fn java_math_f64_unary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "double" {
        return None;
    }
    let (operation, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.sqrt(") {
            ("sqrt", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.floor(") {
            ("floor", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.ceil(") {
            ("ceil", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.rint(") {
            ("rint", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.nextup(") {
            ("next_up", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.nextdown(") {
            ("next_down", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.ulp(") {
            ("ulp", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.log10(") {
            ("log10", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.log(") {
            ("log", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.exp(") {
            ("exp", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.expm1(") {
            ("expm1", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.log1p(") {
            ("log1p", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.sin(") {
            ("sin", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.cos(") {
            ("cos", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.tan(") {
            ("tan", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.sinh(") {
            ("sinh", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.cosh(") {
            ("cosh", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.tanh(") {
            ("tanh", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.asin(") {
            ("asin", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.acos(") {
            ("acos", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.atan(") {
            ("atan", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.cbrt(") {
            ("cbrt", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.toradians(") {
            ("to_radians", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "math.todegrees(") {
            ("to_degrees", value)
        } else {
            return None;
        };
    let value = parse_java_float_literal(value.strip_suffix(')')?.trim())?;
    let folded = match operation {
        "sqrt" => value.sqrt(),
        "floor" => value.floor(),
        "ceil" => value.ceil(),
        "rint" => value.round_ties_even(),
        "next_up" => value.next_up(),
        "next_down" => value.next_down(),
        "ulp" => java_ulp_f64(value),
        "log10" => value.log10(),
        "log" => value.ln(),
        "exp" => value.exp(),
        "expm1" => value.exp_m1(),
        "log1p" => value.ln_1p(),
        "sin" => value.sin(),
        "cos" => value.cos(),
        "tan" => value.tan(),
        "sinh" => value.sinh(),
        "cosh" => value.cosh(),
        "tanh" => value.tanh(),
        "asin" => value.asin(),
        "acos" => value.acos(),
        "atan" => value.atan(),
        "cbrt" => value.cbrt(),
        "to_radians" => value.to_radians(),
        "to_degrees" => value.to_degrees(),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_float_predicate_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    if let Some(alias) = java_boxed_float_predicate_alias(expression, arg_types) {
        return Some(alias);
    }
    let (class_name, value) = if let Some(value) = expression.strip_prefix("float.") {
        ("float", value)
    } else if let Some(value) = expression.strip_prefix("double.") {
        ("double", value)
    } else {
        return None;
    };
    let (operation, value) = if let Some(value) = value.strip_prefix("isnan(") {
        ("is_nan", value)
    } else if let Some(value) = value.strip_prefix("isinfinite(") {
        ("is_infinite", value)
    } else if let Some(value) = value.strip_prefix("isfinite(") {
        ("is_finite", value)
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    match (
        class_name,
        arg_types.first().map(|value| value.to_ascii_lowercase()),
    ) {
        ("float", Some(arg_type)) if arg_type == "float" => Some(format!("{operation}_f32")),
        ("double", Some(arg_type)) if arg_type == "double" => Some(format!("{operation}_f64")),
        _ => None,
    }
}

fn java_float_predicate_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let (class_name, value) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.") {
            ("float", value)
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.") {
            ("double", value)
        } else {
            return None;
        };
    let (operation, value) = if let Some(value) = strip_ascii_prefix_ignore_case(value, "isnan(") {
        ("is_nan", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(value, "isinfinite(") {
        ("is_infinite", value)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(value, "isfinite(") {
        ("is_finite", value)
    } else {
        return None;
    };
    let value = parse_java_float_literal(value.strip_suffix(')')?.trim())?;
    let predicate = match (class_name, operation) {
        ("float", "is_nan") => (value as f32).is_nan(),
        ("double", "is_nan") => value.is_nan(),
        ("float", "is_infinite") => (value as f32).is_infinite(),
        ("double", "is_infinite") => value.is_infinite(),
        ("float", "is_finite") => (value as f32).is_finite(),
        ("double", "is_finite") => value.is_finite(),
        _ => return None,
    };
    Some(format!("constant:{predicate}"))
}

fn java_boxed_float_predicate_alias(expression: &str, arg_types: &[String]) -> Option<String> {
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let (lhs, operation) = if let Some(lhs) = expression.strip_suffix(".isnan()") {
        (lhs, "is_nan")
    } else if let Some(lhs) = expression.strip_suffix(".isinfinite()") {
        (lhs, "is_infinite")
    } else if let Some(lhs) = expression.strip_suffix(".isfinite()") {
        (lhs, "is_finite")
    } else {
        return None;
    };
    if !is_java_identifier(lhs.trim()) {
        return None;
    }
    match arg_type.as_str() {
        "float" => Some(format!("{operation}_f32")),
        "double" => Some(format!("{operation}_f64")),
        _ => None,
    }
}

fn java_float_constant_alias(expression: &str, return_type: &str) -> Option<String> {
    match (return_type.to_ascii_lowercase().as_str(), expression) {
        ("float", "float.nan") => Some("float_nan_const".to_string()),
        ("float", "float.positive_infinity") => Some("float_pos_inf_const".to_string()),
        ("float", "float.negative_infinity") => Some("float_neg_inf_const".to_string()),
        ("float", "float.max_value") => Some("float_max_value_const".to_string()),
        ("float", "float.min_value") => Some("float_min_value_const".to_string()),
        ("float", "float.min_normal") => Some("float_min_normal_const".to_string()),
        ("double", "double.nan") => Some("double_nan_const".to_string()),
        ("double", "double.positive_infinity") => Some("double_pos_inf_const".to_string()),
        ("double", "double.negative_infinity") => Some("double_neg_inf_const".to_string()),
        ("double", "double.max_value") => Some("double_max_value_const".to_string()),
        ("double", "double.min_value") => Some("double_min_value_const".to_string()),
        ("double", "double.min_normal") => Some("double_min_normal_const".to_string()),
        _ => None,
    }
}

fn java_float_bit_conversion_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let (value, expected_arg_type, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("float.floattointbits(") {
            (value, "float", "int", "f32_to_i32_bits")
        } else if let Some(value) = expression.strip_prefix("float.floattorawintbits(") {
            (value, "float", "int", "f32_to_raw_i32_bits")
        } else if let Some(value) = expression.strip_prefix("float.intbitstofloat(") {
            (value, "int", "float", "i32_bits_to_f32")
        } else if let Some(value) = expression.strip_prefix("double.doubletolongbits(") {
            (value, "double", "bigint", "f64_to_i64_bits")
        } else if let Some(value) = expression.strip_prefix("double.doubletorawlongbits(") {
            (value, "double", "bigint", "f64_to_raw_i64_bits")
        } else if let Some(value) = expression.strip_prefix("double.longbitstodouble(") {
            (value, "bigint", "double", "i64_bits_to_f64")
        } else {
            return None;
        };
    if arg_type != expected_arg_type || !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_float_bit_conversion_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, kind, expected_return_type) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "float.floattointbits(")
    {
        (value, "f32_bits", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "float.floattorawintbits(")
    {
        (value, "f32_raw_bits", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.intbitstofloat(")
    {
        (value, "i32_to_f32", "float")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "double.doubletolongbits(")
    {
        (value, "f64_bits", "bigint")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "double.doubletorawlongbits(")
    {
        (value, "f64_raw_bits", "bigint")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "double.longbitstodouble(")
    {
        (value, "i64_to_f64", "double")
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    let folded = match kind {
        "f32_bits" => java_f32_to_i32_bits(parse_java_float_literal(value)? as f32).to_string(),
        "f32_raw_bits" => (parse_java_float_literal(value)? as f32)
            .to_bits()
            .to_string(),
        "i32_to_f32" => {
            let bits = i32::try_from(parse_java_integer_literal(value)?).ok()? as u32;
            let value = f32::from_bits(bits);
            if value.is_nan() {
                return None;
            }
            java_f32_to_string(value)
        }
        "f64_bits" => java_f64_to_i64_bits(parse_java_float_literal(value)?).to_string(),
        "f64_raw_bits" => (parse_java_float_literal(value)?.to_bits() as i64).to_string(),
        "i64_to_f64" => {
            let bits = parse_java_integer_literal(value)? as u64;
            let value = f64::from_bits(bits);
            if value.is_nan() {
                return None;
            }
            java_f64_to_string(value)
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_float_static_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (value, expected_arg_type, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("float.sum(") {
            (value, "float", "float", "float_sum_args")
        } else if let Some(value) = expression.strip_prefix("float.max(") {
            (value, "float", "float", "float_max_args")
        } else if let Some(value) = expression.strip_prefix("float.min(") {
            (value, "float", "float", "float_min_args")
        } else if let Some(value) = expression.strip_prefix("double.sum(") {
            (value, "double", "double", "double_sum_args")
        } else if let Some(value) = expression.strip_prefix("double.max(") {
            (value, "double", "double", "double_max_args")
        } else if let Some(value) = expression.strip_prefix("double.min(") {
            (value, "double", "double", "double_min_args")
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case(expected_return_type)
        || !arg_types
            .iter()
            .take(2)
            .all(|arg_type| arg_type.eq_ignore_ascii_case(expected_arg_type))
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let (lhs, rhs) = params.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some(alias.to_string())
}

fn java_float_static_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, operation, expected_return_type) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.sum(") {
            (value, "f32_sum", "float")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.max(") {
            (value, "f32_max", "float")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.min(") {
            (value, "f32_min", "float")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.sum(") {
            (value, "f64_sum", "double")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.max(") {
            (value, "f64_max", "double")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.min(") {
            (value, "f64_min", "double")
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match operation {
        "f32_sum" => {
            let lhs = parse_java_float_literal(params[0].trim())? as f32;
            let rhs = parse_java_float_literal(params[1].trim())? as f32;
            (lhs + rhs).to_string()
        }
        "f32_max" => {
            let lhs = parse_java_float_literal(params[0].trim())? as f32;
            let rhs = parse_java_float_literal(params[1].trim())? as f32;
            java_max_f32(lhs, rhs).to_string()
        }
        "f32_min" => {
            let lhs = parse_java_float_literal(params[0].trim())? as f32;
            let rhs = parse_java_float_literal(params[1].trim())? as f32;
            java_min_f32(lhs, rhs).to_string()
        }
        "f64_sum" => {
            let lhs = parse_java_float_literal(params[0].trim())?;
            let rhs = parse_java_float_literal(params[1].trim())?;
            (lhs + rhs).to_string()
        }
        "f64_max" => {
            let lhs = parse_java_float_literal(params[0].trim())?;
            let rhs = parse_java_float_literal(params[1].trim())?;
            java_max_f64(lhs, rhs).to_string()
        }
        "f64_min" => {
            let lhs = parse_java_float_literal(params[0].trim())?;
            let rhs = parse_java_float_literal(params[1].trim())?;
            java_min_f64(lhs, rhs).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_numeric_parse_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if let Some(alias) = java_unsigned_numeric_parse_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_numeric_radix_parse_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_numeric_decode_alias(expression, return_type, arg_types) {
        return Some(alias);
    }

    let (value, expected_return_type, alias) = if let Some(value) = expression
        .strip_prefix("byte.parsebyte(")
        .or_else(|| expression.strip_prefix("byte.valueof("))
    {
        (value, "tinyint", "parse_i8_text")
    } else if let Some(value) = expression
        .strip_prefix("short.parseshort(")
        .or_else(|| expression.strip_prefix("short.valueof("))
    {
        (value, "smallint", "parse_i16_text")
    } else if let Some(value) = expression
        .strip_prefix("integer.parseint(")
        .or_else(|| expression.strip_prefix("integer.valueof("))
    {
        (value, "int", "parse_i32_text")
    } else if let Some(value) = expression
        .strip_prefix("long.parselong(")
        .or_else(|| expression.strip_prefix("long.valueof("))
    {
        (value, "bigint", "parse_i64_text")
    } else if let Some(value) = expression
        .strip_prefix("float.parsefloat(")
        .or_else(|| expression.strip_prefix("float.valueof("))
    {
        (value, "float", "parse_f32_text")
    } else if let Some(value) = expression
        .strip_prefix("double.parsedouble(")
        .or_else(|| expression.strip_prefix("double.valueof("))
    {
        (value, "double", "parse_f64_text")
    } else {
        return None;
    };
    if return_type.to_ascii_lowercase() != expected_return_type {
        return None;
    }
    if !matches!(
        arg_types.first().map(|value| value.to_ascii_lowercase()),
        Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
    ) {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_boxed_primitive_value_of_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (value, expected_type) = if let Some(value) = expression.strip_prefix("byte.valueof(") {
        (value, "tinyint")
    } else if let Some(value) = expression.strip_prefix("short.valueof(") {
        (value, "smallint")
    } else if let Some(value) = expression.strip_prefix("integer.valueof(") {
        (value, "int")
    } else if let Some(value) = expression.strip_prefix("long.valueof(") {
        (value, "bigint")
    } else if let Some(value) = expression.strip_prefix("float.valueof(") {
        (value, "float")
    } else if let Some(value) = expression.strip_prefix("double.valueof(") {
        (value, "double")
    } else if let Some(value) = expression.strip_prefix("boolean.valueof(") {
        (value, "boolean")
    } else {
        return None;
    };
    if return_type.to_ascii_lowercase() != expected_type
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if arg_type == expected_type
        )
    {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some("identity".to_string())
}

fn java_boxed_primitive_value_of_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    if return_type.eq_ignore_ascii_case("boolean") {
        return Some(format!(
            "constant:{}",
            java_boxed_boolean_value_of_literal(expression)?
        ));
    }
    let expected_type =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "byte.valueof(") {
            value.strip_suffix(')')?;
            "tinyint"
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.valueof(") {
            value.strip_suffix(')')?;
            "smallint"
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.valueof(") {
            value.strip_suffix(')')?;
            "int"
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.valueof(") {
            value.strip_suffix(')')?;
            "bigint"
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.valueof(") {
            value.strip_suffix(')')?;
            "float"
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.valueof(") {
            value.strip_suffix(')')?;
            "double"
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case(expected_type) {
        return None;
    }
    let value = java_boxed_value_of_literal(expression)?;
    let literal = match (expected_type, value) {
        ("tinyint", JavaBoxedNumber::I64(value)) => i8::try_from(value).ok()?.to_string(),
        ("smallint", JavaBoxedNumber::I64(value)) => i16::try_from(value).ok()?.to_string(),
        ("int", JavaBoxedNumber::I64(value)) => i32::try_from(value).ok()?.to_string(),
        ("bigint", JavaBoxedNumber::I64(value)) => value.to_string(),
        ("float", JavaBoxedNumber::F32(value)) => java_f32_to_string(value),
        ("double", JavaBoxedNumber::F64(value)) => java_f64_to_string(value),
        _ => return None,
    };
    Some(format!("constant:{literal}"))
}

fn java_numeric_parse_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if let Some(alias) = java_floating_parse_literal_alias(expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_unsigned_numeric_parse_literal_alias(expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_numeric_decode_literal_alias(expression, return_type) {
        return Some(alias);
    }
    let (value, expected_return_type, min, max) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "byte.parsebyte(")
            .or_else(|| strip_ascii_prefix_ignore_case(expression, "byte.valueof("))
    {
        (value, "tinyint", i8::MIN as i64, i8::MAX as i64)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.parseshort(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "short.valueof("))
    {
        (value, "smallint", i16::MIN as i64, i16::MAX as i64)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.parseint(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "integer.valueof("))
    {
        (value, "int", i32::MIN as i64, i32::MAX as i64)
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.parselong(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "long.valueof("))
    {
        (value, "bigint", i64::MIN, i64::MAX)
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if !(1..=2).contains(&params.len()) {
        return None;
    }
    let digits = parse_java_string_literal(params[0].trim())?;
    let radix = if let Some(radix) = params.get(1) {
        radix.trim().parse::<u32>().ok()?
    } else {
        10
    };
    if !(2..=36).contains(&radix) {
        return None;
    }
    let parsed = i64::from_str_radix(&digits, radix).ok()?;
    if !(min..=max).contains(&parsed) {
        return None;
    }
    Some(format!("constant:{parsed}"))
}

fn java_floating_parse_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, expected_return_type) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "float.parsefloat(")
            .or_else(|| strip_ascii_prefix_ignore_case(expression, "float.valueof("))
    {
        (value, "float")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.parsedouble(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "double.valueof("))
    {
        (value, "double")
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 1 {
        return None;
    }
    let literal = parse_java_string_literal(params[0].trim())?;
    if expected_return_type == "float" {
        literal.parse::<f32>().ok()?;
    } else {
        literal.parse::<f64>().ok()?;
    }
    Some(format!("constant:{literal}"))
}

fn java_unsigned_numeric_parse_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    let (value, expected_return_type, method, int_width) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.parseunsignedint(")
    {
        (value, "int", "Integer.parseUnsignedInt", 32)
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "long.parseunsignedlong(")
    {
        (value, "bigint", "Long.parseUnsignedLong", 64)
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if !(1..=2).contains(&params.len()) {
        return None;
    }
    let digits = parse_java_string_literal(params[0].trim())?;
    let radix = if let Some(radix) = params.get(1) {
        radix.trim().parse::<u32>().ok()?
    } else {
        10
    };
    let parsed = parse_unsigned_digits(&digits, radix, method).ok()?;
    let signed = if int_width == 32 {
        u32::try_from(parsed).ok()? as i32 as i64
    } else {
        parsed as i64
    };
    Some(format!("constant:{signed}"))
}

fn java_numeric_decode_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, expected_return_type, min, max, method) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "byte.decode(") {
            (
                value,
                "tinyint",
                i8::MIN as i128,
                i8::MAX as i128,
                "Byte.decode",
            )
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.decode(") {
            (
                value,
                "smallint",
                i16::MIN as i128,
                i16::MAX as i128,
                "Short.decode",
            )
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.decode(") {
            (
                value,
                "int",
                i32::MIN as i128,
                i32::MAX as i128,
                "Integer.decode",
            )
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.decode(") {
            (
                value,
                "bigint",
                i64::MIN as i128,
                i64::MAX as i128,
                "Long.decode",
            )
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 1 {
        return None;
    }
    let digits = parse_java_string_literal(params[0].trim())?;
    let parsed = decode_java_integer_literal(&digits, method).ok()?;
    if !(min..=max).contains(&parsed) {
        return None;
    }
    Some(format!("constant:{parsed}"))
}

fn java_numeric_radix_parse_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (value, expected_return_type, alias_prefix) = if let Some(value) = expression
        .strip_prefix("byte.parsebyte(")
        .or_else(|| expression.strip_prefix("byte.valueof("))
    {
        (value, "tinyint", "parse_i8_text_radix")
    } else if let Some(value) = expression
        .strip_prefix("short.parseshort(")
        .or_else(|| expression.strip_prefix("short.valueof("))
    {
        (value, "smallint", "parse_i16_text_radix")
    } else if let Some(value) = expression
        .strip_prefix("integer.parseint(")
        .or_else(|| expression.strip_prefix("integer.valueof("))
    {
        (value, "int", "parse_i32_text_radix")
    } else if let Some(value) = expression
        .strip_prefix("long.parselong(")
        .or_else(|| expression.strip_prefix("long.valueof("))
    {
        (value, "bigint", "parse_i64_text_radix")
    } else {
        return None;
    };
    if return_type.to_ascii_lowercase() != expected_return_type
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
        )
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let (value, radix) = params.split_once(',')?;
    if !is_java_identifier(value.trim()) {
        return None;
    }
    let radix = radix.trim().parse::<u32>().ok()?;
    if !(2..=36).contains(&radix) {
        return None;
    }
    Some(format!("{alias_prefix}:{radix}"))
}

fn java_unsigned_numeric_parse_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (value, expected_return_type, alias, literal_alias, args_alias) =
        if let Some(value) = expression.strip_prefix("integer.parseunsignedint(") {
            (
                value,
                "int",
                "parse_unsigned_i32_text",
                "parse_unsigned_i32_text_radix",
                "parse_unsigned_i32_text_radix_args",
            )
        } else if let Some(value) = expression.strip_prefix("long.parseunsignedlong(") {
            (
                value,
                "bigint",
                "parse_unsigned_i64_text",
                "parse_unsigned_i64_text_radix",
                "parse_unsigned_i64_text_radix_args",
            )
        } else {
            return None;
        };
    if return_type.to_ascii_lowercase() != expected_return_type
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
        )
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let Some((value, radix)) = params.split_once(',') else {
        return is_java_identifier(params.trim()).then(|| alias.to_string());
    };
    if !is_java_identifier(value.trim()) {
        return None;
    }
    let radix = radix.trim();
    if is_java_identifier(radix) {
        return matches!(
            arg_types.get(1).map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if arg_type == "int"
        )
        .then(|| args_alias.to_string());
    }
    let radix = radix.parse::<u32>().ok()?;
    if !(2..=36).contains(&radix) {
        return None;
    }
    Some(format!("{literal_alias}:{radix}"))
}

fn java_numeric_decode_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (value, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("byte.decode(") {
            (value, "tinyint", "decode_i8_text")
        } else if let Some(value) = expression.strip_prefix("short.decode(") {
            (value, "smallint", "decode_i16_text")
        } else if let Some(value) = expression.strip_prefix("integer.decode(") {
            (value, "int", "decode_i32_text")
        } else if let Some(value) = expression.strip_prefix("long.decode(") {
            (value, "bigint", "decode_i64_text")
        } else {
            return None;
        };
    if return_type.to_ascii_lowercase() != expected_return_type
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
        )
    {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_primitive_to_string_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        return None;
    }
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    if let Some(alias) = java_signed_to_string_radix_alias(expression, &arg_type, arg_types) {
        return Some(alias);
    }
    if let Some(alias) = java_objects_to_string_default_alias(expression, arg_types) {
        return Some(alias);
    }
    if let Some(value) = expression
        .strip_prefix("string.valueof(")
        .or_else(|| expression.strip_prefix("objects.tostring("))
        .or_else(|| expression.strip_prefix("java.util.objects.tostring("))
    {
        let alias = match arg_type.as_str() {
            "tinyint" => "to_string_i8",
            "smallint" => "to_string_i16",
            "int" => "to_string_i32",
            "bigint" | "counter" => "to_string_i64",
            "float" => "to_string_f32",
            "double" => "to_string_f64",
            "boolean" => "to_string_bool",
            "text" | "varchar" | "ascii" => "identity",
            _ => return None,
        };
        let value = value.strip_suffix(')')?.trim();
        if !is_java_identifier(value) {
            return None;
        }
        return Some(alias.to_string());
    }
    let (value, expected_arg_type, alias) =
        if let Some(value) = expression.strip_prefix("byte.tostring(") {
            (value, "tinyint", "to_string_i8")
        } else if let Some(value) = expression.strip_prefix("short.tostring(") {
            (value, "smallint", "to_string_i16")
        } else if let Some(value) = expression.strip_prefix("integer.tostring(") {
            (value, "int", "to_string_i32")
        } else if let Some(value) = expression.strip_prefix("long.tostring(") {
            (value, "bigint", "to_string_i64")
        } else if let Some(value) = expression.strip_prefix("float.tostring(") {
            (value, "float", "to_string_f32")
        } else if let Some(value) = expression.strip_prefix("double.tostring(") {
            (value, "double", "to_string_f64")
        } else if let Some(value) = expression.strip_prefix("boolean.tostring(") {
            (value, "boolean", "to_string_bool")
        } else {
            return None;
        };
    if arg_type != expected_arg_type {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_primitive_to_string_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        return None;
    }
    if let Some(value) = strip_ascii_prefix_ignore_case(expression, "string.valueof(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "objects.tostring("))
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "java.util.objects.tostring("))
    {
        let value = value.strip_suffix(')')?.trim();
        if let Some(literal) = parse_java_string_literal(value) {
            return Some(format!("constant:{literal}"));
        }
        if let Some(value) = parse_java_boolean_literal(value) {
            return Some(format!("constant:{value}"));
        }
        if let Some(value) = parse_java_char_literal(value) {
            return Some(format!("constant:{value}"));
        }
        if let Some(value) = parse_java_integer_literal(value) {
            return Some(format!("constant:{value}"));
        }
        if let Some(value) = parse_java_float_literal(value) {
            let literal = if is_java_f32_literal(value) {
                java_f32_to_string(value as f32)
            } else {
                java_f64_to_string(value)
            };
            return Some(format!("constant:{literal}"));
        }
        return None;
    }

    let (value, kind) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "byte.tostring(")
    {
        (value, "i8")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.tostring(") {
        (value, "i16")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.tostring(") {
        (value, "i32")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.tostring(") {
        (value, "i64")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.tostring(") {
        (value, "f32")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.tostring(") {
        (value, "f64")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.tostring(") {
        (value, "bool")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.tostring(") {
        (value, "char")
    } else {
        return None;
    };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.is_empty() || params.len() > 2 {
        return None;
    }
    let literal = match kind {
        "i8" if params.len() == 1 => parse_java_integer_literal(params[0].trim())
            .and_then(|value| i8::try_from(value).ok().map(|value| value.to_string())),
        "i16" if params.len() == 1 => parse_java_integer_literal(params[0].trim())
            .and_then(|value| i16::try_from(value).ok().map(|value| value.to_string())),
        "i32" => {
            let value = parse_java_integer_literal(params[0].trim())?;
            let value = i32::try_from(value).ok()?;
            let radix = params
                .get(1)
                .and_then(|radix| radix.trim().parse::<i32>().ok())
                .map(java_string_radix)
                .unwrap_or(10);
            Some(format_signed_radix(value as i128, radix))
        }
        "i64" => {
            let value = parse_java_integer_literal(params[0].trim())?;
            let radix = params
                .get(1)
                .and_then(|radix| radix.trim().parse::<i32>().ok())
                .map(java_string_radix)
                .unwrap_or(10);
            Some(format_signed_radix(value as i128, radix))
        }
        "f32" if params.len() == 1 => {
            parse_java_float_literal(params[0].trim()).map(|value| java_f32_to_string(value as f32))
        }
        "f64" if params.len() == 1 => {
            parse_java_float_literal(params[0].trim()).map(java_f64_to_string)
        }
        "bool" if params.len() == 1 => {
            parse_java_boolean_literal(params[0].trim()).map(|value| value.to_string())
        }
        "char" if params.len() == 1 => {
            parse_java_char_literal(params[0].trim()).map(|value| value.to_string())
        }
        _ => None,
    }?;
    Some(format!("constant:{literal}"))
}

fn java_objects_to_string_default_alias(expression: &str, arg_types: &[String]) -> Option<String> {
    let first_arg_is_text = matches!(
        arg_types.first().map(|value| value.to_ascii_lowercase()),
        Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
    );
    if !first_arg_is_text {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "objects.tostring(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "java.util.objects.tostring("))?;
    let params = value.strip_suffix(')')?.trim();
    let params = split_java_args(params)?;
    if params.len() != 2 {
        return None;
    }
    let value = params[0].trim();
    let fallback = params[1].trim();
    if !is_java_identifier(value) {
        return None;
    }
    if is_java_identifier(fallback)
        && matches!(
            arg_types.get(1).map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
        )
    {
        Some("objects_to_string_default_text_args".to_string())
    } else {
        parse_java_string_literal(fallback)
            .map(|literal| format!("objects_to_string_default_text_literal:{literal}"))
    }
}

fn java_signed_to_string_radix_alias(
    expression: &str,
    arg_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let (value, expected_arg_type, literal_alias, args_alias) =
        if let Some(value) = expression.strip_prefix("integer.tostring(") {
            (
                value,
                "int",
                "to_string_i32_radix",
                "to_string_i32_radix_args",
            )
        } else if let Some(value) = expression.strip_prefix("long.tostring(") {
            (
                value,
                "bigint",
                "to_string_i64_radix",
                "to_string_i64_radix_args",
            )
        } else {
            return None;
        };
    if arg_type != expected_arg_type {
        return None;
    }
    java_radix_string_alias(value, arg_types, literal_alias, args_alias)
}

fn java_primitive_compare_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "int" || arg_types.len() < 2 {
        return None;
    }
    let (value, expected_arg_type, alias) =
        if let Some(value) = expression.strip_prefix("byte.compare(") {
            (value, "tinyint", "java_compare_i8")
        } else if let Some(value) = expression.strip_prefix("short.compare(") {
            (value, "smallint", "java_compare_i16")
        } else if let Some(value) = expression.strip_prefix("integer.compare(") {
            (value, "int", "java_compare_i32")
        } else if let Some(value) = expression.strip_prefix("integer.compareunsigned(") {
            (value, "int", "java_compare_unsigned_i32")
        } else if let Some(value) = expression.strip_prefix("long.compare(") {
            (value, "bigint", "java_compare_i64")
        } else if let Some(value) = expression.strip_prefix("long.compareunsigned(") {
            (value, "bigint", "java_compare_unsigned_i64")
        } else if let Some(value) = expression.strip_prefix("boolean.compare(") {
            (value, "boolean", "java_compare_bool")
        } else if let Some(value) = expression.strip_prefix("float.compare(") {
            (value, "float", "java_compare_f32")
        } else if let Some(value) = expression.strip_prefix("double.compare(") {
            (value, "double", "java_compare_f64")
        } else {
            return None;
        };
    if !arg_types
        .iter()
        .take(2)
        .all(|arg_type| arg_type.eq_ignore_ascii_case(expected_arg_type))
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let (lhs, rhs) = params.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some(alias.to_string())
}

fn java_primitive_compare_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let (value, kind) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "byte.compare(")
    {
        (value, "i8")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.compare(") {
        (value, "i16")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.compare(") {
        (value, "i32")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.compareunsigned(")
    {
        (value, "u32")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.compare(") {
        (value, "i64")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.compareunsigned(")
    {
        (value, "u64")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.compare(") {
        (value, "bool")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.compare(") {
        (value, "char")
    } else {
        return None;
    };
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let comparison = match kind {
        "i8" => {
            let lhs = i8::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i8::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "i16" => {
            let lhs = i16::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i16::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "i32" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "u32" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()? as u32;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "i64" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "u64" => {
            let lhs = parse_java_integer_literal(params[0].trim())? as u64;
            let rhs = parse_java_integer_literal(params[1].trim())? as u64;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "bool" => {
            let lhs = parse_java_boolean_literal(params[0].trim())?;
            let rhs = parse_java_boolean_literal(params[1].trim())?;
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        "char" => {
            let lhs = parse_java_char_literal(params[0].trim())?;
            let rhs = parse_java_char_literal(params[1].trim())?;
            java_ordering_to_i32((lhs as u32).cmp(&(rhs as u32)))
        }
        _ => return None,
    };
    Some(format!("constant:{comparison}"))
}

fn java_ordering_to_i32(ordering: std::cmp::Ordering) -> i32 {
    match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn java_boxed_primitive_compare_to_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "int" || arg_types.len() < 2 {
        return None;
    }
    let method_index = expression.find(".compareto(")?;
    let lhs = expression[..method_index].trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    let rhs_start = method_index + ".compareto(".len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    if !is_java_identifier(rhs) {
        return None;
    }
    let first_arg_type = arg_types.first()?.to_ascii_lowercase();
    if !arg_types
        .iter()
        .take(2)
        .all(|arg_type| arg_type.eq_ignore_ascii_case(&first_arg_type))
    {
        return None;
    }
    match first_arg_type.as_str() {
        "tinyint" => Some("java_compare_i8".to_string()),
        "smallint" => Some("java_compare_i16".to_string()),
        "int" => Some("java_compare_i32".to_string()),
        "bigint" | "counter" => Some("java_compare_i64".to_string()),
        "boolean" => Some("java_compare_bool".to_string()),
        "float" => Some("java_compare_f32".to_string()),
        "double" => Some("java_compare_f64".to_string()),
        _ => None,
    }
}

fn java_boxed_primitive_compare_to_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let lower = expression.to_ascii_lowercase();
    let method_index = lower.find(".compareto(")?;
    let lhs = expression[..method_index].trim();
    let rhs_start = method_index + ".compareto(".len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    let (lhs_type, lhs_value) = java_boxed_compare_value_of_literal(lhs)?;
    let (rhs_type, rhs_value) = java_boxed_compare_value_of_literal(rhs)?;
    if lhs_type != rhs_type {
        return None;
    }
    let comparison = match (lhs_value, rhs_value) {
        (JavaBoxedCompareValue::I64(lhs), JavaBoxedCompareValue::I64(rhs)) => {
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        (JavaBoxedCompareValue::Bool(lhs), JavaBoxedCompareValue::Bool(rhs)) => {
            java_ordering_to_i32(lhs.cmp(&rhs))
        }
        (JavaBoxedCompareValue::F32(lhs), JavaBoxedCompareValue::F32(rhs)) => {
            java_compare_f32_values(lhs, rhs)
        }
        (JavaBoxedCompareValue::F64(lhs), JavaBoxedCompareValue::F64(rhs)) => {
            java_compare_f64_values(lhs, rhs)
        }
        _ => return None,
    };
    Some(format!("constant:{comparison}"))
}

#[derive(Debug, Clone, Copy)]
enum JavaBoxedCompareValue {
    I64(i64),
    Bool(bool),
    F32(f32),
    F64(f64),
}

fn java_boxed_compare_value_of_literal(
    expression: &str,
) -> Option<(&'static str, JavaBoxedCompareValue)> {
    let (value, source_type) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "byte.valueof(") {
            (value, "tinyint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.valueof(") {
            (value, "smallint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.valueof(") {
            (value, "int")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.valueof(") {
            (value, "bigint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.valueof(") {
            (value, "boolean")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.valueof(") {
            (value, "float")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.valueof(") {
            (value, "double")
        } else {
            return None;
        };
    let value = value.strip_suffix(')')?.trim();
    let value = match source_type {
        "tinyint" => {
            JavaBoxedCompareValue::I64(i8::try_from(parse_java_integer_literal(value)?).ok()? as i64)
        }
        "smallint" => JavaBoxedCompareValue::I64(
            i16::try_from(parse_java_integer_literal(value)?).ok()? as i64,
        ),
        "int" => JavaBoxedCompareValue::I64(
            i32::try_from(parse_java_integer_literal(value)?).ok()? as i64,
        ),
        "bigint" => JavaBoxedCompareValue::I64(parse_java_integer_literal(value)?),
        "boolean" => JavaBoxedCompareValue::Bool(parse_java_boolean_literal(value)?),
        "float" => JavaBoxedCompareValue::F32(parse_java_float_literal(value)? as f32),
        "double" => JavaBoxedCompareValue::F64(parse_java_float_literal(value)?),
        _ => return None,
    };
    Some((source_type, value))
}

fn java_primitive_hash_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "int" {
        return None;
    }
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let (value, expected_arg_type, alias) =
        if let Some(value) = expression.strip_prefix("byte.hashcode(") {
            (value, "tinyint", "java_hash_i8")
        } else if let Some(value) = expression.strip_prefix("short.hashcode(") {
            (value, "smallint", "java_hash_i16")
        } else if let Some(value) = expression.strip_prefix("integer.hashcode(") {
            (value, "int", "java_hash_i32")
        } else if let Some(value) = expression.strip_prefix("long.hashcode(") {
            (value, "bigint", "java_hash_i64")
        } else if let Some(value) = expression.strip_prefix("boolean.hashcode(") {
            (value, "boolean", "java_hash_bool")
        } else if let Some(value) = expression.strip_prefix("float.hashcode(") {
            (value, "float", "java_hash_f32")
        } else if let Some(value) = expression.strip_prefix("double.hashcode(") {
            (value, "double", "java_hash_f64")
        } else {
            return None;
        };
    if arg_type != expected_arg_type {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_primitive_hash_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let (value, kind) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "byte.hashcode(")
    {
        (value, "i8")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.hashcode(") {
        (value, "i16")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.hashcode(") {
        (value, "i32")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.hashcode(") {
        (value, "i64")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.hashcode(") {
        (value, "bool")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.hashcode(") {
        (value, "char")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.hashcode(") {
        (value, "f32")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.hashcode(") {
        (value, "f64")
    } else {
        return None;
    };
    let value = value.strip_suffix(')')?.trim();
    let hash = match kind {
        "i8" => i8::try_from(parse_java_integer_literal(value)?).ok()? as i32,
        "i16" => i16::try_from(parse_java_integer_literal(value)?).ok()? as i32,
        "i32" => i32::try_from(parse_java_integer_literal(value)?).ok()?,
        "i64" => {
            let value = parse_java_integer_literal(value)? as u64;
            (value ^ (value >> 32)) as i32
        }
        "bool" => {
            if parse_java_boolean_literal(value)? {
                1231
            } else {
                1237
            }
        }
        "char" => parse_java_char_literal(value)? as i32,
        "f32" => java_f32_to_i32_bits(parse_java_float_literal(value)? as f32),
        "f64" => {
            let value = java_f64_to_i64_bits(parse_java_float_literal(value)?) as u64;
            (value ^ (value >> 32)) as i32
        }
        _ => return None,
    };
    Some(format!("constant:{hash}"))
}

fn java_character_predicate_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("boolean") {
        return None;
    }
    let (value, kind) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isdigit(")
    {
        (value, "digit")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.isletter(") {
        (value, "letter")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isletterordigit(")
    {
        (value, "letter_or_digit")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.isuppercase(")
    {
        (value, "upper")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.islowercase(")
    {
        (value, "lower")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.iswhitespace(")
    {
        (value, "whitespace")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.isspacechar(")
    {
        (value, "space_char")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isisocontrol(")
    {
        (value, "iso_control")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isjavaidentifierstart(")
    {
        (value, "java_identifier_start")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isjavaidentifierpart(")
    {
        (value, "java_identifier_part")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.isalphabetic(")
    {
        (value, "alphabetic")
    } else {
        return None;
    };
    let ch = parse_java_char_literal(value.strip_suffix(')')?.trim())?;
    if !ch.is_ascii() && kind != "iso_control" {
        return None;
    }
    let predicate = match kind {
        "digit" => ch.is_ascii_digit(),
        "letter" | "alphabetic" => ch.is_ascii_alphabetic(),
        "letter_or_digit" => ch.is_ascii_alphanumeric(),
        "upper" => ch.is_ascii_uppercase(),
        "lower" => ch.is_ascii_lowercase(),
        "whitespace" => matches!(ch, ' ' | '\t' | '\n' | '\u{000B}' | '\u{000C}' | '\r'),
        "space_char" => ch == ' ',
        "iso_control" => {
            let code_point = ch as u32;
            code_point <= 0x1f || (0x7f..=0x9f).contains(&code_point)
        }
        "java_identifier_start" => ch == '$' || ch == '_' || ch.is_ascii_alphabetic(),
        "java_identifier_part" => ch == '$' || ch == '_' || ch.is_ascii_alphanumeric(),
        _ => return None,
    };
    Some(format!("constant:{predicate}"))
}

fn java_character_numeric_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.digit(") {
        let params = split_java_args(value.strip_suffix(')')?.trim())?;
        if params.len() != 2 {
            return None;
        }
        let ch = parse_java_char_literal(params[0].trim())?;
        let radix = params[1].trim().parse::<i32>().ok()?;
        let digit = java_ascii_character_digit(ch, radix);
        return Some(format!("constant:{digit}"));
    }
    let (value, kind) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "character.getnumericvalue(")
    {
        (value, "numeric")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.touppercase(")
    {
        (value, "upper")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.tolowercase(")
    {
        (value, "lower")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "character.totitlecase(")
    {
        (value, "title")
    } else {
        return None;
    };
    let ch = parse_java_char_literal(value.strip_suffix(')')?.trim())?;
    if !ch.is_ascii() {
        return None;
    }
    let value = match kind {
        "numeric" => java_ascii_character_numeric_value(ch),
        "upper" | "title" => ch.to_ascii_uppercase() as i32,
        "lower" => ch.to_ascii_lowercase() as i32,
        _ => return None,
    };
    Some(format!("constant:{value}"))
}

fn java_ascii_character_digit(ch: char, radix: i32) -> i32 {
    if !(2..=36).contains(&radix) || !ch.is_ascii() {
        return -1;
    }
    let value = java_ascii_character_numeric_value(ch);
    if (0..radix).contains(&value) {
        value
    } else {
        -1
    }
}

fn java_ascii_character_numeric_value(ch: char) -> i32 {
    match ch {
        '0'..='9' => ch as i32 - '0' as i32,
        'A'..='Z' => ch as i32 - 'A' as i32 + 10,
        'a'..='z' => ch as i32 - 'a' as i32 + 10,
        _ => -1,
    }
}

fn java_boxed_primitive_hash_code_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "int" {
        return None;
    }
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let lhs = if let Some(lhs) = expression.strip_suffix(".hashcode()") {
        lhs
    } else if let Some(value) = expression
        .strip_prefix("objects.hashcode(")
        .or_else(|| expression.strip_prefix("java.util.objects.hashcode("))
    {
        value.strip_suffix(')')?
    } else {
        return None;
    }
    .trim();
    if !is_java_identifier(lhs) {
        return None;
    }
    match arg_type.as_str() {
        "tinyint" => Some("java_hash_i8".to_string()),
        "smallint" => Some("java_hash_i16".to_string()),
        "int" => Some("java_hash_i32".to_string()),
        "bigint" | "counter" => Some("java_hash_i64".to_string()),
        "boolean" => Some("java_hash_bool".to_string()),
        "float" => Some("java_hash_f32".to_string()),
        "double" => Some("java_hash_f64".to_string()),
        "text" | "varchar" | "ascii" => Some("java_hash_text".to_string()),
        _ => None,
    }
}

fn java_boxed_primitive_hash_code_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let value = if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".hashcode()") {
        java_literal_hash_code(receiver.trim())?
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "objects.hashcode(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "java.util.objects.hashcode("))
    {
        java_literal_hash_code(value.strip_suffix(')')?.trim())?
    } else {
        return None;
    };
    Some(format!("constant:{value}"))
}

fn java_literal_hash_code(expression: &str) -> Option<i32> {
    if expression.eq_ignore_ascii_case("null") {
        return Some(0);
    }
    if let Some(value) = java_boxed_boolean_value_of_literal(expression) {
        return Some(if value { 1231 } else { 1237 });
    }
    if let Some(value) = java_boxed_value_of_literal(expression) {
        return Some(java_boxed_hash_code(value));
    }
    if let Some(value) = parse_java_string_literal(expression) {
        return Some(java_string_hash_code(&value));
    }
    if let Some(value) = parse_java_boolean_literal(expression) {
        return Some(if value { 1231 } else { 1237 });
    }
    if let Some(value) = parse_java_integer_literal(expression) {
        return Some(i32::try_from(value).ok().unwrap_or_else(|| {
            let value = value as u64;
            (value ^ (value >> 32)) as i32
        }));
    }
    if let Some(value) = parse_java_float_literal(expression) {
        if is_java_f32_literal(expression) {
            return Some(java_f32_to_i32_bits(value as f32));
        }
        let bits = java_f64_to_i64_bits(value) as u64;
        return Some((bits ^ (bits >> 32)) as i32);
    }
    None
}

fn java_boxed_hash_code(value: JavaBoxedNumber) -> i32 {
    match value {
        JavaBoxedNumber::I64(value) => i32::try_from(value).ok().unwrap_or_else(|| {
            let value = value as u64;
            (value ^ (value >> 32)) as i32
        }),
        JavaBoxedNumber::F32(value) => java_f32_to_i32_bits(value),
        JavaBoxedNumber::F64(value) => {
            let value = java_f64_to_i64_bits(value) as u64;
            (value ^ (value >> 32)) as i32
        }
    }
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(unit as i32)
    })
}

fn java_unsigned_conversion_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    if let Some(alias) =
        java_unsigned_to_string_radix_alias(expression, return_type, &arg_type, arg_types)
    {
        return Some(alias);
    }
    let (value, expected_return_type, expected_arg_type, alias) =
        if let Some(value) = expression.strip_prefix("byte.tounsignedint(") {
            (value, "int", "tinyint", "to_unsigned_int_i8")
        } else if let Some(value) = expression.strip_prefix("byte.tounsignedlong(") {
            (value, "bigint", "tinyint", "to_unsigned_long_i8")
        } else if let Some(value) = expression.strip_prefix("short.tounsignedint(") {
            (value, "int", "smallint", "to_unsigned_int_i16")
        } else if let Some(value) = expression.strip_prefix("short.tounsignedlong(") {
            (value, "bigint", "smallint", "to_unsigned_long_i16")
        } else if let Some(value) = expression.strip_prefix("integer.tounsignedlong(") {
            (value, "bigint", "int", "to_unsigned_long_i32")
        } else if let Some(value) = expression.strip_prefix("integer.tounsignedstring(") {
            (value, "text", "int", "to_unsigned_string_i32")
        } else if let Some(value) = expression.strip_prefix("long.tounsignedstring(") {
            (value, "text", "bigint", "to_unsigned_string_i64")
        } else if let Some(value) = expression.strip_prefix("integer.tobinarystring(") {
            (value, "text", "int", "to_binary_string_i32")
        } else if let Some(value) = expression.strip_prefix("long.tobinarystring(") {
            (value, "text", "bigint", "to_binary_string_i64")
        } else if let Some(value) = expression.strip_prefix("integer.tooctalstring(") {
            (value, "text", "int", "to_octal_string_i32")
        } else if let Some(value) = expression.strip_prefix("long.tooctalstring(") {
            (value, "text", "bigint", "to_octal_string_i64")
        } else if let Some(value) = expression.strip_prefix("integer.tohexstring(") {
            (value, "text", "int", "to_hex_string_i32")
        } else if let Some(value) = expression.strip_prefix("long.tohexstring(") {
            (value, "text", "bigint", "to_hex_string_i64")
        } else {
            return None;
        };
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        value if value == expected_return_type
            || (expected_return_type == "text" && matches!(value, "varchar" | "ascii"))
    ) || arg_type != expected_arg_type
    {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_unsigned_conversion_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    let (value, expected_return_type, converted) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "byte.tounsignedint(")
    {
        let value = i8::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u8 as i32;
        (value.to_string(), "int", value.to_string())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "byte.tounsignedlong(") {
        let value = i8::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u8 as i64;
        (value.to_string(), "bigint", value.to_string())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.tounsignedint(") {
        let value = i16::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u16 as i32;
        (value.to_string(), "int", value.to_string())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.tounsignedlong(")
    {
        let value = i16::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u16 as i64;
        (value.to_string(), "bigint", value.to_string())
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.tounsignedlong(")
    {
        let value = i32::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u32 as i64;
        (value.to_string(), "bigint", value.to_string())
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.tounsignedstring(")
    {
        (
            java_unsigned_i32_literal_to_string(value)?,
            "text",
            java_unsigned_i32_literal_to_string(value)?,
        )
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.tounsignedstring(")
    {
        (
            java_unsigned_i64_literal_to_string(value)?,
            "text",
            java_unsigned_i64_literal_to_string(value)?,
        )
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.tobinarystring(")
    {
        let value = i32::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u32 as u64;
        (format_unsigned_radix(value, 2), "text", String::new())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.tobinarystring(") {
        let value = parse_java_integer_literal(value.strip_suffix(')')?.trim())? as u64;
        (format_unsigned_radix(value, 2), "text", String::new())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.tooctalstring(")
    {
        let value = i32::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u32 as u64;
        (format_unsigned_radix(value, 8), "text", String::new())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.tooctalstring(") {
        let value = parse_java_integer_literal(value.strip_suffix(')')?.trim())? as u64;
        (format_unsigned_radix(value, 8), "text", String::new())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.tohexstring(") {
        let value = i32::try_from(parse_java_integer_literal(value.strip_suffix(')')?.trim())?)
            .ok()? as u32 as u64;
        (format_unsigned_radix(value, 16), "text", String::new())
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.tohexstring(") {
        let value = parse_java_integer_literal(value.strip_suffix(')')?.trim())? as u64;
        (format_unsigned_radix(value, 16), "text", String::new())
    } else {
        return None;
    };
    if !(return_type == expected_return_type
        || (expected_return_type == "text" && matches!(return_type.as_str(), "varchar" | "ascii")))
    {
        return None;
    }
    Some(format!(
        "constant:{}",
        if converted.is_empty() {
            value
        } else {
            converted
        }
    ))
}

fn java_unsigned_i32_literal_to_string(value: &str) -> Option<String> {
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.is_empty() || params.len() > 2 {
        return None;
    }
    let value = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32 as u64;
    let radix = params
        .get(1)
        .and_then(|radix| radix.trim().parse::<i32>().ok())
        .map(java_string_radix)
        .unwrap_or(10);
    Some(format_unsigned_radix(value, radix))
}

fn java_unsigned_i64_literal_to_string(value: &str) -> Option<String> {
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.is_empty() || params.len() > 2 {
        return None;
    }
    let value = parse_java_integer_literal(params[0].trim())? as u64;
    let radix = params
        .get(1)
        .and_then(|radix| radix.trim().parse::<i32>().ok())
        .map(java_string_radix)
        .unwrap_or(10);
    Some(format_unsigned_radix(value, radix))
}

fn java_unsigned_to_string_radix_alias(
    expression: &str,
    return_type: &str,
    arg_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        return None;
    }
    let (value, expected_arg_type, literal_alias, args_alias) =
        if let Some(value) = expression.strip_prefix("integer.tounsignedstring(") {
            (
                value,
                "int",
                "to_unsigned_string_i32_radix",
                "to_unsigned_string_i32_radix_args",
            )
        } else if let Some(value) = expression.strip_prefix("long.tounsignedstring(") {
            (
                value,
                "bigint",
                "to_unsigned_string_i64_radix",
                "to_unsigned_string_i64_radix_args",
            )
        } else {
            return None;
        };
    if arg_type != expected_arg_type {
        return None;
    }
    java_radix_string_alias(value, arg_types, literal_alias, args_alias)
}

fn java_radix_string_alias(
    value: &str,
    arg_types: &[String],
    literal_alias: &str,
    args_alias: &str,
) -> Option<String> {
    let params = value.strip_suffix(')')?;
    let (value, radix) = params.split_once(',')?;
    if !is_java_identifier(value.trim()) {
        return None;
    }
    let radix = radix.trim();
    if is_java_identifier(radix) {
        if matches!(
            arg_types.get(1).map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if arg_type == "int"
        ) {
            return Some(args_alias.to_string());
        }
        return None;
    }
    let radix = radix.parse::<i32>().ok()?;
    Some(format!("{literal_alias}:{radix}"))
}

fn java_bit_op_alias(expression: &str, return_type: &str, arg_types: &[String]) -> Option<String> {
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let (value, expected_arg_type, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("integer.bitcount(") {
            (value, "int", "int", "bit_count_i32")
        } else if let Some(value) = expression.strip_prefix("integer.numberofleadingzeros(") {
            (value, "int", "int", "leading_zeros_i32")
        } else if let Some(value) = expression.strip_prefix("integer.numberoftrailingzeros(") {
            (value, "int", "int", "trailing_zeros_i32")
        } else if let Some(value) = expression.strip_prefix("integer.highestonebit(") {
            (value, "int", "int", "highest_one_bit_i32")
        } else if let Some(value) = expression.strip_prefix("integer.lowestonebit(") {
            (value, "int", "int", "lowest_one_bit_i32")
        } else if let Some(value) = expression.strip_prefix("integer.reverse(") {
            (value, "int", "int", "reverse_bits_i32")
        } else if let Some(value) = expression.strip_prefix("integer.reversebytes(") {
            (value, "int", "int", "reverse_bytes_i32")
        } else if let Some(value) = expression.strip_prefix("long.bitcount(") {
            (value, "bigint", "int", "bit_count_i64")
        } else if let Some(value) = expression.strip_prefix("long.numberofleadingzeros(") {
            (value, "bigint", "int", "leading_zeros_i64")
        } else if let Some(value) = expression.strip_prefix("long.numberoftrailingzeros(") {
            (value, "bigint", "int", "trailing_zeros_i64")
        } else if let Some(value) = expression.strip_prefix("long.highestonebit(") {
            (value, "bigint", "bigint", "highest_one_bit_i64")
        } else if let Some(value) = expression.strip_prefix("long.lowestonebit(") {
            (value, "bigint", "bigint", "lowest_one_bit_i64")
        } else if let Some(value) = expression.strip_prefix("long.reverse(") {
            (value, "bigint", "bigint", "reverse_bits_i64")
        } else if let Some(value) = expression.strip_prefix("long.reversebytes(") {
            (value, "bigint", "bigint", "reverse_bytes_i64")
        } else {
            return None;
        };
    if arg_type != expected_arg_type || return_type.to_ascii_lowercase() != expected_return_type {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_bit_op_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, kind, expected_return_type) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.bitcount(")
    {
        (value, "i32_bit_count", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.numberofleadingzeros(")
    {
        (value, "i32_leading_zeros", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.numberoftrailingzeros(")
    {
        (value, "i32_trailing_zeros", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.highestonebit(")
    {
        (value, "i32_highest_one_bit", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.lowestonebit(")
    {
        (value, "i32_lowest_one_bit", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.reverse(") {
        (value, "i32_reverse", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.reversebytes(")
    {
        (value, "i32_reverse_bytes", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.bitcount(") {
        (value, "i64_bit_count", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "long.numberofleadingzeros(")
    {
        (value, "i64_leading_zeros", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "long.numberoftrailingzeros(")
    {
        (value, "i64_trailing_zeros", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.highestonebit(") {
        (value, "i64_highest_one_bit", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.lowestonebit(") {
        (value, "i64_lowest_one_bit", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.reverse(") {
        (value, "i64_reverse", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.reversebytes(") {
        (value, "i64_reverse_bytes", "bigint")
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let raw_value = value.strip_suffix(')')?.trim();
    let folded = if kind.starts_with("i32_") {
        let value = i32::try_from(parse_java_integer_literal(raw_value)?).ok()? as u32;
        java_i32_bit_op_literal(kind, value).to_string()
    } else {
        let value = parse_java_integer_literal(raw_value)? as u64;
        java_i64_bit_op_literal(kind, value).to_string()
    };
    Some(format!("constant:{folded}"))
}

fn java_i32_bit_op_literal(kind: &str, value: u32) -> i32 {
    match kind {
        "i32_bit_count" => value.count_ones() as i32,
        "i32_leading_zeros" => value.leading_zeros() as i32,
        "i32_trailing_zeros" => value.trailing_zeros() as i32,
        "i32_highest_one_bit" => {
            if value == 0 {
                0
            } else {
                (1u32 << (31 - value.leading_zeros())) as i32
            }
        }
        "i32_lowest_one_bit" => (value & value.wrapping_neg()) as i32,
        "i32_reverse" => value.reverse_bits() as i32,
        "i32_reverse_bytes" => (value as i32).swap_bytes(),
        _ => 0,
    }
}

fn java_i64_bit_op_literal(kind: &str, value: u64) -> i64 {
    match kind {
        "i64_bit_count" => value.count_ones() as i64,
        "i64_leading_zeros" => value.leading_zeros() as i64,
        "i64_trailing_zeros" => value.trailing_zeros() as i64,
        "i64_highest_one_bit" => {
            if value == 0 {
                0
            } else {
                (1u64 << (63 - value.leading_zeros())) as i64
            }
        }
        "i64_lowest_one_bit" => (value & value.wrapping_neg()) as i64,
        "i64_reverse" => value.reverse_bits() as i64,
        "i64_reverse_bytes" => (value as i64).swap_bytes(),
        _ => 0,
    }
}

fn java_bit_rotate_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (value, expected_arg_type, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("integer.rotateleft(") {
            (value, "int", "int", "rotate_left_i32")
        } else if let Some(value) = expression.strip_prefix("integer.rotateright(") {
            (value, "int", "int", "rotate_right_i32")
        } else if let Some(value) = expression.strip_prefix("long.rotateleft(") {
            (value, "bigint", "bigint", "rotate_left_i64")
        } else if let Some(value) = expression.strip_prefix("long.rotateright(") {
            (value, "bigint", "bigint", "rotate_right_i64")
        } else {
            return None;
        };
    if !arg_types[0].eq_ignore_ascii_case(expected_arg_type)
        || !arg_types[1].eq_ignore_ascii_case("int")
        || !return_type.eq_ignore_ascii_case(expected_return_type)
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let (lhs, rhs) = params.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some(alias.to_string())
}

fn java_bit_rotate_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, kind, expected_return_type) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.rotateleft(")
    {
        (value, "i32_left", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.rotateright(") {
        (value, "i32_right", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.rotateleft(") {
        (value, "i64_left", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.rotateright(") {
        (value, "i64_right", "bigint")
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let distance = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()? as u32;
    let folded = match kind {
        "i32_left" => {
            let value = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32;
            (value.rotate_left(distance & 31) as i32).to_string()
        }
        "i32_right" => {
            let value = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32;
            (value.rotate_right(distance & 31) as i32).to_string()
        }
        "i64_left" => {
            let value = parse_java_integer_literal(params[0].trim())? as u64;
            (value.rotate_left(distance & 63) as i64).to_string()
        }
        "i64_right" => {
            let value = parse_java_integer_literal(params[0].trim())? as u64;
            (value.rotate_right(distance & 63) as i64).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_integer_static_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if let Some(alias) = java_integer_static_literal_alias(expression, return_type) {
        return Some(alias);
    }
    if let Some(alias) = java_integer_static_binary_alias(expression, return_type, arg_types) {
        return Some(alias);
    }
    java_integer_signum_alias(expression, return_type, arg_types)
}

fn java_integer_static_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if let Some(alias) = java_integer_static_binary_literal_alias(expression, return_type) {
        return Some(alias);
    }
    let (value, kind) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.signum(") {
            (value, "i32_signum")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.signum(") {
            (value, "i64_signum")
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    let folded = match kind {
        "i32_signum" => i32::try_from(parse_java_integer_literal(value)?)
            .ok()?
            .signum(),
        "i64_signum" => parse_java_integer_literal(value)?.signum() as i32,
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_integer_static_binary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let (value, kind, expected_return_type) = if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.sum(")
    {
        (value, "i32_sum", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.max(") {
        (value, "i32_max", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.min(") {
        (value, "i32_min", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.divideunsigned(")
    {
        (value, "u32_div", "int")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "integer.remainderunsigned(")
    {
        (value, "u32_rem", "int")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.sum(") {
        (value, "i64_sum", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.max(") {
        (value, "i64_max", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.min(") {
        (value, "i64_min", "bigint")
    } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.divideunsigned(") {
        (value, "u64_div", "bigint")
    } else if let Some(value) =
        strip_ascii_prefix_ignore_case(expression, "long.remainderunsigned(")
    {
        (value, "u64_rem", "bigint")
    } else {
        return None;
    };
    if !return_type.eq_ignore_ascii_case(expected_return_type) {
        return None;
    }
    let params = split_java_args(value.strip_suffix(')')?.trim())?;
    if params.len() != 2 {
        return None;
    }
    let folded = match kind {
        "i32_sum" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            lhs.wrapping_add(rhs).to_string()
        }
        "i32_max" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            lhs.max(rhs).to_string()
        }
        "i32_min" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()?;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()?;
            lhs.min(rhs).to_string()
        }
        "u32_div" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()? as u32;
            if rhs == 0 {
                return None;
            }
            ((lhs / rhs) as i32).to_string()
        }
        "u32_rem" => {
            let lhs = i32::try_from(parse_java_integer_literal(params[0].trim())?).ok()? as u32;
            let rhs = i32::try_from(parse_java_integer_literal(params[1].trim())?).ok()? as u32;
            if rhs == 0 {
                return None;
            }
            ((lhs % rhs) as i32).to_string()
        }
        "i64_sum" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            lhs.wrapping_add(rhs).to_string()
        }
        "i64_max" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            lhs.max(rhs).to_string()
        }
        "i64_min" => {
            let lhs = parse_java_integer_literal(params[0].trim())?;
            let rhs = parse_java_integer_literal(params[1].trim())?;
            lhs.min(rhs).to_string()
        }
        "u64_div" => {
            let lhs = parse_java_integer_literal(params[0].trim())? as u64;
            let rhs = parse_java_integer_literal(params[1].trim())? as u64;
            if rhs == 0 {
                return None;
            }
            ((lhs / rhs) as i64).to_string()
        }
        "u64_rem" => {
            let lhs = parse_java_integer_literal(params[0].trim())? as u64;
            let rhs = parse_java_integer_literal(params[1].trim())? as u64;
            if rhs == 0 {
                return None;
            }
            ((lhs % rhs) as i64).to_string()
        }
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn java_integer_static_binary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if arg_types.len() < 2 {
        return None;
    }
    let (value, expected_arg_type, expected_return_type, alias) =
        if let Some(value) = expression.strip_prefix("integer.sum(") {
            (value, "int", "int", "int_sum_args")
        } else if let Some(value) = expression.strip_prefix("integer.max(") {
            (value, "int", "int", "int_max_args")
        } else if let Some(value) = expression.strip_prefix("integer.min(") {
            (value, "int", "int", "int_min_args")
        } else if let Some(value) = expression.strip_prefix("integer.divideunsigned(") {
            (value, "int", "int", "int_divide_unsigned_args")
        } else if let Some(value) = expression.strip_prefix("integer.remainderunsigned(") {
            (value, "int", "int", "int_remainder_unsigned_args")
        } else if let Some(value) = expression.strip_prefix("long.sum(") {
            (value, "bigint", "bigint", "long_sum_args")
        } else if let Some(value) = expression.strip_prefix("long.max(") {
            (value, "bigint", "bigint", "long_max_args")
        } else if let Some(value) = expression.strip_prefix("long.min(") {
            (value, "bigint", "bigint", "long_min_args")
        } else if let Some(value) = expression.strip_prefix("long.divideunsigned(") {
            (value, "bigint", "bigint", "long_divide_unsigned_args")
        } else if let Some(value) = expression.strip_prefix("long.remainderunsigned(") {
            (value, "bigint", "bigint", "long_remainder_unsigned_args")
        } else {
            return None;
        };
    if !return_type.eq_ignore_ascii_case(expected_return_type)
        || !arg_types
            .iter()
            .take(2)
            .all(|arg_type| arg_type.eq_ignore_ascii_case(expected_arg_type))
    {
        return None;
    }
    let params = value.strip_suffix(')')?;
    let (lhs, rhs) = params.split_once(',')?;
    if !is_java_identifier(lhs.trim()) || !is_java_identifier(rhs.trim()) {
        return None;
    }
    Some(alias.to_string())
}

fn java_integer_signum_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("int") {
        return None;
    }
    let Some(arg_type) = arg_types.first().map(|value| value.to_ascii_lowercase()) else {
        return None;
    };
    let (value, expected_arg_type, alias) =
        if let Some(value) = expression.strip_prefix("integer.signum(") {
            (value, "int", "int_signum")
        } else if let Some(value) = expression.strip_prefix("long.signum(") {
            (value, "bigint", "long_signum")
        } else {
            return None;
        };
    if arg_type != expected_arg_type {
        return None;
    }
    let value = value.strip_suffix(')')?.trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some(alias.to_string())
}

fn java_boolean_parse_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    if !matches!(
        arg_types.first().map(|value| value.to_ascii_lowercase()),
        Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
    ) {
        return None;
    }
    let value = if let Some(value) = expression.strip_prefix("boolean.parseboolean(") {
        value
    } else if let Some(value) = expression.strip_prefix("boolean.valueof(") {
        value
    } else {
        return None;
    }
    .strip_suffix(')')?
    .trim();
    if !is_java_identifier(value) {
        return None;
    }
    Some("parse_bool_text".to_string())
}

fn java_boolean_parse_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if !return_type.eq_ignore_ascii_case("boolean") {
        return None;
    }
    let value = strip_ascii_prefix_ignore_case(expression, "boolean.parseboolean(")
        .or_else(|| strip_ascii_prefix_ignore_case(expression, "boolean.valueof("))?
        .strip_suffix(')')?
        .trim();
    let literal = parse_java_string_literal(value)?;
    Some(format!(
        "constant:{}",
        if literal.eq_ignore_ascii_case("true") {
            "true"
        } else {
            "false"
        }
    ))
}

fn java_boolean_binary_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let has_boolean_args = arg_types
        .iter()
        .take(2)
        .all(|arg_type| arg_type.eq_ignore_ascii_case("boolean"));
    if arg_types.len() >= 2 && has_boolean_args {
        let (value, alias) = if let Some(value) = expression.strip_prefix("boolean.logicaland(") {
            (value, "bool_and")
        } else if let Some(value) = expression.strip_prefix("boolean.logicalor(") {
            (value, "bool_or")
        } else if let Some(value) = expression.strip_prefix("boolean.logicalxor(") {
            (value, "bool_xor")
        } else {
            ("", "")
        };
        if !alias.is_empty() {
            let params = value.strip_suffix(')')?;
            let (lhs, rhs) = params.split_once(',')?;
            if is_java_identifier(lhs.trim()) && is_java_identifier(rhs.trim()) {
                return Some(alias.to_string());
            }
            return None;
        }
    }
    if let Some((lhs, rhs)) = expression.split_once("&&") {
        if is_java_identifier(lhs.trim()) && is_java_identifier(rhs.trim()) {
            return Some("bool_and".to_string());
        }
    }
    if let Some((lhs, rhs)) = expression.split_once("||") {
        if is_java_identifier(lhs.trim()) && is_java_identifier(rhs.trim()) {
            return Some("bool_or".to_string());
        }
    }
    None
}

fn java_boolean_binary_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.logicaland(") {
        let params = split_java_args(value.strip_suffix(')')?.trim())?;
        if params.len() != 2 {
            return None;
        }
        let lhs = parse_java_boolean_literal(params[0].trim())?;
        let rhs = parse_java_boolean_literal(params[1].trim())?;
        return Some(format!("constant:{}", lhs && rhs));
    }
    if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.logicalor(") {
        let params = split_java_args(value.strip_suffix(')')?.trim())?;
        if params.len() != 2 {
            return None;
        }
        let lhs = parse_java_boolean_literal(params[0].trim())?;
        let rhs = parse_java_boolean_literal(params[1].trim())?;
        return Some(format!("constant:{}", lhs || rhs));
    }
    if let Some(value) = strip_ascii_prefix_ignore_case(expression, "boolean.logicalxor(") {
        let params = split_java_args(value.strip_suffix(')')?.trim())?;
        if params.len() != 2 {
            return None;
        }
        let lhs = parse_java_boolean_literal(params[0].trim())?;
        let rhs = parse_java_boolean_literal(params[1].trim())?;
        return Some(format!("constant:{}", lhs ^ rhs));
    }
    if let Some((lhs, rhs)) = expression.split_once("&&") {
        let lhs = parse_java_boolean_literal(lhs.trim())?;
        let rhs = parse_java_boolean_literal(rhs.trim())?;
        return Some(format!("constant:{}", lhs && rhs));
    }
    if let Some((lhs, rhs)) = expression.split_once("||") {
        let lhs = parse_java_boolean_literal(lhs.trim())?;
        let rhs = parse_java_boolean_literal(rhs.trim())?;
        return Some(format!("constant:{}", lhs || rhs));
    }
    if let Some((lhs, rhs)) = expression.split_once('^') {
        let lhs = parse_java_boolean_literal(lhs.trim())?;
        let rhs = parse_java_boolean_literal(rhs.trim())?;
        return Some(format!("constant:{}", lhs ^ rhs));
    }
    None
}

fn java_boxed_primitive_to_string_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        return None;
    }
    let receiver = expression.strip_suffix(".tostring()")?.trim();
    if !is_java_identifier(receiver) {
        return None;
    }
    let alias = match arg_types.first().map(|value| value.to_ascii_lowercase()) {
        Some(arg_type) if arg_type == "tinyint" => "to_string_i8",
        Some(arg_type) if arg_type == "smallint" => "to_string_i16",
        Some(arg_type) if arg_type == "int" => "to_string_i32",
        Some(arg_type) if matches!(arg_type.as_str(), "bigint" | "counter") => "to_string_i64",
        Some(arg_type) if arg_type == "float" => "to_string_f32",
        Some(arg_type) if arg_type == "double" => "to_string_f64",
        Some(arg_type) if arg_type == "boolean" => "to_string_bool",
        Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii") => "identity",
        _ => return None,
    };
    Some(alias.to_string())
}

fn java_boxed_primitive_to_string_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    if !matches!(
        return_type.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "ascii"
    ) {
        return None;
    }
    let receiver = strip_ascii_suffix_ignore_case(expression, ".tostring()")?.trim();
    let literal = if let Some(value) = java_boxed_boolean_value_of_literal(receiver) {
        value.to_string()
    } else {
        match java_boxed_value_of_literal(receiver)? {
            JavaBoxedNumber::I64(value) => value.to_string(),
            JavaBoxedNumber::F32(value) => java_f32_to_string(value),
            JavaBoxedNumber::F64(value) => java_f64_to_string(value),
        }
    };
    Some(format!("constant:{literal}"))
}

fn java_boxed_boolean_value_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean"
        || !matches!(
            arg_types.first().map(|value| value.to_ascii_lowercase()),
            Some(arg_type) if arg_type == "boolean"
        )
    {
        return None;
    }
    let receiver = expression.strip_suffix(".booleanvalue()")?.trim();
    is_java_identifier(receiver).then(|| "identity".to_string())
}

fn java_boxed_boolean_value_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    if return_type.to_ascii_lowercase() != "boolean" {
        return None;
    }
    let receiver = strip_ascii_suffix_ignore_case(expression, ".booleanvalue()")?.trim();
    Some(format!(
        "constant:{}",
        java_boxed_boolean_value_of_literal(receiver)?
    ))
}

fn java_boxed_numeric_conversion_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    let target_type = if expression.ends_with(".bytevalue()") {
        "tinyint"
    } else if expression.ends_with(".shortvalue()") {
        "smallint"
    } else if expression.ends_with(".intvalue()") {
        "int"
    } else if expression.ends_with(".longvalue()") {
        "bigint"
    } else if expression.ends_with(".floatvalue()") {
        "float"
    } else if expression.ends_with(".doublevalue()") {
        "double"
    } else {
        return None;
    };
    if canonical_numeric_arg_type(return_type)? != target_type {
        return None;
    }
    let receiver = expression.split_once('.')?.0.trim();
    if !is_java_identifier(receiver) {
        return None;
    }
    let source_type = canonical_numeric_arg_type(arg_types.first()?)?;
    Some(format!(
        "boxed_numeric_conversion:{source_type}:{target_type}"
    ))
}

fn java_boxed_numeric_conversion_literal_alias(
    expression: &str,
    return_type: &str,
) -> Option<String> {
    let (receiver, target_type) = if let Some(receiver) =
        strip_ascii_suffix_ignore_case(expression, ".bytevalue()")
    {
        (receiver, "tinyint")
    } else if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".shortvalue()") {
        (receiver, "smallint")
    } else if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".intvalue()") {
        (receiver, "int")
    } else if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".longvalue()") {
        (receiver, "bigint")
    } else if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".floatvalue()") {
        (receiver, "float")
    } else if let Some(receiver) = strip_ascii_suffix_ignore_case(expression, ".doublevalue()") {
        (receiver, "double")
    } else {
        return None;
    };
    if canonical_numeric_arg_type(return_type)? != target_type {
        return None;
    }
    let value = java_boxed_value_of_literal(receiver.trim())?;
    let folded = match target_type {
        "tinyint" => java_boxed_to_i8(value).to_string(),
        "smallint" => java_boxed_to_i16(value).to_string(),
        "int" => java_boxed_to_i32(value).to_string(),
        "bigint" => java_boxed_to_i64(value).to_string(),
        "float" => java_boxed_to_f32(value).to_string(),
        "double" => java_boxed_to_f64(value).to_string(),
        _ => return None,
    };
    Some(format!("constant:{folded}"))
}

fn strip_ascii_suffix_ignore_case<'a>(value: &'a str, suffix: &str) -> Option<&'a str> {
    if value.len() < suffix.len() {
        return None;
    }
    let (prefix, actual_suffix) = value.split_at(value.len() - suffix.len());
    actual_suffix.eq_ignore_ascii_case(suffix).then_some(prefix)
}

fn java_boxed_value_of_literal(expression: &str) -> Option<JavaBoxedNumber> {
    let (value, source_type) =
        if let Some(value) = strip_ascii_prefix_ignore_case(expression, "byte.valueof(") {
            (value, "tinyint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "short.valueof(") {
            (value, "smallint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "integer.valueof(") {
            (value, "int")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "long.valueof(") {
            (value, "bigint")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "float.valueof(") {
            (value, "float")
        } else if let Some(value) = strip_ascii_prefix_ignore_case(expression, "double.valueof(") {
            (value, "double")
        } else {
            return None;
        };
    let value = value.strip_suffix(')')?.trim();
    match source_type {
        "tinyint" => Some(JavaBoxedNumber::I64(
            i8::try_from(parse_java_integer_literal(value)?).ok()? as i64,
        )),
        "smallint" => Some(JavaBoxedNumber::I64(
            i16::try_from(parse_java_integer_literal(value)?).ok()? as i64,
        )),
        "int" => Some(JavaBoxedNumber::I64(
            i32::try_from(parse_java_integer_literal(value)?).ok()? as i64,
        )),
        "bigint" => Some(JavaBoxedNumber::I64(parse_java_integer_literal(value)?)),
        "float" => Some(JavaBoxedNumber::F32(parse_java_float_literal(value)? as f32)),
        "double" => Some(JavaBoxedNumber::F64(parse_java_float_literal(value)?)),
        _ => None,
    }
}

fn java_boxed_boolean_value_of_literal(expression: &str) -> Option<bool> {
    let value = strip_ascii_prefix_ignore_case(expression, "boolean.valueof(")?;
    parse_java_boolean_literal(value.strip_suffix(')')?.trim())
}

fn canonical_numeric_arg_type(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "tinyint" => Some("tinyint"),
        "smallint" => Some("smallint"),
        "int" => Some("int"),
        "bigint" | "counter" => Some("bigint"),
        "float" => Some("float"),
        "double" => Some("double"),
        _ => None,
    }
}

fn java_no_arg_text_method_alias(
    expression: &str,
    return_type: &str,
    arg_types: &[String],
) -> Option<String> {
    if !matches!(
        arg_types.first().map(|value| value.to_ascii_lowercase()),
        Some(arg_type) if matches!(arg_type.as_str(), "text" | "varchar" | "ascii")
    ) {
        return None;
    }
    let return_type = return_type.to_ascii_lowercase();
    for (suffix, alias, expected_return_type) in [
        (".length()", "text_length", "int"),
        (".tostring()", "identity", "text"),
        (".intern()", "identity", "text"),
        (".trim()", "java_trim", "text"),
        (".strip()", "trim", "text"),
        (".stripleading()", "trim_start", "text"),
        (".striptrailing()", "trim_end", "text"),
        (".isempty()", "text_is_empty", "boolean"),
        (".isblank()", "text_is_blank", "boolean"),
    ] {
        let Some(receiver) = expression.strip_suffix(suffix) else {
            continue;
        };
        if is_java_identifier(receiver.trim()) && return_type == expected_return_type {
            return Some(alias.to_string());
        }
    }
    None
}

fn java_no_arg_text_method_literal_alias(expression: &str, return_type: &str) -> Option<String> {
    let return_type = return_type.to_ascii_lowercase();
    for (suffix, expected_return_type) in [
        (".length()", "int"),
        (".tostring()", "text"),
        (".intern()", "text"),
        (".tolowercase()", "text"),
        (".touppercase()", "text"),
        (".trim()", "text"),
        (".strip()", "text"),
        (".stripleading()", "text"),
        (".striptrailing()", "text"),
        (".isempty()", "boolean"),
        (".isblank()", "boolean"),
    ] {
        let Some(receiver) = strip_ascii_suffix_ignore_case(expression, suffix) else {
            continue;
        };
        if return_type != expected_return_type
            && !(expected_return_type == "text"
                && matches!(return_type.as_str(), "varchar" | "ascii"))
        {
            return None;
        }
        let literal = parse_java_string_literal(receiver.trim())?;
        let folded = match suffix {
            ".length()" => literal.encode_utf16().count().to_string(),
            ".tostring()" | ".intern()" => literal,
            ".tolowercase()" => literal.to_lowercase(),
            ".touppercase()" => literal.to_uppercase(),
            ".trim()" => java_text_trim(&literal),
            ".strip()" => literal.trim().to_string(),
            ".stripleading()" => literal.trim_start().to_string(),
            ".striptrailing()" => literal.trim_end().to_string(),
            ".isempty()" => literal.is_empty().to_string(),
            ".isblank()" => literal.chars().all(char::is_whitespace).to_string(),
            _ => return None,
        };
        return Some(format!("constant:{folded}"));
    }
    None
}

fn is_java_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn map_utf8(
    value: Option<Vec<u8>>,
    f: impl FnOnce(String) -> String,
) -> Result<Option<Vec<u8>>, String> {
    match value {
        Some(bytes) => {
            let text = String::from_utf8(bytes).map_err(|e| e.to_string())?;
            Ok(Some(f(text).into_bytes()))
        }
        None => Ok(None),
    }
}

fn java_text_trim(value: &str) -> String {
    let Some(start) = value
        .char_indices()
        .find(|(_, ch)| (*ch as u32) > 0x20)
        .map(|(idx, _)| idx)
    else {
        return String::new();
    };
    let end = value
        .char_indices()
        .rev()
        .find(|(_, ch)| (*ch as u32) > 0x20)
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(value.len());
    value[start..end].to_string()
}

fn sum_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let mut sum = 0i32;
    for arg in args.iter().flatten() {
        let bytes: [u8; 4] = arg
            .as_slice()
            .try_into()
            .map_err(|_| "Expected 4-byte i32 argument".to_string())?;
        sum = sum.saturating_add(i32::from_be_bytes(bytes));
    }
    Ok(sum)
}

fn sum_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let mut sum = 0i64;
    for arg in args.iter().flatten() {
        let bytes: [u8; 8] = arg
            .as_slice()
            .try_into()
            .map_err(|_| "Expected 8-byte i64 argument".to_string())?;
        sum = sum.saturating_add(i64::from_be_bytes(bytes));
    }
    Ok(sum)
}

fn sum_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let mut sum = 0f32;
    for arg in args.iter().flatten() {
        sum += decode_f32(arg)?;
    }
    Ok(sum)
}

fn sum_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let mut sum = 0f64;
    for arg in args.iter().flatten() {
        sum += decode_f64(arg)?;
    }
    Ok(sum)
}

fn sub_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let bytes: [u8; 4] = first
        .as_slice()
        .try_into()
        .map_err(|_| "Expected 4-byte i32 argument".to_string())?;
    let mut result = i32::from_be_bytes(bytes);
    for arg in values {
        let bytes: [u8; 4] = arg
            .as_slice()
            .try_into()
            .map_err(|_| "Expected 4-byte i32 argument".to_string())?;
        result = result.saturating_sub(i32::from_be_bytes(bytes));
    }
    Ok(result)
}

fn sub_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let bytes: [u8; 8] = first
        .as_slice()
        .try_into()
        .map_err(|_| "Expected 8-byte i64 argument".to_string())?;
    let mut result = i64::from_be_bytes(bytes);
    for arg in values {
        let bytes: [u8; 8] = arg
            .as_slice()
            .try_into()
            .map_err(|_| "Expected 8-byte i64 argument".to_string())?;
        result = result.saturating_sub(i64::from_be_bytes(bytes));
    }
    Ok(result)
}

fn sub_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f32(first)?;
    for arg in values {
        result -= decode_f32(arg)?;
    }
    Ok(result)
}

fn sub_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f64(first)?;
    for arg in values {
        result -= decode_f64(arg)?;
    }
    Ok(result)
}

fn mul_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i32(first)?;
    for arg in values {
        result = result.saturating_mul(decode_i32(arg)?);
    }
    Ok(result)
}

fn mul_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i64(first)?;
    for arg in values {
        result = result.saturating_mul(decode_i64(arg)?);
    }
    Ok(result)
}

fn mul_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f32(first)?;
    for arg in values {
        result *= decode_f32(arg)?;
    }
    Ok(result)
}

fn mul_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f64(first)?;
    for arg in values {
        result *= decode_f64(arg)?;
    }
    Ok(result)
}

fn div_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i32(first)?;
    for arg in values {
        let divisor = decode_i32(arg)?;
        if divisor == 0 {
            return Err("Division by zero".to_string());
        }
        result = result.wrapping_div(divisor);
    }
    Ok(result)
}

fn div_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i64(first)?;
    for arg in values {
        let divisor = decode_i64(arg)?;
        if divisor == 0 {
            return Err("Division by zero".to_string());
        }
        result = result.wrapping_div(divisor);
    }
    Ok(result)
}

fn div_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f32(first)?;
    for arg in values {
        result /= decode_f32(arg)?;
    }
    Ok(result)
}

fn div_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f64(first)?;
    for arg in values {
        result /= decode_f64(arg)?;
    }
    Ok(result)
}

fn rem_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i32(first)?;
    for arg in values {
        let divisor = decode_i32(arg)?;
        if divisor == 0 {
            return Err("Division by zero".to_string());
        }
        result = result.wrapping_rem(divisor);
    }
    Ok(result)
}

fn rem_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0);
    };
    let mut result = decode_i64(first)?;
    for arg in values {
        let divisor = decode_i64(arg)?;
        if divisor == 0 {
            return Err("Division by zero".to_string());
        }
        result = result.wrapping_rem(divisor);
    }
    Ok(result)
}

fn rem_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f32(first)?;
    for arg in values {
        result %= decode_f32(arg)?;
    }
    Ok(result)
}

fn rem_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let mut values = args.iter().flatten();
    let Some(first) = values.next() else {
        return Ok(0.0);
    };
    let mut result = decode_f64(first)?;
    for arg in values {
        result %= decode_f64(arg)?;
    }
    Ok(result)
}

fn neg_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(first_i32_arg(args)?.unwrap_or(0).saturating_neg())
}

fn neg_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(first_i64_arg(args)?.unwrap_or(0).saturating_neg())
}

fn neg_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(-first_f32_arg(args)?.unwrap_or(0.0))
}

fn neg_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(-first_f64_arg(args)?.unwrap_or(0.0))
}

fn inc_exact_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    first_i32_arg(args)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "Integer overflow in Math.incrementExact".to_string())
}

fn inc_exact_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    first_i64_arg(args)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "Integer overflow in Math.incrementExact".to_string())
}

fn dec_exact_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    first_i32_arg(args)?
        .unwrap_or(0)
        .checked_sub(1)
        .ok_or_else(|| "Integer overflow in Math.decrementExact".to_string())
}

fn dec_exact_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    first_i64_arg(args)?
        .unwrap_or(0)
        .checked_sub(1)
        .ok_or_else(|| "Integer overflow in Math.decrementExact".to_string())
}

fn neg_exact_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    first_i32_arg(args)?
        .unwrap_or(0)
        .checked_neg()
        .ok_or_else(|| "Integer overflow in Math.negateExact".to_string())
}

fn neg_exact_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    first_i64_arg(args)?
        .unwrap_or(0)
        .checked_neg()
        .ok_or_else(|| "Integer overflow in Math.negateExact".to_string())
}

fn add_exact_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    lhs.checked_add(rhs)
        .ok_or_else(|| "Integer overflow in Math.addExact".to_string())
}

fn add_exact_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    lhs.checked_add(rhs)
        .ok_or_else(|| "Integer overflow in Math.addExact".to_string())
}

fn sub_exact_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    lhs.checked_sub(rhs)
        .ok_or_else(|| "Integer overflow in Math.subtractExact".to_string())
}

fn sub_exact_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    lhs.checked_sub(rhs)
        .ok_or_else(|| "Integer overflow in Math.subtractExact".to_string())
}

fn mul_exact_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    lhs.checked_mul(rhs)
        .ok_or_else(|| "Integer overflow in Math.multiplyExact".to_string())
}

fn mul_exact_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    lhs.checked_mul(rhs)
        .ok_or_else(|| "Integer overflow in Math.multiplyExact".to_string())
}

fn mul_full_i32_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0) as i64;
    let rhs = i32_arg(args, 1)?.unwrap_or(0) as i64;
    Ok(lhs * rhs)
}

fn mul_high_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0) as i128;
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0) as i128;
    Ok(((lhs * rhs) >> 64) as i64)
}

fn floor_div_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    java_floor_div_i32(lhs, rhs)
}

fn floor_div_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    java_floor_div_i64(lhs, rhs)
}

fn floor_mod_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    java_floor_mod_i32(lhs, rhs)
}

fn floor_mod_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    java_floor_mod_i64(lhs, rhs)
}

fn java_floor_div_i32(lhs: i32, rhs: i32) -> Result<i32, String> {
    if rhs == 0 {
        return Err("Division by zero".to_string());
    }
    let quotient = lhs.wrapping_div(rhs);
    let remainder = lhs.wrapping_rem(rhs);
    if remainder != 0 && ((lhs ^ rhs) < 0) {
        Ok(quotient.wrapping_sub(1))
    } else {
        Ok(quotient)
    }
}

fn java_floor_div_i64(lhs: i64, rhs: i64) -> Result<i64, String> {
    if rhs == 0 {
        return Err("Division by zero".to_string());
    }
    let quotient = lhs.wrapping_div(rhs);
    let remainder = lhs.wrapping_rem(rhs);
    if remainder != 0 && ((lhs ^ rhs) < 0) {
        Ok(quotient.wrapping_sub(1))
    } else {
        Ok(quotient)
    }
}

fn java_floor_mod_i32(lhs: i32, rhs: i32) -> Result<i32, String> {
    if rhs == 0 {
        return Err("Division by zero".to_string());
    }
    let remainder = lhs.wrapping_rem(rhs);
    if remainder != 0 && ((lhs ^ rhs) < 0) {
        Ok(remainder.wrapping_add(rhs))
    } else {
        Ok(remainder)
    }
}

fn java_floor_mod_i64(lhs: i64, rhs: i64) -> Result<i64, String> {
    if rhs == 0 {
        return Err("Division by zero".to_string());
    }
    let remainder = lhs.wrapping_rem(rhs);
    if remainder != 0 && ((lhs ^ rhs) < 0) {
        Ok(remainder.wrapping_add(rhs))
    } else {
        Ok(remainder)
    }
}

fn to_i32_exact(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = first_i64_arg(args)?.unwrap_or(0);
    i32::try_from(value).map_err(|_| "Integer overflow in Math.toIntExact".to_string())
}

fn abs_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(first_i32_arg(args)?.unwrap_or(0).saturating_abs())
}

fn abs_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(first_i64_arg(args)?.unwrap_or(0).saturating_abs())
}

fn abs_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).abs())
}

fn abs_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).abs())
}

fn max_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.max(rhs))
}

fn max_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.max(rhs))
}

fn max_f32_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_max_f32(lhs, rhs))
}

fn max_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_max_f64(lhs, rhs))
}

fn min_i32_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.min(rhs))
}

fn min_i64_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.min(rhs))
}

fn min_f32_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_min_f32(lhs, rhs))
}

fn min_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_min_f64(lhs, rhs))
}

fn java_max_f32(lhs: f32, rhs: f32) -> f32 {
    if lhs.is_nan() || rhs.is_nan() {
        return f32::NAN;
    }
    if lhs == 0.0 && rhs == 0.0 {
        return if lhs.is_sign_positive() || rhs.is_sign_positive() {
            0.0
        } else {
            -0.0
        };
    }
    if lhs >= rhs { lhs } else { rhs }
}

fn java_max_f64(lhs: f64, rhs: f64) -> f64 {
    if lhs.is_nan() || rhs.is_nan() {
        return f64::NAN;
    }
    if lhs == 0.0 && rhs == 0.0 {
        return if lhs.is_sign_positive() || rhs.is_sign_positive() {
            0.0
        } else {
            -0.0
        };
    }
    if lhs >= rhs { lhs } else { rhs }
}

fn java_min_f32(lhs: f32, rhs: f32) -> f32 {
    if lhs.is_nan() || rhs.is_nan() {
        return f32::NAN;
    }
    if lhs == 0.0 && rhs == 0.0 {
        return if lhs.is_sign_negative() || rhs.is_sign_negative() {
            -0.0
        } else {
            0.0
        };
    }
    if lhs <= rhs { lhs } else { rhs }
}

fn java_min_f64(lhs: f64, rhs: f64) -> f64 {
    if lhs.is_nan() || rhs.is_nan() {
        return f64::NAN;
    }
    if lhs == 0.0 && rhs == 0.0 {
        return if lhs.is_sign_negative() || rhs.is_sign_negative() {
            -0.0
        } else {
            0.0
        };
    }
    if lhs <= rhs { lhs } else { rhs }
}

fn sqrt_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).sqrt())
}

fn floor_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).floor())
}

fn ceil_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).ceil())
}

fn next_up_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).next_up())
}

fn next_down_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).next_down())
}

fn ulp_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(java_ulp_f32(first_f32_arg(args)?.unwrap_or(0.0)))
}

fn java_ulp_f32(value: f32) -> f32 {
    if value.is_nan() {
        return f32::NAN;
    }
    if value.is_infinite() {
        return f32::INFINITY;
    }

    let value = value.abs();
    if value == 0.0 {
        return f32::from_bits(1);
    }

    let exponent = ((value.to_bits() >> 23) & 0xff) as i32;
    if exponent == 0 {
        return f32::from_bits(1);
    }

    let ulp_exponent = exponent - 127 - 23;
    if ulp_exponent >= -126 {
        f32::from_bits(((ulp_exponent + 127) as u32) << 23)
    } else {
        f32::from_bits(1_u32 << ((ulp_exponent + 149) as u32))
    }
}

fn rint_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).round_ties_even())
}

fn next_up_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).next_up())
}

fn next_down_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).next_down())
}

fn ulp_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(java_ulp_f64(first_f64_arg(args)?.unwrap_or(0.0)))
}

fn java_ulp_f64(value: f64) -> f64 {
    if value.is_nan() {
        return f64::NAN;
    }
    if value.is_infinite() {
        return f64::INFINITY;
    }

    let value = value.abs();
    if value == 0.0 {
        return f64::from_bits(1);
    }

    let exponent = ((value.to_bits() >> 52) & 0x7ff) as i32;
    if exponent == 0 {
        return f64::from_bits(1);
    }

    let ulp_exponent = exponent - 1023 - 52;
    if ulp_exponent >= -1022 {
        f64::from_bits(((ulp_exponent + 1023) as u64) << 52)
    } else {
        f64::from_bits(1_u64 << ((ulp_exponent + 1074) as u32))
    }
}

fn log_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).ln())
}

fn log10_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).log10())
}

fn exp_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).exp())
}

fn expm1_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).exp_m1())
}

fn log1p_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).ln_1p())
}

fn sin_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).sin())
}

fn cos_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).cos())
}

fn tan_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).tan())
}

fn sinh_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).sinh())
}

fn cosh_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).cosh())
}

fn tanh_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).tanh())
}

fn asin_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).asin())
}

fn acos_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).acos())
}

fn atan_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).atan())
}

fn cbrt_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).cbrt())
}

fn to_radians_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).to_radians())
}

fn to_degrees_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).to_degrees())
}

fn signum_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).signum())
}

fn signum_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).signum())
}

fn round_f32_to_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(java_round_f32(first_f32_arg(args)?.unwrap_or(0.0)))
}

fn round_f64_to_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(java_round_f64(first_f64_arg(args)?.unwrap_or(0.0)))
}

fn get_exponent_f32_to_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(java_get_exponent_f32(first_f32_arg(args)?.unwrap_or(0.0)))
}

fn get_exponent_f64_to_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(java_get_exponent_f64(first_f64_arg(args)?.unwrap_or(0.0)))
}

fn java_get_exponent_f32(value: f32) -> i32 {
    let exponent = ((value.to_bits() >> 23) & 0xff) as i32;
    match exponent {
        0 => -127,
        0xff => 128,
        value => value - 127,
    }
}

fn java_get_exponent_f64(value: f64) -> i32 {
    let exponent = ((value.to_bits() >> 52) & 0x7ff) as i32;
    match exponent {
        0 => -1023,
        0x7ff => 1024,
        value => value - 1023,
    }
}

fn java_round_f32(value: f32) -> i32 {
    (value + 0.5).floor() as i32
}

fn java_round_f64(value: f64) -> i64 {
    (value + 0.5).floor() as i64
}

fn fma_f32_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let multiplicand = f32_arg(args, 0)?.unwrap_or(0.0);
    let multiplier = f32_arg(args, 1)?.unwrap_or(0.0);
    let addend = f32_arg(args, 2)?.unwrap_or(0.0);
    Ok(multiplicand.mul_add(multiplier, addend))
}

fn fma_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let multiplicand = f64_arg(args, 0)?.unwrap_or(0.0);
    let multiplier = f64_arg(args, 1)?.unwrap_or(0.0);
    let addend = f64_arg(args, 2)?.unwrap_or(0.0);
    Ok(multiplicand.mul_add(multiplier, addend))
}

fn pow_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs.powf(rhs))
}

fn hypot_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs.hypot(rhs))
}

fn atan2_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs.atan2(rhs))
}

fn copysign_f32_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs.copysign(rhs))
}

fn copysign_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs.copysign(rhs))
}

fn next_after_f32_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let start = f32_arg(args, 0)?.unwrap_or(0.0);
    let direction = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_next_after_f32(start, direction))
}

fn next_after_f64_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let start = f64_arg(args, 0)?.unwrap_or(0.0);
    let direction = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_next_after_f64(start, direction))
}

fn java_next_after_f32(start: f32, direction: f64) -> f32 {
    if start.is_nan() || direction.is_nan() {
        return f32::NAN;
    }
    if (start as f64) == direction {
        return direction as f32;
    }
    if start == 0.0 {
        return if direction > 0.0 {
            f32::from_bits(1)
        } else {
            -f32::from_bits(1)
        };
    }

    let bits = start.to_bits();
    if (direction > start as f64) == (start > 0.0) {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    }
}

fn java_next_after_f64(start: f64, direction: f64) -> f64 {
    if start.is_nan() || direction.is_nan() {
        return f64::NAN;
    }
    if start == direction {
        return direction;
    }
    if start == 0.0 {
        return if direction > 0.0 {
            f64::from_bits(1)
        } else {
            -f64::from_bits(1)
        };
    }

    let bits = start.to_bits();
    if (direction > start) == (start > 0.0) {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}

fn is_nan_f32(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).is_nan())
}

fn is_nan_f64(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).is_nan())
}

fn is_infinite_f32(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).is_infinite())
}

fn is_infinite_f64(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).is_infinite())
}

fn is_finite_f32(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0).is_finite())
}

fn is_finite_f64(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0).is_finite())
}

fn decode_i32(value: &[u8]) -> Result<i32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| "Expected 4-byte i32 argument".to_string())?;
    Ok(i32::from_be_bytes(bytes))
}

fn decode_i16(value: &[u8]) -> Result<i16, String> {
    let bytes: [u8; 2] = value
        .try_into()
        .map_err(|_| "Expected 2-byte i16 argument".to_string())?;
    Ok(i16::from_be_bytes(bytes))
}

fn decode_i8(value: &[u8]) -> Result<i8, String> {
    let bytes: [u8; 1] = value
        .try_into()
        .map_err(|_| "Expected 1-byte i8 argument".to_string())?;
    Ok(i8::from_be_bytes(bytes))
}

fn decode_i64(value: &[u8]) -> Result<i64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| "Expected 8-byte i64 argument".to_string())?;
    Ok(i64::from_be_bytes(bytes))
}

fn decode_f32(value: &[u8]) -> Result<f32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| "Expected 4-byte f32 argument".to_string())?;
    Ok(f32::from_be_bytes(bytes))
}

fn decode_f64(value: &[u8]) -> Result<f64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| "Expected 8-byte f64 argument".to_string())?;
    Ok(f64::from_be_bytes(bytes))
}

fn first_i32_arg(args: &[Option<Vec<u8>>]) -> Result<Option<i32>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    decode_i32(first).map(Some)
}

fn first_i16_arg(args: &[Option<Vec<u8>>]) -> Result<Option<i16>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    decode_i16(first).map(Some)
}

fn first_i8_arg(args: &[Option<Vec<u8>>]) -> Result<Option<i8>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    decode_i8(first).map(Some)
}

fn first_i64_arg(args: &[Option<Vec<u8>>]) -> Result<Option<i64>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    let bytes: [u8; 8] = first
        .as_slice()
        .try_into()
        .map_err(|_| "Expected 8-byte i64 argument".to_string())?;
    Ok(Some(i64::from_be_bytes(bytes)))
}

fn first_f32_arg(args: &[Option<Vec<u8>>]) -> Result<Option<f32>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    decode_f32(first).map(Some)
}

fn first_f64_arg(args: &[Option<Vec<u8>>]) -> Result<Option<f64>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    decode_f64(first).map(Some)
}

fn add_i32_literal(args: &[Option<Vec<u8>>], literal: i32) -> Result<i32, String> {
    Ok(first_i32_arg(args)?.unwrap_or(0).saturating_add(literal))
}

fn add_i64_literal(args: &[Option<Vec<u8>>], literal: i64) -> Result<i64, String> {
    Ok(first_i64_arg(args)?.unwrap_or(0).saturating_add(literal))
}

fn add_f32_literal(args: &[Option<Vec<u8>>], literal: f32) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0) + literal)
}

fn add_f64_literal(args: &[Option<Vec<u8>>], literal: f64) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0) + literal)
}

fn sub_i32_literal(args: &[Option<Vec<u8>>], literal: i32) -> Result<i32, String> {
    Ok(first_i32_arg(args)?.unwrap_or(0).saturating_sub(literal))
}

fn sub_i64_literal(args: &[Option<Vec<u8>>], literal: i64) -> Result<i64, String> {
    Ok(first_i64_arg(args)?.unwrap_or(0).saturating_sub(literal))
}

fn sub_f32_literal(args: &[Option<Vec<u8>>], literal: f32) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0) - literal)
}

fn sub_f64_literal(args: &[Option<Vec<u8>>], literal: f64) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0) - literal)
}

fn mul_i32_literal(args: &[Option<Vec<u8>>], literal: i32) -> Result<i32, String> {
    Ok(first_i32_arg(args)?.unwrap_or(0).saturating_mul(literal))
}

fn mul_i64_literal(args: &[Option<Vec<u8>>], literal: i64) -> Result<i64, String> {
    Ok(first_i64_arg(args)?.unwrap_or(0).saturating_mul(literal))
}

fn mul_f32_literal(args: &[Option<Vec<u8>>], literal: f32) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0) * literal)
}

fn mul_f64_literal(args: &[Option<Vec<u8>>], literal: f64) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0) * literal)
}

fn div_i32_literal(args: &[Option<Vec<u8>>], literal: i32) -> Result<i32, String> {
    if literal == 0 {
        return Err("Division by zero".to_string());
    }
    Ok(first_i32_arg(args)?.unwrap_or(0).wrapping_div(literal))
}

fn div_i64_literal(args: &[Option<Vec<u8>>], literal: i64) -> Result<i64, String> {
    if literal == 0 {
        return Err("Division by zero".to_string());
    }
    Ok(first_i64_arg(args)?.unwrap_or(0).wrapping_div(literal))
}

fn div_f32_literal(args: &[Option<Vec<u8>>], literal: f32) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0) / literal)
}

fn div_f64_literal(args: &[Option<Vec<u8>>], literal: f64) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0) / literal)
}

fn rem_i32_literal(args: &[Option<Vec<u8>>], literal: i32) -> Result<i32, String> {
    if literal == 0 {
        return Err("Division by zero".to_string());
    }
    Ok(first_i32_arg(args)?.unwrap_or(0).wrapping_rem(literal))
}

fn rem_i64_literal(args: &[Option<Vec<u8>>], literal: i64) -> Result<i64, String> {
    if literal == 0 {
        return Err("Division by zero".to_string());
    }
    Ok(first_i64_arg(args)?.unwrap_or(0).wrapping_rem(literal))
}

fn rem_f32_literal(args: &[Option<Vec<u8>>], literal: f32) -> Result<f32, String> {
    Ok(first_f32_arg(args)?.unwrap_or(0.0) % literal)
}

fn rem_f64_literal(args: &[Option<Vec<u8>>], literal: f64) -> Result<f64, String> {
    Ok(first_f64_arg(args)?.unwrap_or(0.0) % literal)
}

fn concat_literal(
    value: Option<Vec<u8>>,
    literal: &str,
    prefix: bool,
) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let text = String::from_utf8(value).map_err(|e| e.to_string())?;
    let result = if prefix {
        format!("{literal}{text}")
    } else {
        format!("{text}{literal}")
    };
    Ok(Some(result.into_bytes()))
}

fn parse_comparison_alias(value: &str) -> Result<(&str, &str), String> {
    value
        .split_once(':')
        .ok_or_else(|| format!("Invalid comparison alias: {value}"))
}

fn boolean_bytes(value: bool) -> Vec<u8> {
    vec![u8::from(value)]
}

fn compare_i32_literal(args: &[Option<Vec<u8>>], op: &str, literal: i32) -> Result<bool, String> {
    let value = first_i32_arg(args)?.unwrap_or(0);
    Ok(compare_i64_values(value as i64, op, literal as i64))
}

fn compare_i64_literal(args: &[Option<Vec<u8>>], op: &str, literal: i64) -> Result<bool, String> {
    let value = first_numeric_i64_arg(args)?.unwrap_or(0);
    Ok(compare_i64_values(value, op, literal))
}

fn compare_f32_literal(args: &[Option<Vec<u8>>], op: &str, literal: f32) -> Result<bool, String> {
    let value = first_f32_arg(args)?.unwrap_or(0.0);
    Ok(compare_f64_values(value as f64, op, literal as f64))
}

fn compare_f64_literal(args: &[Option<Vec<u8>>], op: &str, literal: f64) -> Result<bool, String> {
    let value = first_f64_arg(args)?.unwrap_or(0.0);
    Ok(compare_f64_values(value, op, literal))
}

fn compare_i32_args(args: &[Option<Vec<u8>>], op: &str) -> Result<bool, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(compare_i64_values(lhs as i64, op, rhs as i64))
}

fn compare_i64_args(args: &[Option<Vec<u8>>], op: &str) -> Result<bool, String> {
    let lhs = numeric_i64_arg(args, 0)?.unwrap_or(0);
    let rhs = numeric_i64_arg(args, 1)?.unwrap_or(0);
    Ok(compare_i64_values(lhs, op, rhs))
}

fn compare_f32_args(args: &[Option<Vec<u8>>], op: &str) -> Result<bool, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(compare_f64_values(lhs as f64, op, rhs as f64))
}

fn compare_f64_args(args: &[Option<Vec<u8>>], op: &str) -> Result<bool, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(compare_f64_values(lhs, op, rhs))
}

fn compare_bool_args(args: &[Option<Vec<u8>>], op: &str) -> Result<bool, String> {
    let lhs = first_bool_arg(args, 0)?.unwrap_or(false);
    let rhs = first_bool_arg(args, 1)?.unwrap_or(false);
    Ok(match op {
        "eq" => lhs == rhs,
        "ne" => lhs != rhs,
        _ => false,
    })
}

fn java_compare_i8(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i8_arg(args, 0)?.unwrap_or(0);
    let rhs = i8_arg(args, 1)?.unwrap_or(0);
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_i16(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i16_arg(args, 0)?.unwrap_or(0);
    let rhs = i16_arg(args, 1)?.unwrap_or(0);
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0);
    let rhs = i64_arg(args, 1)?.unwrap_or(0);
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_unsigned_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0) as u32;
    let rhs = i32_arg(args, 1)?.unwrap_or(0) as u32;
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_unsigned_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0) as u64;
    let rhs = i64_arg(args, 1)?.unwrap_or(0) as u64;
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn boxed_equals_f32_args(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_f32_to_i32_bits(lhs) == java_f32_to_i32_bits(rhs))
}

fn boxed_equals_f64_args(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_f64_to_i64_bits(lhs) == java_f64_to_i64_bits(rhs))
}

fn java_compare_bool(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = first_bool_arg(args, 0)?.unwrap_or(false);
    let rhs = first_bool_arg(args, 1)?.unwrap_or(false);
    Ok(ordering_to_i32(lhs.cmp(&rhs)))
}

fn java_compare_f32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_compare_f32_values(lhs, rhs))
}

fn java_compare_f32_values(lhs: f32, rhs: f32) -> i32 {
    if lhs < rhs {
        return -1;
    }
    if lhs > rhs {
        return 1;
    }
    ordering_to_i32(java_f32_to_i32_bits(lhs).cmp(&java_f32_to_i32_bits(rhs)))
}

fn java_compare_f64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_compare_f64_values(lhs, rhs))
}

fn java_compare_f64_values(lhs: f64, rhs: f64) -> i32 {
    if lhs < rhs {
        return -1;
    }
    if lhs > rhs {
        return 1;
    }
    ordering_to_i32(java_f64_to_i64_bits(lhs).cmp(&java_f64_to_i64_bits(rhs)))
}

fn java_f32_to_i32_bits(value: f32) -> i32 {
    if value.is_nan() {
        0x7fc0_0000u32 as i32
    } else {
        value.to_bits() as i32
    }
}

fn java_f64_to_i64_bits(value: f64) -> i64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000u64 as i64
    } else {
        value.to_bits() as i64
    }
}

fn f32_to_i32_bits(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(java_f32_to_i32_bits(f32_arg(args, 0)?.unwrap_or(0.0)))
}

fn f32_to_raw_i32_bits(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(f32_arg(args, 0)?.unwrap_or(0.0).to_bits() as i32)
}

fn i32_bits_to_f32(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    Ok(f32::from_bits(i32_arg(args, 0)?.unwrap_or(0) as u32))
}

fn f64_to_i64_bits(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(java_f64_to_i64_bits(f64_arg(args, 0)?.unwrap_or(0.0)))
}

fn f64_to_raw_i64_bits(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(f64_arg(args, 0)?.unwrap_or(0.0).to_bits() as i64)
}

fn i64_bits_to_f64(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    Ok(f64::from_bits(i64_arg(args, 0)?.unwrap_or(0) as u64))
}

fn ordering_to_i32(ordering: std::cmp::Ordering) -> i32 {
    match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn first_numeric_i64_arg(args: &[Option<Vec<u8>>]) -> Result<Option<i64>, String> {
    numeric_i64_arg(args, 0)
}

fn i8_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<i8>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_i8(value).map(Some)
}

fn i16_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<i16>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_i16(value).map(Some)
}

fn i32_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<i32>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_i32(value).map(Some)
}

fn i64_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<i64>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_i64(value).map(Some)
}

fn numeric_i64_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<i64>, String> {
    let Some(Some(first)) = args.get(index) else {
        return Ok(None);
    };
    match first.len() {
        4 => decode_i32(first).map(|value| Some(value as i64)),
        8 => decode_i64(first).map(Some),
        len => Err(format!(
            "Expected 4-byte or 8-byte numeric argument, got {len} bytes"
        )),
    }
}

fn f32_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<f32>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_f32(value).map(Some)
}

fn f64_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<f64>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    decode_f64(value).map(Some)
}

#[derive(Debug, Clone, Copy)]
enum JavaBoxedNumber {
    I64(i64),
    F32(f32),
    F64(f64),
}

fn boxed_numeric_conversion(
    args: &[Option<Vec<u8>>],
    alias: &str,
) -> Result<Option<Vec<u8>>, String> {
    let (source_type, target_type) = alias
        .split_once(':')
        .ok_or_else(|| format!("Invalid boxed numeric conversion alias: {alias}"))?;
    let value = match source_type {
        "tinyint" => JavaBoxedNumber::I64(i8_arg(args, 0)?.unwrap_or(0) as i64),
        "smallint" => JavaBoxedNumber::I64(i16_arg(args, 0)?.unwrap_or(0) as i64),
        "int" => JavaBoxedNumber::I64(i32_arg(args, 0)?.unwrap_or(0) as i64),
        "bigint" => JavaBoxedNumber::I64(i64_arg(args, 0)?.unwrap_or(0)),
        "float" => JavaBoxedNumber::F32(f32_arg(args, 0)?.unwrap_or(0.0)),
        "double" => JavaBoxedNumber::F64(f64_arg(args, 0)?.unwrap_or(0.0)),
        other => return Err(format!("Unsupported boxed numeric source type: {other}")),
    };
    let bytes = match target_type {
        "tinyint" => vec![java_boxed_to_i8(value) as u8],
        "smallint" => java_boxed_to_i16(value).to_be_bytes().to_vec(),
        "int" => java_boxed_to_i32(value).to_be_bytes().to_vec(),
        "bigint" => java_boxed_to_i64(value).to_be_bytes().to_vec(),
        "float" => java_boxed_to_f32(value).to_be_bytes().to_vec(),
        "double" => java_boxed_to_f64(value).to_be_bytes().to_vec(),
        other => return Err(format!("Unsupported boxed numeric target type: {other}")),
    };
    Ok(Some(bytes))
}

fn java_boxed_to_i8(value: JavaBoxedNumber) -> i8 {
    java_boxed_to_i32(value) as i8
}

fn java_boxed_to_i16(value: JavaBoxedNumber) -> i16 {
    java_boxed_to_i32(value) as i16
}

fn java_boxed_to_i32(value: JavaBoxedNumber) -> i32 {
    match value {
        JavaBoxedNumber::I64(value) => value as i32,
        JavaBoxedNumber::F32(value) => value as i32,
        JavaBoxedNumber::F64(value) => value as i32,
    }
}

fn java_boxed_to_i64(value: JavaBoxedNumber) -> i64 {
    match value {
        JavaBoxedNumber::I64(value) => value,
        JavaBoxedNumber::F32(value) => value as i64,
        JavaBoxedNumber::F64(value) => value as i64,
    }
}

fn java_boxed_to_f32(value: JavaBoxedNumber) -> f32 {
    match value {
        JavaBoxedNumber::I64(value) => value as f32,
        JavaBoxedNumber::F32(value) => value,
        JavaBoxedNumber::F64(value) => value as f32,
    }
}

fn java_boxed_to_f64(value: JavaBoxedNumber) -> f64 {
    match value {
        JavaBoxedNumber::I64(value) => value as f64,
        JavaBoxedNumber::F32(value) => value as f64,
        JavaBoxedNumber::F64(value) => value,
    }
}

fn compare_i64_values(lhs: i64, op: &str, rhs: i64) -> bool {
    match op {
        "eq" => lhs == rhs,
        "ne" => lhs != rhs,
        "gt" => lhs > rhs,
        "gte" => lhs >= rhs,
        "lt" => lhs < rhs,
        "lte" => lhs <= rhs,
        _ => false,
    }
}

fn compare_f64_values(lhs: f64, op: &str, rhs: f64) -> bool {
    match op {
        "eq" => lhs == rhs,
        "ne" => lhs != rhs,
        "gt" => lhs > rhs,
        "gte" => lhs >= rhs,
        "lt" => lhs < rhs,
        "lte" => lhs <= rhs,
        _ => false,
    }
}

fn text_equals_literal(args: &[Option<Vec<u8>>], literal: &str) -> Result<bool, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(false);
    };
    let value = String::from_utf8(first.clone()).map_err(|e| e.to_string())?;
    Ok(value == literal)
}

fn text_equals_args(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let Some(lhs) = text_arg(args, 0)? else {
        return Ok(false);
    };
    let Some(rhs) = text_arg(args, 1)? else {
        return Ok(false);
    };
    Ok(lhs == rhs)
}

fn text_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<String>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    String::from_utf8(value.clone())
        .map(Some)
        .map_err(|e| e.to_string())
}

fn bool_equals_literal(args: &[Option<Vec<u8>>], literal: bool) -> Result<bool, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(false);
    };
    let value = match first.as_slice() {
        [0] => false,
        [1] => true,
        other => return Err(format!("Expected one-byte boolean argument, got {other:?}")),
    };
    Ok(value == literal)
}

fn not_bool(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    Ok(!first_bool_arg(args, 0)?.unwrap_or(false))
}

fn boolean_binary(
    args: &[Option<Vec<u8>>],
    op: impl FnOnce(bool, bool) -> bool,
) -> Result<bool, String> {
    let lhs = first_bool_arg(args, 0)?.unwrap_or(false);
    let rhs = first_bool_arg(args, 1)?.unwrap_or(false);
    Ok(op(lhs, rhs))
}

fn to_string_i8(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        first_i8_arg(args)?.unwrap_or(0).to_string().into_bytes(),
    ))
}

fn to_string_i16(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        first_i16_arg(args)?.unwrap_or(0).to_string().into_bytes(),
    ))
}

fn to_string_i32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        first_i32_arg(args)?.unwrap_or(0).to_string().into_bytes(),
    ))
}

fn to_string_i64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        first_i64_arg(args)?.unwrap_or(0).to_string().into_bytes(),
    ))
}

fn to_string_i32_radix_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let radix = i32_arg(args, 1)?.unwrap_or(10);
    to_string_i32_radix(args, radix)
}

fn to_string_i64_radix_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let radix = i32_arg(args, 1)?.unwrap_or(10);
    to_string_i64_radix(args, radix)
}

fn to_string_i32_radix(args: &[Option<Vec<u8>>], radix: i32) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format_signed_radix(
            first_i32_arg(args)?.unwrap_or(0) as i128,
            java_string_radix(radix),
        )
        .into_bytes(),
    ))
}

fn to_string_i64_radix(args: &[Option<Vec<u8>>], radix: i32) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format_signed_radix(
            first_i64_arg(args)?.unwrap_or(0) as i128,
            java_string_radix(radix),
        )
        .into_bytes(),
    ))
}

fn to_string_f32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        java_f32_to_string(first_f32_arg(args)?.unwrap_or(0.0)).into_bytes(),
    ))
}

fn to_string_f64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        java_f64_to_string(first_f64_arg(args)?.unwrap_or(0.0)).into_bytes(),
    ))
}

fn java_f32_to_string(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f32::INFINITY {
        return "Infinity".to_string();
    }
    if value == f32::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    normalize_java_float_string(value.to_string())
}

fn java_f64_to_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    normalize_java_float_string(value.to_string())
}

fn normalize_java_float_string(value: String) -> String {
    if value.contains(['.', 'e', 'E']) {
        value
    } else {
        format!("{value}.0")
    }
}

fn to_string_bool(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        first_bool_arg(args, 0)?
            .unwrap_or(false)
            .to_string()
            .into_bytes(),
    ))
}

fn to_unsigned_int_i8(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i8_arg(args, 0)?.unwrap_or(0) as u8 as i32)
}

fn to_unsigned_long_i8(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(i8_arg(args, 0)?.unwrap_or(0) as u8 as i64)
}

fn to_unsigned_int_i16(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i16_arg(args, 0)?.unwrap_or(0) as u16 as i32)
}

fn to_unsigned_long_i16(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(i16_arg(args, 0)?.unwrap_or(0) as u16 as i64)
}

fn to_unsigned_long_i32(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(i32_arg(args, 0)?.unwrap_or(0) as u32 as i64)
}

fn to_unsigned_string_i32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        (i32_arg(args, 0)?.unwrap_or(0) as u32)
            .to_string()
            .into_bytes(),
    ))
}

fn to_unsigned_string_i64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        (i64_arg(args, 0)?.unwrap_or(0) as u64)
            .to_string()
            .into_bytes(),
    ))
}

fn to_unsigned_string_i32_radix_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let radix = i32_arg(args, 1)?.unwrap_or(10);
    to_unsigned_string_i32_radix(args, radix)
}

fn to_unsigned_string_i64_radix_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let radix = i32_arg(args, 1)?.unwrap_or(10);
    to_unsigned_string_i64_radix(args, radix)
}

fn to_unsigned_string_i32_radix(
    args: &[Option<Vec<u8>>],
    radix: i32,
) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format_unsigned_radix(
            i32_arg(args, 0)?.unwrap_or(0) as u32 as u64,
            java_string_radix(radix),
        )
        .into_bytes(),
    ))
}

fn to_unsigned_string_i64_radix(
    args: &[Option<Vec<u8>>],
    radix: i32,
) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format_unsigned_radix(
            i64_arg(args, 0)?.unwrap_or(0) as u64,
            java_string_radix(radix),
        )
        .into_bytes(),
    ))
}

fn to_binary_string_i32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:b}", i32_arg(args, 0)?.unwrap_or(0) as u32).into_bytes(),
    ))
}

fn to_binary_string_i64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:b}", i64_arg(args, 0)?.unwrap_or(0) as u64).into_bytes(),
    ))
}

fn to_octal_string_i32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:o}", i32_arg(args, 0)?.unwrap_or(0) as u32).into_bytes(),
    ))
}

fn to_octal_string_i64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:o}", i64_arg(args, 0)?.unwrap_or(0) as u64).into_bytes(),
    ))
}

fn to_hex_string_i32(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:x}", i32_arg(args, 0)?.unwrap_or(0) as u32).into_bytes(),
    ))
}

fn to_hex_string_i64(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(
        format!("{:x}", i64_arg(args, 0)?.unwrap_or(0) as u64).into_bytes(),
    ))
}

fn parse_string_radix_alias(body: &str, prefix: &str) -> Result<i32, String> {
    body.trim_start_matches(prefix)
        .parse::<i32>()
        .map_err(|e| format!("Invalid Java string radix alias: {e}"))
}

fn java_string_radix(radix: i32) -> u32 {
    if (2..=36).contains(&radix) {
        radix as u32
    } else {
        10
    }
}

fn format_signed_radix(value: i128, radix: u32) -> String {
    if radix == 10 {
        return value.to_string();
    }
    if value < 0 {
        format!("-{}", format_unsigned_radix((-value) as u64, radix))
    } else {
        format_unsigned_radix(value as u64, radix)
    }
}

fn format_unsigned_radix(mut value: u64, radix: u32) -> String {
    debug_assert!((2..=36).contains(&radix));
    if value == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::new();
    while value > 0 {
        let digit = (value % radix as u64) as u8;
        digits.push(match digit {
            0..=9 => b'0' + digit,
            _ => b'a' + (digit - 10),
        });
        value /= radix as u64;
    }
    digits.reverse();
    String::from_utf8(digits).expect("radix digits are ascii")
}

fn bit_count_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i32_arg(args, 0)?.unwrap_or(0) as u32).count_ones() as i32)
}

fn bit_count_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i64_arg(args, 0)?.unwrap_or(0) as u64).count_ones() as i32)
}

fn leading_zeros_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i32_arg(args, 0)?.unwrap_or(0) as u32).leading_zeros() as i32)
}

fn leading_zeros_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i64_arg(args, 0)?.unwrap_or(0) as u64).leading_zeros() as i32)
}

fn trailing_zeros_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i32_arg(args, 0)?.unwrap_or(0) as u32).trailing_zeros() as i32)
}

fn trailing_zeros_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i64_arg(args, 0)?.unwrap_or(0) as u64).trailing_zeros() as i32)
}

fn highest_one_bit_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = i32_arg(args, 0)?.unwrap_or(0) as u32;
    Ok(if value == 0 {
        0
    } else {
        (1u32 << (31 - value.leading_zeros())) as i32
    })
}

fn highest_one_bit_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let value = i64_arg(args, 0)?.unwrap_or(0) as u64;
    Ok(if value == 0 {
        0
    } else {
        (1u64 << (63 - value.leading_zeros())) as i64
    })
}

fn lowest_one_bit_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = i32_arg(args, 0)?.unwrap_or(0) as u32;
    Ok((value & value.wrapping_neg()) as i32)
}

fn lowest_one_bit_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let value = i64_arg(args, 0)?.unwrap_or(0) as u64;
    Ok((value & value.wrapping_neg()) as i64)
}

fn reverse_bits_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok((i32_arg(args, 0)?.unwrap_or(0) as u32).reverse_bits() as i32)
}

fn reverse_bits_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok((i64_arg(args, 0)?.unwrap_or(0) as u64).reverse_bits() as i64)
}

fn reverse_bytes_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i32_arg(args, 0)?.unwrap_or(0).swap_bytes())
}

fn reverse_bytes_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    Ok(i64_arg(args, 0)?.unwrap_or(0).swap_bytes())
}

fn rotate_left_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = i32_arg(args, 0)?.unwrap_or(0) as u32;
    let distance = i32_arg(args, 1)?.unwrap_or(0) as u32;
    Ok(value.rotate_left(distance & 31) as i32)
}

fn rotate_left_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let value = i64_arg(args, 0)?.unwrap_or(0) as u64;
    let distance = i32_arg(args, 1)?.unwrap_or(0) as u32;
    Ok(value.rotate_left(distance & 63) as i64)
}

fn rotate_right_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = i32_arg(args, 0)?.unwrap_or(0) as u32;
    let distance = i32_arg(args, 1)?.unwrap_or(0) as u32;
    Ok(value.rotate_right(distance & 31) as i32)
}

fn rotate_right_i64(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let value = i64_arg(args, 0)?.unwrap_or(0) as u64;
    let distance = i32_arg(args, 1)?.unwrap_or(0) as u32;
    Ok(value.rotate_right(distance & 63) as i64)
}

fn int_sum_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.wrapping_add(rhs))
}

fn int_max_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.max(rhs))
}

fn int_min_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0);
    let rhs = i32_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.min(rhs))
}

fn int_divide_unsigned_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0) as u32;
    let rhs = i32_arg(args, 1)?.unwrap_or(0) as u32;
    if rhs == 0 {
        return Err("Integer.divideUnsigned division by zero".to_string());
    }
    Ok((lhs / rhs) as i32)
}

fn int_remainder_unsigned_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let lhs = i32_arg(args, 0)?.unwrap_or(0) as u32;
    let rhs = i32_arg(args, 1)?.unwrap_or(0) as u32;
    if rhs == 0 {
        return Err("Integer.remainderUnsigned division by zero".to_string());
    }
    Ok((lhs % rhs) as i32)
}

fn int_signum(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i32_arg(args, 0)?.unwrap_or(0).signum())
}

fn long_sum_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0);
    let rhs = i64_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.wrapping_add(rhs))
}

fn long_max_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0);
    let rhs = i64_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.max(rhs))
}

fn long_min_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0);
    let rhs = i64_arg(args, 1)?.unwrap_or(0);
    Ok(lhs.min(rhs))
}

fn long_divide_unsigned_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0) as u64;
    let rhs = i64_arg(args, 1)?.unwrap_or(0) as u64;
    if rhs == 0 {
        return Err("Long.divideUnsigned division by zero".to_string());
    }
    Ok((lhs / rhs) as i64)
}

fn long_remainder_unsigned_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let lhs = i64_arg(args, 0)?.unwrap_or(0) as u64;
    let rhs = i64_arg(args, 1)?.unwrap_or(0) as u64;
    if rhs == 0 {
        return Err("Long.remainderUnsigned division by zero".to_string());
    }
    Ok((lhs % rhs) as i64)
}

fn long_signum(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i64_arg(args, 0)?.unwrap_or(0).signum() as i32)
}

fn java_hash_i8(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i8_arg(args, 0)?.unwrap_or(0) as i32)
}

fn java_hash_i16(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i16_arg(args, 0)?.unwrap_or(0) as i32)
}

fn java_hash_i32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(i32_arg(args, 0)?.unwrap_or(0))
}

fn java_hash_i64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = i64_arg(args, 0)?.unwrap_or(0) as u64;
    Ok((value ^ (value >> 32)) as i32)
}

fn java_hash_bool(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(if first_bool_arg(args, 0)?.unwrap_or(false) {
        1231
    } else {
        1237
    })
}

fn java_hash_f32(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    Ok(java_f32_to_i32_bits(f32_arg(args, 0)?.unwrap_or(0.0)))
}

fn java_hash_f64(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let value = java_f64_to_i64_bits(f64_arg(args, 0)?.unwrap_or(0.0)) as u64;
    Ok((value ^ (value >> 32)) as i32)
}

fn java_hash_text(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(0);
    };
    let mut hash = 0i32;
    for unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(unit as i32);
    }
    Ok(hash)
}

fn object_is_null(args: &[Option<Vec<u8>>]) -> bool {
    matches!(args.first(), None | Some(None))
}

fn objects_require_non_null(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    match args.first() {
        Some(Some(value)) => Ok(Some(value.clone())),
        _ => Err("Objects.requireNonNull argument is null".to_string()),
    }
}

fn objects_require_non_null_else_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    if let Some(Some(value)) = args.first() {
        return Ok(Some(value.clone()));
    }
    match args.get(1) {
        Some(Some(value)) => Ok(Some(value.clone())),
        _ => Err("Objects.requireNonNullElse default value is null".to_string()),
    }
}

fn objects_require_non_null_else_text_literal(
    args: &[Option<Vec<u8>>],
    literal: &str,
) -> Result<Option<Vec<u8>>, String> {
    if let Some(Some(value)) = args.first() {
        String::from_utf8(value.clone()).map_err(|e| e.to_string())?;
        return Ok(Some(value.clone()));
    }
    Ok(Some(literal.as_bytes().to_vec()))
}

fn objects_to_string_default_text_args(
    args: &[Option<Vec<u8>>],
) -> Result<Option<Vec<u8>>, String> {
    if let Some(Some(value)) = args.first() {
        String::from_utf8(value.clone()).map_err(|e| e.to_string())?;
        return Ok(Some(value.clone()));
    }
    match args.get(1) {
        Some(Some(value)) => {
            String::from_utf8(value.clone()).map_err(|e| e.to_string())?;
            Ok(Some(value.clone()))
        }
        _ => Ok(None),
    }
}

fn objects_to_string_default_text_literal(
    args: &[Option<Vec<u8>>],
    literal: &str,
) -> Result<Option<Vec<u8>>, String> {
    if let Some(Some(value)) = args.first() {
        String::from_utf8(value.clone()).map_err(|e| e.to_string())?;
        return Ok(Some(value.clone()));
    }
    Ok(Some(literal.as_bytes().to_vec()))
}

fn float_sum_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs + rhs)
}

fn float_max_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_max_f32(lhs, rhs))
}

fn float_min_args(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let lhs = f32_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f32_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_min_f32(lhs, rhs))
}

fn double_sum_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(lhs + rhs)
}

fn double_max_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_max_f64(lhs, rhs))
}

fn double_min_args(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let lhs = f64_arg(args, 0)?.unwrap_or(0.0);
    let rhs = f64_arg(args, 1)?.unwrap_or(0.0);
    Ok(java_min_f64(lhs, rhs))
}

fn first_bool_arg(args: &[Option<Vec<u8>>], index: usize) -> Result<Option<bool>, String> {
    let Some(Some(value)) = args.get(index) else {
        return Ok(None);
    };
    match value.as_slice() {
        [0] => Ok(Some(false)),
        [1] => Ok(Some(true)),
        other => Err(format!("Expected one-byte boolean argument, got {other:?}")),
    }
}

fn first_text_arg(args: &[Option<Vec<u8>>]) -> Result<Option<String>, String> {
    let Some(Some(first)) = args.first() else {
        return Ok(None);
    };
    String::from_utf8(first.clone())
        .map(Some)
        .map_err(|e| e.to_string())
}

fn text_predicate_literal(
    args: &[Option<Vec<u8>>],
    literal: &str,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<bool, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(false);
    };
    Ok(predicate(&value, literal))
}

fn text_predicate_args(
    args: &[Option<Vec<u8>>],
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<bool, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(false);
    };
    let Some(other) = text_arg(args, 1)? else {
        return Ok(false);
    };
    Ok(predicate(&value, &other))
}

fn text_equals_ignore_case(lhs: &str, rhs: &str) -> bool {
    java_text_compare_to_ignore_case(lhs, rhs) == 0
}

fn text_compare_to_literal(args: &[Option<Vec<u8>>], literal: &str) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(java_text_compare_to("", literal));
    };
    Ok(java_text_compare_to(&value, literal))
}

fn text_compare_to_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(java_text_compare_to("", ""));
    };
    let Some(other) = text_arg(args, 1)? else {
        return Ok(java_text_compare_to(&value, ""));
    };
    Ok(java_text_compare_to(&value, &other))
}

fn text_compare_to_ignore_case_literal(
    args: &[Option<Vec<u8>>],
    literal: &str,
) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(java_text_compare_to_ignore_case("", literal));
    };
    Ok(java_text_compare_to_ignore_case(&value, literal))
}

fn text_compare_to_ignore_case_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(java_text_compare_to_ignore_case("", ""));
    };
    let Some(other) = text_arg(args, 1)? else {
        return Ok(java_text_compare_to_ignore_case(&value, ""));
    };
    Ok(java_text_compare_to_ignore_case(&value, &other))
}

fn java_text_compare_to(lhs: &str, rhs: &str) -> i32 {
    let mut lhs_units = lhs.encode_utf16();
    let mut rhs_units = rhs.encode_utf16();

    loop {
        match (lhs_units.next(), rhs_units.next()) {
            (Some(lhs_unit), Some(rhs_unit)) if lhs_unit != rhs_unit => {
                return lhs_unit as i32 - rhs_unit as i32;
            }
            (Some(_), Some(_)) => {}
            (Some(_), None) => return lhs_units.count() as i32 + 1,
            (None, Some(_)) => return -(rhs_units.count() as i32 + 1),
            (None, None) => return 0,
        }
    }
}

fn java_text_compare_to_ignore_case(lhs: &str, rhs: &str) -> i32 {
    let lhs: String = lhs.chars().map(java_ignore_case_char).collect();
    let rhs: String = rhs.chars().map(java_ignore_case_char).collect();
    java_text_compare_to(&lhs, &rhs)
}

fn java_ignore_case_char(ch: char) -> char {
    let upper = single_case_mapping(ch.to_uppercase(), ch);
    single_case_mapping(upper.to_lowercase(), upper)
}

fn single_case_mapping(mapped: impl Iterator<Item = char>, fallback: char) -> char {
    let mut mapped = mapped;
    let Some(first) = mapped.next() else {
        return fallback;
    };
    if mapped.next().is_some() {
        fallback
    } else {
        first
    }
}

fn parse_bool_text(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(false);
    };
    Ok(value.eq_ignore_ascii_case("true"))
}

fn parse_i8_text(args: &[Option<Vec<u8>>]) -> Result<i8, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Byte.parseByte".to_string());
    };
    value
        .parse::<i8>()
        .map_err(|e| format!("Byte.parseByte failed: {e}"))
}

fn parse_i16_text(args: &[Option<Vec<u8>>]) -> Result<i16, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Short.parseShort".to_string());
    };
    value
        .parse::<i16>()
        .map_err(|e| format!("Short.parseShort failed: {e}"))
}

fn parse_i32_text(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Integer.parseInt".to_string());
    };
    value
        .parse::<i32>()
        .map_err(|e| format!("Integer.parseInt failed: {e}"))
}

fn parse_i64_text(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Long.parseLong".to_string());
    };
    value
        .parse::<i64>()
        .map_err(|e| format!("Long.parseLong failed: {e}"))
}

fn parse_radix_alias(body: &str, prefix: &str) -> Result<u32, String> {
    let radix = body
        .trim_start_matches(prefix)
        .parse::<u32>()
        .map_err(|e| format!("Invalid Java parse radix alias: {e}"))?;
    if (2..=36).contains(&radix) {
        Ok(radix)
    } else {
        Err(format!("Java parse radix out of range: {radix}"))
    }
}

fn parse_i8_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i8, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Byte.parseByte".to_string());
    };
    i8::from_str_radix(&value, radix).map_err(|e| format!("Byte.parseByte failed: {e}"))
}

fn parse_i16_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i16, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Short.parseShort".to_string());
    };
    i16::from_str_radix(&value, radix).map_err(|e| format!("Short.parseShort failed: {e}"))
}

fn parse_i32_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Integer.parseInt".to_string());
    };
    i32::from_str_radix(&value, radix).map_err(|e| format!("Integer.parseInt failed: {e}"))
}

fn parse_i64_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i64, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Long.parseLong".to_string());
    };
    i64::from_str_radix(&value, radix).map_err(|e| format!("Long.parseLong failed: {e}"))
}

fn parse_unsigned_i32_text(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    parse_unsigned_i32_text_radix(args, 10)
}

fn parse_unsigned_i64_text(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    parse_unsigned_i64_text_radix(args, 10)
}

fn parse_unsigned_i32_text_radix_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    parse_unsigned_i32_text_radix(args, java_parse_radix_arg(args)?)
}

fn parse_unsigned_i64_text_radix_args(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    parse_unsigned_i64_text_radix(args, java_parse_radix_arg(args)?)
}

fn parse_unsigned_i32_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Integer.parseUnsignedInt".to_string());
    };
    parse_unsigned_digits(&value, radix, "Integer.parseUnsignedInt").and_then(|value| {
        u32::try_from(value)
            .map(|value| value as i32)
            .map_err(|e| format!("Integer.parseUnsignedInt failed: {e}"))
    })
}

fn parse_unsigned_i64_text_radix(args: &[Option<Vec<u8>>], radix: u32) -> Result<i64, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Long.parseUnsignedLong".to_string());
    };
    parse_unsigned_digits(&value, radix, "Long.parseUnsignedLong").map(|value| value as i64)
}

fn java_parse_radix_arg(args: &[Option<Vec<u8>>]) -> Result<u32, String> {
    let radix = i32_arg(args, 1)?.unwrap_or(10);
    if (2..=36).contains(&radix) {
        Ok(radix as u32)
    } else {
        Err(format!("Java parse radix out of range: {radix}"))
    }
}

fn parse_unsigned_digits(value: &str, radix: u32, label: &str) -> Result<u64, String> {
    if !(2..=36).contains(&radix) {
        return Err(format!("{label} radix out of range: {radix}"));
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() || value.starts_with('-') {
        return Err(format!("{label} failed: invalid unsigned integer"));
    }
    u64::from_str_radix(value, radix).map_err(|e| format!("{label} failed: {e}"))
}

fn decode_i8_text(args: &[Option<Vec<u8>>]) -> Result<i8, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot decode null with Byte.decode".to_string());
    };
    let decoded = decode_java_integer_literal(&value, "Byte.decode")?;
    i8::try_from(decoded).map_err(|e| format!("Byte.decode failed: {e}"))
}

fn decode_i16_text(args: &[Option<Vec<u8>>]) -> Result<i16, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot decode null with Short.decode".to_string());
    };
    let decoded = decode_java_integer_literal(&value, "Short.decode")?;
    i16::try_from(decoded).map_err(|e| format!("Short.decode failed: {e}"))
}

fn decode_i32_text(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot decode null with Integer.decode".to_string());
    };
    let decoded = decode_java_integer_literal(&value, "Integer.decode")?;
    i32::try_from(decoded).map_err(|e| format!("Integer.decode failed: {e}"))
}

fn decode_i64_text(args: &[Option<Vec<u8>>]) -> Result<i64, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot decode null with Long.decode".to_string());
    };
    let decoded = decode_java_integer_literal(&value, "Long.decode")?;
    i64::try_from(decoded).map_err(|e| format!("Long.decode failed: {e}"))
}

fn decode_java_integer_literal(value: &str, method: &str) -> Result<i128, String> {
    if value.is_empty() {
        return Err(format!("{method} failed: empty input"));
    }
    let (negative, digits) = if let Some(digits) = value.strip_prefix('-') {
        (true, digits)
    } else if let Some(digits) = value.strip_prefix('+') {
        (false, digits)
    } else {
        (false, value)
    };
    let (radix, digits) = if let Some(digits) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        (16, digits)
    } else if let Some(digits) = digits.strip_prefix('#') {
        (16, digits)
    } else if digits.len() > 1 {
        if let Some(digits) = digits.strip_prefix('0') {
            (8, digits)
        } else {
            (10, digits)
        }
    } else {
        (10, digits)
    };
    if digits.is_empty() {
        return Err(format!("{method} failed: missing digits"));
    }
    let magnitude =
        i128::from_str_radix(digits, radix).map_err(|e| format!("{method} failed: {e}"))?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn parse_f32_text(args: &[Option<Vec<u8>>]) -> Result<f32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Float.parseFloat".to_string());
    };
    value
        .parse::<f32>()
        .map_err(|e| format!("Float.parseFloat failed: {e}"))
}

fn parse_f64_text(args: &[Option<Vec<u8>>]) -> Result<f64, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot parse null with Double.parseDouble".to_string());
    };
    value
        .parse::<f64>()
        .map_err(|e| format!("Double.parseDouble failed: {e}"))
}

fn text_length(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(0);
    };
    Ok(value.encode_utf16().count() as i32)
}

fn text_is_empty(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(false);
    };
    Ok(value.is_empty())
}

fn text_is_blank(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(false);
    };
    Ok(value.chars().all(char::is_whitespace))
}

fn text_substring_from(args: &[Option<Vec<u8>>], begin: i32) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(None);
    };
    java_substring_chars(
        &value,
        begin as isize,
        value.encode_utf16().count() as isize,
    )
    .map(|value| Some(value.into_bytes()))
}

fn text_substring_range(
    args: &[Option<Vec<u8>>],
    begin: i32,
    end: i32,
) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(None);
    };
    java_substring_chars(&value, begin as isize, end as isize).map(|value| Some(value.into_bytes()))
}

fn text_substring_from_arg(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(None);
    };
    let begin = i32_arg(args, 1)?.unwrap_or(0);
    java_substring_chars(
        &value,
        begin as isize,
        value.encode_utf16().count() as isize,
    )
    .map(|value| Some(value.into_bytes()))
}

fn text_substring_range_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(None);
    };
    let begin = i32_arg(args, 1)?.unwrap_or(0);
    let end = i32_arg(args, 2)?.unwrap_or(value.encode_utf16().count() as i32);
    java_substring_chars(&value, begin as isize, end as isize).map(|value| Some(value.into_bytes()))
}

fn text_char_at_literal(args: &[Option<Vec<u8>>], index: i32) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot call String.charAt on null".to_string());
    };
    java_text_char_at(&value, index)
}

fn text_char_at_arg(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Err("Cannot call String.charAt on null".to_string());
    };
    let index = i32_arg(args, 1)?.unwrap_or(0);
    java_text_char_at(&value, index)
}

fn java_text_char_at(value: &str, index: i32) -> Result<i32, String> {
    if index < 0 {
        return Err(format!("String.charAt index out of range: {index}"));
    }
    value
        .encode_utf16()
        .nth(index as usize)
        .map(|unit| unit as i32)
        .ok_or_else(|| format!("String.charAt index out of range: {index}"))
}

fn text_code_point_at_literal(args: &[Option<Vec<u8>>], index: i32) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot call String.codePointAt on null".to_string());
    };
    java_text_code_point_at(&value, index)
}

fn text_code_point_at_arg(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Err("Cannot call String.codePointAt on null".to_string());
    };
    let index = i32_arg(args, 1)?.unwrap_or(0);
    java_text_code_point_at(&value, index)
}

fn java_text_code_point_at(value: &str, index: i32) -> Result<i32, String> {
    if index < 0 {
        return Err(format!("String.codePointAt index out of range: {index}"));
    }
    let units: Vec<u16> = value.encode_utf16().collect();
    let Some(unit) = units.get(index as usize).copied() else {
        return Err(format!("String.codePointAt index out of range: {index}"));
    };
    if (0xD800..=0xDBFF).contains(&unit) {
        if let Some(next) = units.get(index as usize + 1).copied() {
            if (0xDC00..=0xDFFF).contains(&next) {
                return Ok(0x10000 + (((unit as i32 - 0xD800) << 10) | (next as i32 - 0xDC00)));
            }
        }
    }
    Ok(unit as i32)
}

fn text_code_point_before_literal(args: &[Option<Vec<u8>>], index: i32) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot call String.codePointBefore on null".to_string());
    };
    java_text_code_point_before(&value, index)
}

fn text_code_point_before_arg(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Err("Cannot call String.codePointBefore on null".to_string());
    };
    let index = i32_arg(args, 1)?.unwrap_or(0);
    java_text_code_point_before(&value, index)
}

fn java_text_code_point_before(value: &str, index: i32) -> Result<i32, String> {
    if index <= 0 {
        return Err(format!(
            "String.codePointBefore index out of range: {index}"
        ));
    }
    let units: Vec<u16> = value.encode_utf16().collect();
    if index as usize > units.len() {
        return Err(format!(
            "String.codePointBefore index out of range: {index}"
        ));
    }
    let unit_index = index as usize - 1;
    let unit = units[unit_index];
    if (0xDC00..=0xDFFF).contains(&unit) && unit_index > 0 {
        let prev = units[unit_index - 1];
        if (0xD800..=0xDBFF).contains(&prev) {
            return Ok(0x10000 + (((prev as i32 - 0xD800) << 10) | (unit as i32 - 0xDC00)));
        }
    }
    Ok(unit as i32)
}

fn text_code_point_count_literal(
    args: &[Option<Vec<u8>>],
    begin: i32,
    end: i32,
) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot call String.codePointCount on null".to_string());
    };
    java_text_code_point_count(&value, begin, end)
}

fn text_code_point_count_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Err("Cannot call String.codePointCount on null".to_string());
    };
    let begin = i32_arg(args, 1)?.unwrap_or(0);
    let end = i32_arg(args, 2)?.unwrap_or_else(|| value.encode_utf16().count() as i32);
    java_text_code_point_count(&value, begin, end)
}

fn java_text_code_point_count(value: &str, begin: i32, end: i32) -> Result<i32, String> {
    let units: Vec<u16> = value.encode_utf16().collect();
    if begin < 0 || end < begin || end as usize > units.len() {
        return Err(format!(
            "String.codePointCount index out of range: {begin}..{end}"
        ));
    }
    let mut count = 0;
    let mut index = begin as usize;
    let end = end as usize;
    while index < end {
        count += 1;
        if (0xD800..=0xDBFF).contains(&units[index])
            && index + 1 < end
            && (0xDC00..=0xDFFF).contains(&units[index + 1])
        {
            index += 2;
        } else {
            index += 1;
        }
    }
    Ok(count)
}

fn text_offset_by_code_points_literal(
    args: &[Option<Vec<u8>>],
    index: i32,
    offset: i32,
) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Err("Cannot call String.offsetByCodePoints on null".to_string());
    };
    java_text_offset_by_code_points(&value, index, offset)
}

fn text_offset_by_code_points_args(args: &[Option<Vec<u8>>]) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Err("Cannot call String.offsetByCodePoints on null".to_string());
    };
    let index = i32_arg(args, 1)?.unwrap_or(0);
    let offset = i32_arg(args, 2)?.unwrap_or(0);
    java_text_offset_by_code_points(&value, index, offset)
}

fn java_text_offset_by_code_points(value: &str, index: i32, offset: i32) -> Result<i32, String> {
    let units: Vec<u16> = value.encode_utf16().collect();
    if index < 0 || index as usize > units.len() {
        return Err(format!(
            "String.offsetByCodePoints index out of range: {index}"
        ));
    }
    let mut unit_index = index as usize;
    if offset >= 0 {
        for _ in 0..offset {
            if unit_index >= units.len() {
                return Err(format!(
                    "String.offsetByCodePoints offset out of range: {offset}"
                ));
            }
            if (0xD800..=0xDBFF).contains(&units[unit_index])
                && unit_index + 1 < units.len()
                && (0xDC00..=0xDFFF).contains(&units[unit_index + 1])
            {
                unit_index += 2;
            } else {
                unit_index += 1;
            }
        }
    } else {
        for _ in 0..-(offset as i64) {
            if unit_index == 0 {
                return Err(format!(
                    "String.offsetByCodePoints offset out of range: {offset}"
                ));
            }
            unit_index -= 1;
            if (0xDC00..=0xDFFF).contains(&units[unit_index]) && unit_index > 0 {
                let prev = units[unit_index - 1];
                if (0xD800..=0xDBFF).contains(&prev) {
                    unit_index -= 1;
                }
            }
        }
    }
    Ok(unit_index as i32)
}

fn text_repeat_arg(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(None);
    };
    let count = i32_arg(args, 1)?.unwrap_or(0);
    if count < 0 {
        return Err("Negative repeat count".to_string());
    }
    Ok(Some(value.repeat(count as usize).into_bytes()))
}

fn text_starts_with_literal_from(
    args: &[Option<Vec<u8>>],
    prefix: &str,
    from_index: i32,
) -> Result<bool, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(false);
    };
    Ok(java_text_starts_with_from(&value, prefix, from_index))
}

fn text_starts_with_from_args(args: &[Option<Vec<u8>>]) -> Result<bool, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(false);
    };
    let Some(prefix) = text_arg(args, 1)? else {
        return Ok(false);
    };
    let from_index = i32_arg(args, 2)?.unwrap_or(0);
    Ok(java_text_starts_with_from(&value, &prefix, from_index))
}

fn java_text_starts_with_from(value: &str, prefix: &str, from_index: i32) -> bool {
    if from_index < 0 {
        return false;
    }
    let len = value.encode_utf16().count() as i32;
    if from_index > len {
        return false;
    }
    let Some(byte_start) = utf16_index_to_byte_boundary(value, from_index as usize) else {
        return false;
    };
    value[byte_start..].starts_with(prefix)
}

fn java_substring_chars(value: &str, begin: isize, end: isize) -> Result<String, String> {
    let len = value.encode_utf16().count() as isize;
    if begin < 0 || end < begin || end > len {
        return Err(format!(
            "Substring bounds out of range: begin={begin}, end={end}, length={len}"
        ));
    }
    let Some(byte_begin) = utf16_index_to_byte_boundary(value, begin as usize) else {
        return Err(format!(
            "Substring bounds split a surrogate pair: begin={begin}, end={end}"
        ));
    };
    let Some(byte_end) = utf16_index_to_byte_boundary(value, end as usize) else {
        return Err(format!(
            "Substring bounds split a surrogate pair: begin={begin}, end={end}"
        ));
    };
    Ok(value[byte_begin..byte_end].to_string())
}

fn utf16_index_to_byte_boundary(value: &str, target: usize) -> Option<usize> {
    let mut units = 0usize;
    for (byte_index, ch) in value.char_indices() {
        if units == target {
            return Some(byte_index);
        }
        if units > target {
            return None;
        }
        units += ch.len_utf16();
    }
    (units == target).then_some(value.len())
}

fn utf16_index_to_next_byte_boundary(value: &str, target: usize) -> Option<usize> {
    let mut units = 0usize;
    for (byte_index, ch) in value.char_indices() {
        if units >= target {
            return Some(byte_index);
        }
        units += ch.len_utf16();
    }
    (units >= target).then_some(value.len())
}

fn utf16_units_before_byte_index(value: &str, byte_index: usize) -> i32 {
    value[..byte_index].encode_utf16().count() as i32
}

fn text_index_literal(args: &[Option<Vec<u8>>], literal: &str, last: bool) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(-1);
    };
    Ok(java_text_index(&value, literal, last))
}

fn text_index_args(args: &[Option<Vec<u8>>], last: bool) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(-1);
    };
    let Some(needle) = text_arg(args, 1)? else {
        return Ok(-1);
    };
    Ok(java_text_index(&value, &needle, last))
}

fn java_text_index(value: &str, needle: &str, last: bool) -> i32 {
    let byte_index = if last {
        value.rfind(needle)
    } else {
        value.find(needle)
    };
    byte_index
        .map(|idx| utf16_units_before_byte_index(value, idx))
        .unwrap_or(-1)
}

fn text_index_literal_from(
    args: &[Option<Vec<u8>>],
    literal: &str,
    from_index: i32,
    last: bool,
) -> Result<i32, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(-1);
    };
    Ok(java_text_index_from(&value, literal, from_index, last))
}

fn text_index_from_args(args: &[Option<Vec<u8>>], last: bool) -> Result<i32, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(-1);
    };
    let Some(needle) = text_arg(args, 1)? else {
        return Ok(-1);
    };
    let from_index = i32_arg(args, 2)?.unwrap_or(0);
    Ok(java_text_index_from(&value, &needle, from_index, last))
}

fn java_text_index_from(value: &str, needle: &str, from_index: i32, last: bool) -> i32 {
    let len = value.encode_utf16().count() as i32;
    if last {
        if from_index < 0 {
            return -1;
        }
        if needle.is_empty() {
            return from_index.min(len);
        }
        return value
            .match_indices(needle)
            .map(|(idx, _)| utf16_units_before_byte_index(value, idx))
            .filter(|idx| *idx <= from_index)
            .max()
            .unwrap_or(-1);
    }

    let start = from_index.max(0);
    if start > len {
        return if needle.is_empty() { len } else { -1 };
    }
    if needle.is_empty() {
        return start;
    }
    let byte_start =
        utf16_index_to_next_byte_boundary(value, start as usize).unwrap_or(value.len());
    value[byte_start..]
        .find(needle)
        .map(|idx| utf16_units_before_byte_index(value, byte_start + idx))
        .unwrap_or(-1)
}

fn parse_range_alias(value: &str) -> Result<(i32, i32), String> {
    let (begin, end) = value
        .split_once(':')
        .ok_or_else(|| format!("Invalid substring range alias: {value}"))?;
    Ok((
        begin.parse().map_err(|e| format!("{e}"))?,
        end.parse().map_err(|e| format!("{e}"))?,
    ))
}

fn parse_index_from_alias(value: &str) -> Result<(&str, i32), String> {
    let (literal, from_index) = value
        .rsplit_once(':')
        .ok_or_else(|| format!("Invalid index-from alias: {value}"))?;
    Ok((literal, from_index.parse().map_err(|e| format!("{e}"))?))
}

fn parse_replace_alias(value: &str) -> Result<(&str, &str), String> {
    value
        .split_once(':')
        .ok_or_else(|| format!("Invalid replace alias: {value}"))
}

fn parse_replace_len_alias(value: &str) -> Result<(&str, &str), String> {
    let (target_len, rest) = value
        .split_once(':')
        .ok_or_else(|| format!("Invalid length-prefixed replace alias: {value}"))?;
    let target_len: usize = target_len.parse().map_err(|e| format!("{e}"))?;
    if target_len > rest.len() || !rest.is_char_boundary(target_len) {
        return Err(format!("Invalid replace target length: {target_len}"));
    }
    Ok(rest.split_at(target_len))
}

fn text_replace_literals(
    args: &[Option<Vec<u8>>],
    target: &str,
    replacement: &str,
) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = first_text_arg(args)? else {
        return Ok(None);
    };
    Ok(Some(value.replace(target, replacement).into_bytes()))
}

fn text_replace_args(args: &[Option<Vec<u8>>]) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = text_arg(args, 0)? else {
        return Ok(None);
    };
    let Some(target) = text_arg(args, 1)? else {
        return Ok(None);
    };
    let Some(replacement) = text_arg(args, 2)? else {
        return Ok(None);
    };
    Ok(Some(value.replace(&target, &replacement).into_bytes()))
}

fn encode_literal(cql_type: &str, literal: &str) -> Result<Vec<u8>, String> {
    match cql_type.to_ascii_lowercase().as_str() {
        "tinyint" => literal
            .parse::<i8>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "smallint" => literal
            .parse::<i16>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "int" => literal
            .parse::<i32>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "bigint" | "counter" => literal
            .parse::<i64>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "float" => literal
            .parse::<f32>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "double" => literal
            .parse::<f64>()
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|e| e.to_string()),
        "boolean" => parse_java_boolean_literal(literal)
            .map(boolean_bytes)
            .ok_or_else(|| format!("Invalid boolean literal: {literal}")),
        "text" | "varchar" | "ascii" => Ok(literal.as_bytes().to_vec()),
        _ => Ok(literal.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udf_manager_registers_and_executes_native_function() {
        let mut mgr = UdfManager::new();
        let def = UdfDefinition {
            name: "double_add".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "rust".to_string(),
            body: "add_i32".to_string(),
            called_on_null_input: false,
        };
        mgr.register_function(def).unwrap();

        let result = mgr
            .execute_function(
                "ks",
                "double_add",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(2i32.to_be_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 5);
    }

    #[test]
    fn uda_manager_accumulates_with_state_function() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "sum_state".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "native".to_string(),
            body: "add_i32".to_string(),
            called_on_null_input: true,
        })
        .unwrap();
        mgr.register_aggregate(UdaDefinition {
            name: "sum_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            state_func: "sum_state".to_string(),
            final_func: None,
            state_type: "int".to_string(),
            init_cond: Some("0".to_string()),
        })
        .unwrap();

        let result = mgr
            .execute_aggregate(
                "ks",
                "sum_int",
                &["int".to_string()],
                &[
                    vec![Some(4i32.to_be_bytes().to_vec())],
                    vec![Some(6i32.to_be_bytes().to_vec())],
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 10);
    }

    #[test]
    fn java_identity_body_maps_to_native_identity() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "echo".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        for (name, arg_type, return_type, body) in [
            (
                "byte_value_of",
                "tinyint",
                "tinyint",
                "return Byte.valueOf(value);",
            ),
            (
                "short_value_of",
                "smallint",
                "smallint",
                "return Short.valueOf(value);",
            ),
            (
                "int_value_of",
                "int",
                "int",
                "return Integer.valueOf(value);",
            ),
            (
                "long_value_of",
                "bigint",
                "bigint",
                "return Long.valueOf(value);",
            ),
            (
                "float_value_of",
                "float",
                "float",
                "return Float.valueOf(value);",
            ),
            (
                "double_value_of",
                "double",
                "double",
                "return Double.valueOf(value);",
            ),
            (
                "bool_value_of",
                "boolean",
                "boolean",
                "return Boolean.valueOf(value);",
            ),
            (
                "bool_boolean_value",
                "boolean",
                "boolean",
                "return value.booleanValue();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "byte_value_of_literal",
                "tinyint",
                "return Byte.valueOf(-12);",
            ),
            (
                "short_value_of_literal",
                "smallint",
                "return Short.valueOf(32000);",
            ),
            (
                "int_value_of_literal",
                "int",
                "return Integer.valueOf(-42);",
            ),
            (
                "long_value_of_literal",
                "bigint",
                "return Long.valueOf(9000000000);",
            ),
            (
                "float_value_of_literal",
                "float",
                "return Float.valueOf(-3.5f);",
            ),
            (
                "double_value_of_literal",
                "double",
                "return Double.valueOf(625.0);",
            ),
            (
                "bool_value_of_literal",
                "boolean",
                "return Boolean.valueOf(true);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let result = mgr
            .execute_function(
                "ks",
                "echo",
                &["text".to_string()],
                &[Some(b"Alice".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(result, b"Alice");

        assert_eq!(
            mgr.execute_function(
                "ks",
                "byte_value_of",
                &["tinyint".to_string()],
                &[Some((-12i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            (-12i8).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "byte_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            (-12i8).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "short_value_of",
                &["smallint".to_string()],
                &[Some(32_000i16.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            32_000i16.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "short_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            32_000i16.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_value_of",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            (-42i32).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "int_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            (-42i32).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_value_of",
                &["bigint".to_string()],
                &[Some(9_000_000_000i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            9_000_000_000i64.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "long_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            9_000_000_000i64.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_value_of",
                &["float".to_string()],
                &[Some((-3.5f32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            (-3.5f32).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "float_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            (-3.5f32).to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_value_of",
                &["double".to_string()],
                &[Some(625.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            625.0f64.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "double_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            625.0f64.to_be_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_value_of",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "bool_value_of_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_boolean_value",
                &["boolean".to_string()],
                &[Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
    }

    #[test]
    fn java_boxed_numeric_conversion_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            ("int_to_byte", "int", "tinyint", "return value.byteValue();"),
            ("long_to_int", "bigint", "int", "return value.intValue();"),
            (
                "float_to_short",
                "float",
                "smallint",
                "return value.shortValue();",
            ),
            ("double_to_int", "double", "int", "return value.intValue();"),
            (
                "double_to_long",
                "double",
                "bigint",
                "return value.longValue();",
            ),
            (
                "int_to_double",
                "int",
                "double",
                "return value.doubleValue();",
            ),
            (
                "double_to_float",
                "double",
                "float",
                "return value.floatValue();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "byte_value_of_to_int_literal",
                "int",
                "return Byte.valueOf(-12).intValue();",
            ),
            (
                "short_value_of_to_long_literal",
                "bigint",
                "return Short.valueOf(32000).longValue();",
            ),
            (
                "int_value_of_to_byte_literal",
                "tinyint",
                "return Integer.valueOf(4660).byteValue();",
            ),
            (
                "long_value_of_to_int_literal",
                "int",
                "return Long.valueOf(9001).intValue();",
            ),
            (
                "double_value_of_to_int_literal",
                "int",
                "return Double.valueOf(123.75).intValue();",
            ),
            (
                "int_value_of_to_double_literal",
                "double",
                "return Integer.valueOf(42).doubleValue();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_byte",
                &["int".to_string()],
                &[Some(0x1234i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![52]
        );
        assert_eq!(
            mgr.execute_function("ks", "int_value_of_to_byte_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![52]
        );
        let byte_to_int_literal = mgr
            .execute_function("ks", "byte_value_of_to_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(byte_to_int_literal.try_into().unwrap()),
            -12
        );
        let short_to_long_literal = mgr
            .execute_function("ks", "short_value_of_to_long_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(short_to_long_literal.try_into().unwrap()),
            32000
        );

        let long_to_int = mgr
            .execute_function(
                "ks",
                "long_to_int",
                &["bigint".to_string()],
                &[Some(0x0000_0001_0000_0002i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_to_int.try_into().unwrap()), 2);
        let long_to_int_literal = mgr
            .execute_function("ks", "long_value_of_to_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(long_to_int_literal.try_into().unwrap()),
            9001
        );

        let float_to_short = mgr
            .execute_function(
                "ks",
                "float_to_short",
                &["float".to_string()],
                &[Some(32_768.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i16::from_be_bytes(float_to_short.try_into().unwrap()),
            i16::MIN
        );

        let double_to_int = mgr
            .execute_function(
                "ks",
                "double_to_int",
                &["double".to_string()],
                &[Some(1.0e40f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_to_int.try_into().unwrap()),
            i32::MAX
        );
        let double_to_int_literal = mgr
            .execute_function("ks", "double_value_of_to_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_to_int_literal.try_into().unwrap()),
            123
        );

        let double_to_long = mgr
            .execute_function(
                "ks",
                "double_to_long",
                &["double".to_string()],
                &[Some(f64::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(double_to_long.try_into().unwrap()), 0);

        let int_to_double = mgr
            .execute_function(
                "ks",
                "int_to_double",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(int_to_double.try_into().unwrap()), 42.0);
        let int_to_double_literal = mgr
            .execute_function("ks", "int_value_of_to_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(int_to_double_literal.try_into().unwrap()),
            42.0
        );

        let double_to_float = mgr
            .execute_function(
                "ks",
                "double_to_float",
                &["double".to_string()],
                &[Some(1.25f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(double_to_float.try_into().unwrap()),
            1.25
        );
    }

    #[test]
    fn java_lowercase_and_addition_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "lower_name".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val.toLowerCase();".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "upper_name".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val.toUpperCase();".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "plus".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return a + b;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "lower_name",
                &["text".to_string()],
                &[Some(b"ALICE".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"alice"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "lower_name",
                &["text".to_string()],
                &[Some("ÅDA".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            "åda".as_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "upper_name",
                &["text".to_string()],
                &[Some("åda".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            "ÅDA".as_bytes()
        );

        let sum = mgr
            .execute_function(
                "ks",
                "plus",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(2i32.to_be_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(sum.try_into().unwrap()), 5);
    }

    #[test]
    fn java_subtraction_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "minus".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return a - b;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let value = mgr
            .execute_function(
                "ks",
                "minus",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(7i32.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(value.try_into().unwrap()), 5);
    }

    #[test]
    fn java_multiply_divide_and_remainder_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("multiply", "return a * b;"),
            ("divide", "return a / b;"),
            ("remainder", "return a % b;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["int".to_string(), "int".to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let args = [
            Some(21i32.to_be_bytes().to_vec()),
            Some(2i32.to_be_bytes().to_vec()),
        ];
        let multiply = mgr
            .execute_function(
                "ks",
                "multiply",
                &["int".to_string(), "int".to_string()],
                &args,
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(multiply.try_into().unwrap()), 42);

        let divide = mgr
            .execute_function(
                "ks",
                "divide",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(84i32.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(divide.try_into().unwrap()), 42);

        let remainder = mgr
            .execute_function(
                "ks",
                "remainder",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(42i32.to_be_bytes().to_vec()),
                    Some(5i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(remainder.try_into().unwrap()), 2);
    }

    #[test]
    fn java_literal_arithmetic_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "increment".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return value + 1;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "decrement".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return value - 1;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "big_increment".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string()],
            return_type: "bigint".to_string(),
            language: "java".to_string(),
            body: "return value + 10;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let incremented = mgr
            .execute_function(
                "ks",
                "increment",
                &["int".to_string()],
                &[Some(41i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(incremented.try_into().unwrap()), 42);

        let decremented = mgr
            .execute_function(
                "ks",
                "decrement",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(decremented.try_into().unwrap()), 41);

        let big_incremented = mgr
            .execute_function(
                "ks",
                "big_increment",
                &["bigint".to_string()],
                &[Some(32i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(big_incremented.try_into().unwrap()), 42);
    }

    #[test]
    fn java_literal_multiply_divide_and_remainder_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("double_value", "return value * 2;"),
            ("halve_value", "return value / 2;"),
            ("remainder_value", "return value % 5;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["int".to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let doubled = mgr
            .execute_function(
                "ks",
                "double_value",
                &["int".to_string()],
                &[Some(21i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(doubled.try_into().unwrap()), 42);

        let halved = mgr
            .execute_function(
                "ks",
                "halve_value",
                &["int".to_string()],
                &[Some(84i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(halved.try_into().unwrap()), 42);

        let remainder = mgr
            .execute_function(
                "ks",
                "remainder_value",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(remainder.try_into().unwrap()), 2);

        mgr.register_function(UdfDefinition {
            name: "big_double".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string()],
            return_type: "bigint".to_string(),
            language: "java".to_string(),
            body: "return value * 2;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        let big_doubled = mgr
            .execute_function(
                "ks",
                "big_double",
                &["bigint".to_string()],
                &[Some(21i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(big_doubled.try_into().unwrap()), 42);
    }

    #[test]
    fn java_float_and_double_arithmetic_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("float_add", "float", "return a + b;"),
            ("float_scale", "float", "return value * 2.5;"),
            ("double_divide", "double", "return a / b;"),
            ("double_remainder", "double", "return value % 2.0;"),
        ] {
            let arg_types = if name == "float_scale" || name == "double_remainder" {
                vec![return_type.to_string()]
            } else {
                vec![return_type.to_string(), return_type.to_string()]
            };
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_sum = mgr
            .execute_function(
                "ks",
                "float_add",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(1.25f32.to_be_bytes().to_vec()),
                    Some(2.75f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_sum.try_into().unwrap()), 4.0);

        let scaled = mgr
            .execute_function(
                "ks",
                "float_scale",
                &["float".to_string()],
                &[Some(4.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(scaled.try_into().unwrap()), 10.0);

        let divided = mgr
            .execute_function(
                "ks",
                "double_divide",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(21.0f64.to_be_bytes().to_vec()),
                    Some(2.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(divided.try_into().unwrap()), 10.5);

        let remainder = mgr
            .execute_function(
                "ks",
                "double_remainder",
                &["double".to_string()],
                &[Some(5.5f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(remainder.try_into().unwrap()), 1.5);
    }

    #[test]
    fn java_float_static_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("float_sum", "float", "return Float.sum(a, b);"),
            ("float_max", "float", "return Float.max(a, b);"),
            ("float_min", "float", "return Float.min(a, b);"),
            ("double_sum", "double", "return Double.sum(a, b);"),
            ("double_max", "double", "return Double.max(a, b);"),
            ("double_min", "double", "return Double.min(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![return_type.to_string(), return_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, return_type, body) in [
            (
                "float_sum_literal",
                "float",
                "return Float.sum(1.25f, 2.75f);",
            ),
            (
                "float_min_literal",
                "float",
                "return Float.min(-0.0f, 0.0f);",
            ),
            (
                "double_sum_literal",
                "double",
                "return Double.sum(2.5, 3.0);",
            ),
            (
                "double_min_literal",
                "double",
                "return Double.min(-0.0, 0.0);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_sum_literal = mgr
            .execute_function("ks", "float_sum_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_sum_literal.try_into().unwrap()),
            4.0
        );

        let float_min_literal = mgr
            .execute_function("ks", "float_min_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_min_literal.try_into().unwrap()).to_bits(),
            0x8000_0000
        );

        let double_sum_literal = mgr
            .execute_function("ks", "double_sum_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_sum_literal.try_into().unwrap()),
            5.5
        );

        let double_min_literal = mgr
            .execute_function("ks", "double_min_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_min_literal.try_into().unwrap()).to_bits(),
            0x8000_0000_0000_0000
        );

        let float_sum = mgr
            .execute_function(
                "ks",
                "float_sum",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(1.25f32.to_be_bytes().to_vec()),
                    Some(2.75f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_sum.try_into().unwrap()), 4.0);

        let float_max_zero = mgr
            .execute_function(
                "ks",
                "float_max",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-0.0f32).to_be_bytes().to_vec()),
                    Some(0.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_max_zero.try_into().unwrap()).to_bits(),
            0x0000_0000
        );

        let float_min_zero = mgr
            .execute_function(
                "ks",
                "float_min",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-0.0f32).to_be_bytes().to_vec()),
                    Some(0.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_min_zero.try_into().unwrap()).to_bits(),
            0x8000_0000
        );

        let float_max_nan = mgr
            .execute_function(
                "ks",
                "float_max",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(f32::NAN.to_be_bytes().to_vec()),
                    Some(1.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert!(f32::from_be_bytes(float_max_nan.try_into().unwrap()).is_nan());

        let double_sum = mgr
            .execute_function(
                "ks",
                "double_sum",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(2.5f64.to_be_bytes().to_vec()),
                    Some(3.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_sum.try_into().unwrap()), 5.5);

        let double_max = mgr
            .execute_function(
                "ks",
                "double_max",
                &["double".to_string(), "double".to_string()],
                &[
                    Some((-7.0f64).to_be_bytes().to_vec()),
                    Some(2.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_max.try_into().unwrap()), 2.0);

        let double_min_zero = mgr
            .execute_function(
                "ks",
                "double_min",
                &["double".to_string(), "double".to_string()],
                &[
                    Some((-0.0f64).to_be_bytes().to_vec()),
                    Some(0.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_min_zero.try_into().unwrap()).to_bits(),
            0x8000_0000_0000_0000
        );

        let double_min_nan = mgr
            .execute_function(
                "ks",
                "double_min",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(f64::NAN.to_be_bytes().to_vec()),
                    Some(1.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert!(f64::from_be_bytes(double_min_nan.try_into().unwrap()).is_nan());
    }

    #[test]
    fn java_division_by_zero_body_returns_error() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "divide_by_literal_zero".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return value / 0;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        assert!(
            mgr.execute_function(
                "ks",
                "divide_by_literal_zero",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .is_err()
        );
    }

    #[test]
    fn java_unary_and_boolean_operator_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "negate_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return -value;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "not_bool".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["boolean".to_string()],
            return_type: "boolean".to_string(),
            language: "java".to_string(),
            body: "return !value;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        for (name, body) in [
            ("bool_and", "return a && b;"),
            ("bool_or", "return a || b;"),
            ("bool_logical_and", "return Boolean.logicalAnd(a, b);"),
            ("bool_logical_or", "return Boolean.logicalOr(a, b);"),
            ("bool_logical_xor", "return Boolean.logicalXor(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["boolean".to_string(), "boolean".to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("bool_and_literal", "return true && false;"),
            ("bool_or_literal", "return true || false;"),
            (
                "bool_logical_xor_literal",
                "return Boolean.logicalXor(true, false);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let negated = mgr
            .execute_function(
                "ks",
                "negate_int",
                &["int".to_string()],
                &[Some(7i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(negated.try_into().unwrap()), -7);

        assert_eq!(
            mgr.execute_function("ks", "not_bool", &["boolean".to_string()], &[Some(vec![0])],)
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_and",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_or",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_logical_and",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_logical_or",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_logical_xor",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "bool_and_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function("ks", "bool_or_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "bool_logical_xor_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_literal_text_concat_bodies_preserve_literal_case() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "suffix".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return value + \"_X\";".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "prefix".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return \"DB_\" + value;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "suffix",
                &["text".to_string()],
                &[Some(b"node".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node_X"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "prefix",
                &["text".to_string()],
                &[Some(b"node".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"DB_node"
        );
    }

    #[test]
    fn java_literal_comparison_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("gt_literal", "int", "return value > 10;"),
            ("text_equals", "text", "return value.equals(\"Ada\");"),
            ("text_not_equals", "text", "return !value.equals(\"skip\");"),
            ("bool_equals", "boolean", "return value == true;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        assert_eq!(
            mgr.execute_function(
                "ks",
                "gt_literal",
                &["int".to_string()],
                &[Some(11i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "gt_literal",
                &["int".to_string()],
                &[Some(10i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_equals",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_not_equals",
                &["text".to_string()],
                &[Some(b"keep".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_equals",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_float_and_double_comparison_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("float_gt_integer_literal", "float", "return value > 2;"),
            ("float_lte_literal", "float", "return value <= 2.5f;"),
            ("double_eq_integer_literal", "double", "return value == 3;"),
            ("double_ne_literal", "double", "return value != 4.25d;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_gt_integer_literal",
                &["float".to_string()],
                &[Some(2.25f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_lte_literal",
                &["float".to_string()],
                &[Some(2.5f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_eq_integer_literal",
                &["double".to_string()],
                &[Some(3.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_ne_literal",
                &["double".to_string()],
                &[Some(4.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_argument_comparison_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("int_gt", "int", "return a > b;"),
            ("bigint_lte", "bigint", "return a <= b;"),
            ("float_ne", "float", "return a != b;"),
            ("double_gte", "double", "return a >= b;"),
            ("bool_eq", "boolean", "return a == b;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, body) in [
            (
                "text_objects_eq_literal",
                "return Objects.equals(value, \"Node,DB\");",
            ),
            (
                "text_objects_ne_literal_left",
                "return !java.util.Objects.equals(\"Node,DB\", value);",
            ),
            (
                "objects_null_eq_literal",
                "return Objects.equals(null, null);",
            ),
            ("objects_int_ne_literal", "return !Objects.equals(42, 7);"),
            (
                "objects_text_eq_literal",
                "return java.util.Objects.equals(\"Ada\", \"Ada\");",
            ),
            (
                "objects_java_lang_bool_eq_literal",
                "return Objects.equals(java.lang.Boolean.TRUE, true);",
            ),
            (
                "objects_float_double_ne_literal",
                "return !Objects.equals(1.0f, 1.0);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: if name.starts_with("text_") {
                    vec!["text".to_string()]
                } else {
                    vec![]
                },
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_gt",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(12i32.to_be_bytes().to_vec()),
                    Some(10i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bigint_lte",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(42i64.to_be_bytes().to_vec()),
                    Some(42i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_ne",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(1.25f32.to_be_bytes().to_vec()),
                    Some(2.5f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_gte",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(9.5f64.to_be_bytes().to_vec()),
                    Some(9.25f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_eq",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_objects_eq_literal",
                &["text".to_string()],
                &[Some(b"Node,DB".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_objects_ne_literal_left",
                &["text".to_string()],
                &[Some(b"node-db".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        for name in [
            "objects_null_eq_literal",
            "objects_int_ne_literal",
            "objects_text_eq_literal",
            "objects_java_lang_bool_eq_literal",
            "objects_float_double_ne_literal",
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                vec![1]
            );
        }
    }

    #[test]
    fn java_float_and_double_unary_negation_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [("neg_float", "float"), ("neg_double", "double")] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: "return -value;".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let negated_float = mgr
            .execute_function(
                "ks",
                "neg_float",
                &["float".to_string()],
                &[Some(3.5f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(negated_float.try_into().unwrap()), -3.5);

        let negated_double = mgr
            .execute_function(
                "ks",
                "neg_double",
                &["double".to_string()],
                &[Some(9.25f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(negated_double.try_into().unwrap()),
            -9.25
        );
    }

    #[test]
    fn java_math_abs_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [
            ("abs_int", "int"),
            ("abs_bigint", "bigint"),
            ("abs_float", "float"),
            ("abs_double", "double"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: "return Math.abs(value);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            ("abs_int_literal", "int", "return Math.abs(-42);"),
            ("abs_bigint_literal", "bigint", "return Math.abs(-9001);"),
            ("abs_float_literal", "float", "return Math.abs(-3.5f);"),
            ("abs_double_literal", "double", "return Math.abs(-9.25);"),
            (
                "strict_abs_int_literal",
                "int",
                "return StrictMath.abs(-42);",
            ),
            (
                "java_lang_strict_abs_int_literal",
                "int",
                "return java.lang.StrictMath.abs(-42);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        mgr.register_function(UdfDefinition {
            name: "java_lang_abs_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return java.lang.Math.abs(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let int_abs = mgr
            .execute_function(
                "ks",
                "abs_int",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_abs.try_into().unwrap()), 42);

        let bigint_abs = mgr
            .execute_function(
                "ks",
                "abs_bigint",
                &["bigint".to_string()],
                &[Some((-9001i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(bigint_abs.try_into().unwrap()), 9001);

        let float_abs = mgr
            .execute_function(
                "ks",
                "abs_float",
                &["float".to_string()],
                &[Some((-3.5f32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_abs.try_into().unwrap()), 3.5);

        let double_abs = mgr
            .execute_function(
                "ks",
                "abs_double",
                &["double".to_string()],
                &[Some((-9.25f64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_abs.try_into().unwrap()), 9.25);
        let int_abs_literal = mgr
            .execute_function("ks", "abs_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_abs_literal.try_into().unwrap()), 42);
        let bigint_abs_literal = mgr
            .execute_function("ks", "abs_bigint_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(bigint_abs_literal.try_into().unwrap()),
            9001
        );
        let float_abs_literal = mgr
            .execute_function("ks", "abs_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_abs_literal.try_into().unwrap()),
            3.5
        );
        let double_abs_literal = mgr
            .execute_function("ks", "abs_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_abs_literal.try_into().unwrap()),
            9.25
        );
        let strict_int_abs_literal = mgr
            .execute_function("ks", "strict_abs_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(strict_int_abs_literal.try_into().unwrap()),
            42
        );
        let java_lang_int_abs = mgr
            .execute_function(
                "ks",
                "java_lang_abs_int",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(java_lang_int_abs.try_into().unwrap()),
            42
        );
        let java_lang_strict_int_abs_literal = mgr
            .execute_function("ks", "java_lang_strict_abs_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(java_lang_strict_int_abs_literal.try_into().unwrap()),
            42
        );
    }

    #[test]
    fn java_math_constant_bodies_map_to_native_constants() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("math_pi", "return Math.PI;"),
            ("math_e", "return Math.E;"),
            ("strict_math_pi", "return StrictMath.PI;"),
            ("java_lang_math_e", "return java.lang.Math.E;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "double".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let pi = mgr
            .execute_function("ks", "math_pi", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(pi.try_into().unwrap()),
            std::f64::consts::PI
        );

        let e = mgr
            .execute_function("ks", "math_e", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(e.try_into().unwrap()),
            std::f64::consts::E
        );
        let strict_pi = mgr
            .execute_function("ks", "strict_math_pi", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(strict_pi.try_into().unwrap()),
            std::f64::consts::PI
        );
        let java_lang_e = mgr
            .execute_function("ks", "java_lang_math_e", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(java_lang_e.try_into().unwrap()),
            std::f64::consts::E
        );
    }

    #[test]
    fn java_boxed_primitive_constant_bodies_map_to_native_constants() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("byte_max", "tinyint", "return Byte.MAX_VALUE;"),
            ("byte_min", "tinyint", "return Byte.MIN_VALUE;"),
            ("byte_size", "int", "return Byte.SIZE;"),
            ("byte_bytes", "int", "return Byte.BYTES;"),
            ("short_max", "smallint", "return Short.MAX_VALUE;"),
            ("short_min", "smallint", "return Short.MIN_VALUE;"),
            ("short_size", "int", "return Short.SIZE;"),
            ("short_bytes", "int", "return Short.BYTES;"),
            ("int_max", "int", "return Integer.MAX_VALUE;"),
            (
                "java_lang_int_max",
                "int",
                "return java.lang.Integer.MAX_VALUE;",
            ),
            ("int_min", "int", "return Integer.MIN_VALUE;"),
            ("int_size", "int", "return Integer.SIZE;"),
            ("int_bytes", "int", "return Integer.BYTES;"),
            ("long_max", "bigint", "return Long.MAX_VALUE;"),
            ("long_min", "bigint", "return Long.MIN_VALUE;"),
            ("long_size", "int", "return Long.SIZE;"),
            ("long_bytes", "int", "return Long.BYTES;"),
            ("float_size", "int", "return Float.SIZE;"),
            ("float_bytes", "int", "return Float.BYTES;"),
            ("double_size", "int", "return Double.SIZE;"),
            ("double_bytes", "int", "return Double.BYTES;"),
            ("boolean_true", "boolean", "return Boolean.TRUE;"),
            ("boolean_false", "boolean", "return Boolean.FALSE;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [
            ("byte_max", i8::MAX.to_be_bytes().to_vec()),
            ("byte_min", i8::MIN.to_be_bytes().to_vec()),
            ("byte_size", 8i32.to_be_bytes().to_vec()),
            ("byte_bytes", 1i32.to_be_bytes().to_vec()),
            ("short_max", i16::MAX.to_be_bytes().to_vec()),
            ("short_min", i16::MIN.to_be_bytes().to_vec()),
            ("short_size", 16i32.to_be_bytes().to_vec()),
            ("short_bytes", 2i32.to_be_bytes().to_vec()),
            ("int_max", i32::MAX.to_be_bytes().to_vec()),
            ("java_lang_int_max", i32::MAX.to_be_bytes().to_vec()),
            ("int_min", i32::MIN.to_be_bytes().to_vec()),
            ("int_size", 32i32.to_be_bytes().to_vec()),
            ("int_bytes", 4i32.to_be_bytes().to_vec()),
            ("long_max", i64::MAX.to_be_bytes().to_vec()),
            ("long_min", i64::MIN.to_be_bytes().to_vec()),
            ("long_size", 64i32.to_be_bytes().to_vec()),
            ("long_bytes", 8i32.to_be_bytes().to_vec()),
            ("float_size", 32i32.to_be_bytes().to_vec()),
            ("float_bytes", 4i32.to_be_bytes().to_vec()),
            ("double_size", 64i32.to_be_bytes().to_vec()),
            ("double_bytes", 8i32.to_be_bytes().to_vec()),
            ("boolean_true", vec![1]),
            ("boolean_false", vec![0]),
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                expected
            );
        }
    }

    #[test]
    fn java_math_signum_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [("signum_float", "float"), ("signum_double", "double")] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: "return Math.signum(value);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "signum_float_literal",
                "float",
                "return Math.signum(-3.5f);",
            ),
            (
                "signum_double_literal",
                "double",
                "return Math.signum(9.25);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let signum_float = mgr
            .execute_function(
                "ks",
                "signum_float",
                &["float".to_string()],
                &[Some((-3.5f32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(signum_float.try_into().unwrap()), -1.0);

        let signum_double = mgr
            .execute_function(
                "ks",
                "signum_double",
                &["double".to_string()],
                &[Some(9.25f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(signum_double.try_into().unwrap()), 1.0);
        let signum_float_literal = mgr
            .execute_function("ks", "signum_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(signum_float_literal.try_into().unwrap()),
            -1.0
        );
        let signum_double_literal = mgr
            .execute_function("ks", "signum_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(signum_double_literal.try_into().unwrap()),
            1.0
        );
    }

    #[test]
    fn java_math_round_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type) in [
            ("round_float", "float", "int"),
            ("round_double", "double", "bigint"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: "return Math.round(value);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            ("round_float_literal", "int", "return Math.round(2.5f);"),
            ("round_double_literal", "bigint", "return Math.round(-1.5);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let rounded_float = mgr
            .execute_function(
                "ks",
                "round_float",
                &["float".to_string()],
                &[Some(2.5f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(rounded_float.try_into().unwrap()), 3);

        let rounded_double = mgr
            .execute_function(
                "ks",
                "round_double",
                &["double".to_string()],
                &[Some((-1.5f64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(rounded_double.try_into().unwrap()), -1);
        let rounded_float_literal = mgr
            .execute_function("ks", "round_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(rounded_float_literal.try_into().unwrap()),
            3
        );
        let rounded_double_literal = mgr
            .execute_function("ks", "round_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(rounded_double_literal.try_into().unwrap()),
            -1
        );
    }

    #[test]
    fn java_math_get_exponent_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [("exp_float", "float"), ("exp_double", "double")] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: "return Math.getExponent(value);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, body) in [
            ("exp_float_literal", "return Math.getExponent(8.0f);"),
            ("exp_double_literal", "return Math.getExponent(0.125);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_exponent = mgr
            .execute_function(
                "ks",
                "exp_float",
                &["float".to_string()],
                &[Some(8.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(float_exponent.try_into().unwrap()), 3);

        let float_subnormal_exponent = mgr
            .execute_function(
                "ks",
                "exp_float",
                &["float".to_string()],
                &[Some(f32::from_bits(1).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(float_subnormal_exponent.try_into().unwrap()),
            -127
        );

        let double_exponent = mgr
            .execute_function(
                "ks",
                "exp_double",
                &["double".to_string()],
                &[Some(0.125f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(double_exponent.try_into().unwrap()), -3);

        let double_infinite_exponent = mgr
            .execute_function(
                "ks",
                "exp_double",
                &["double".to_string()],
                &[Some(f64::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_infinite_exponent.try_into().unwrap()),
            1024
        );
        let float_exponent_literal = mgr
            .execute_function("ks", "exp_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(float_exponent_literal.try_into().unwrap()),
            3
        );
        let double_exponent_literal = mgr
            .execute_function("ks", "exp_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_exponent_literal.try_into().unwrap()),
            -3
        );
    }

    #[test]
    fn java_math_exact_integer_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("inc_int", "int", "return Math.incrementExact(value);"),
            ("dec_bigint", "bigint", "return Math.decrementExact(value);"),
            ("neg_int", "int", "return Math.negateExact(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            ("inc_int_literal", "int", "return Math.incrementExact(41);"),
            (
                "dec_bigint_literal",
                "bigint",
                "return Math.decrementExact(9001);",
            ),
            ("neg_int_literal", "int", "return Math.negateExact(-17);"),
            (
                "to_int_literal",
                "int",
                "return Math.toIntExact(2147483647);",
            ),
            (
                "strict_inc_int_literal",
                "int",
                "return StrictMath.incrementExact(41);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let inc_int = mgr
            .execute_function(
                "ks",
                "inc_int",
                &["int".to_string()],
                &[Some(41i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(inc_int.try_into().unwrap()), 42);

        let dec_bigint = mgr
            .execute_function(
                "ks",
                "dec_bigint",
                &["bigint".to_string()],
                &[Some(9001i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(dec_bigint.try_into().unwrap()), 9000);

        let neg_int = mgr
            .execute_function(
                "ks",
                "neg_int",
                &["int".to_string()],
                &[Some((-17i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(neg_int.try_into().unwrap()), 17);
        for (name, expected) in [
            ("inc_int_literal", 42),
            ("neg_int_literal", 17),
            ("to_int_literal", i32::MAX),
            ("strict_inc_int_literal", 42),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        let dec_bigint_literal = mgr
            .execute_function("ks", "dec_bigint_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(dec_bigint_literal.try_into().unwrap()),
            9000
        );
    }

    #[test]
    fn java_math_exact_integer_bodies_report_overflow() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "inc_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Math.incrementExact(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "inc_int",
                &["int".to_string()],
                &[Some(i32::MAX.to_be_bytes().to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Math.incrementExact"));
    }

    #[test]
    fn java_math_exact_integer_binary_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("add_int", "int", "return Math.addExact(a, b);"),
            ("sub_bigint", "bigint", "return Math.subtractExact(a, b);"),
            ("mul_int", "int", "return Math.multiplyExact(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("add_int_literal", "return Math.addExact(40, 2);"),
            ("sub_int_literal", "return Math.subtractExact(9001, 1);"),
            ("mul_int_literal", "return Math.multiplyExact(7, 6);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [
            ("add_int_literal", 42),
            ("sub_int_literal", 9000),
            ("mul_int_literal", 42),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }

        let add_int = mgr
            .execute_function(
                "ks",
                "add_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(40i32.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(add_int.try_into().unwrap()), 42);

        let sub_bigint = mgr
            .execute_function(
                "ks",
                "sub_bigint",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(9001i64.to_be_bytes().to_vec()),
                    Some(1i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(sub_bigint.try_into().unwrap()), 9000);

        let mul_int = mgr
            .execute_function(
                "ks",
                "mul_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(7i32.to_be_bytes().to_vec()),
                    Some(6i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(mul_int.try_into().unwrap()), 42);
    }

    #[test]
    fn java_math_exact_integer_binary_bodies_report_overflow() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "mul_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Math.multiplyExact(a, b);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "mul_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(i32::MAX.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap_err();
        assert!(err.contains("Math.multiplyExact"));
    }

    #[test]
    fn java_math_wide_multiply_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_types, body) in [
            (
                "mul_full",
                vec!["int".to_string(), "int".to_string()],
                "return Math.multiplyFull(a, b);",
            ),
            (
                "mul_high",
                vec!["bigint".to_string(), "bigint".to_string()],
                "return Math.multiplyHigh(a, b);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "bigint".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            (
                "mul_full_literal",
                "return Math.multiplyFull(2147483647, 2);",
            ),
            ("mul_high_literal", "return Math.multiplyHigh(-9, 2);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "bigint".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let full_literal = mgr
            .execute_function("ks", "mul_full_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(full_literal.try_into().unwrap()),
            i32::MAX as i64 * 2
        );

        let high_literal = mgr
            .execute_function("ks", "mul_high_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(high_literal.try_into().unwrap()), -1);

        let full = mgr
            .execute_function(
                "ks",
                "mul_full",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(i32::MAX.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(full.try_into().unwrap()),
            i32::MAX as i64 * 2
        );

        let high = mgr
            .execute_function(
                "ks",
                "mul_high",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(i64::MIN.to_be_bytes().to_vec()),
                    Some(2i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(high.try_into().unwrap()), -1);
    }

    #[test]
    fn java_math_floor_div_mod_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("floor_div_int", "int", "return Math.floorDiv(a, b);"),
            ("floor_mod_int", "int", "return Math.floorMod(a, b);"),
            ("floor_div_bigint", "bigint", "return Math.floorDiv(a, b);"),
            ("floor_mod_bigint", "bigint", "return Math.floorMod(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("floor_div_int_literal", "return Math.floorDiv(-7, 3);"),
            ("floor_mod_int_literal", "return Math.floorMod(-7, 3);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [("floor_div_int_literal", -3), ("floor_mod_int_literal", 2)] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }

        let floor_div_int = mgr
            .execute_function(
                "ks",
                "floor_div_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-7i32).to_be_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(floor_div_int.try_into().unwrap()), -3);

        let floor_mod_int = mgr
            .execute_function(
                "ks",
                "floor_mod_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-7i32).to_be_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(floor_mod_int.try_into().unwrap()), 2);

        let floor_div_bigint = mgr
            .execute_function(
                "ks",
                "floor_div_bigint",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(7i64.to_be_bytes().to_vec()),
                    Some((-3i64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(floor_div_bigint.try_into().unwrap()), -3);

        let floor_mod_bigint = mgr
            .execute_function(
                "ks",
                "floor_mod_bigint",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(7i64.to_be_bytes().to_vec()),
                    Some((-3i64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(floor_mod_bigint.try_into().unwrap()), -2);
    }

    #[test]
    fn java_math_floor_div_mod_bodies_report_division_by_zero() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "floor_div_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Math.floorDiv(a, b);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "floor_div_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(1i32.to_be_bytes().to_vec()),
                    Some(0i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap_err();
        assert!(err.contains("Division by zero"));
    }

    #[test]
    fn java_math_to_int_exact_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "to_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Math.toIntExact(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let result = mgr
            .execute_function(
                "ks",
                "to_int",
                &["bigint".to_string()],
                &[Some(42i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 42);
    }

    #[test]
    fn java_math_to_int_exact_body_reports_overflow() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "to_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Math.toIntExact(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "to_int",
                &["bigint".to_string()],
                &[Some((i32::MAX as i64 + 1).to_be_bytes().to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Math.toIntExact"));
    }

    #[test]
    fn java_integer_parse_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("parse_byte", "tinyint", "return Byte.parseByte(value);"),
            ("parse_short", "smallint", "return Short.parseShort(value);"),
            ("parse_int", "int", "return Integer.parseInt(value);"),
            ("parse_long", "bigint", "return Long.parseLong(value);"),
            ("value_of_byte", "tinyint", "return Byte.valueOf(value);"),
            ("value_of_short", "smallint", "return Short.valueOf(value);"),
            ("value_of_int", "int", "return Integer.valueOf(value);"),
            ("value_of_long", "bigint", "return Long.valueOf(value);"),
            (
                "parse_byte_radix",
                "tinyint",
                "return Byte.parseByte(value, 16);",
            ),
            (
                "parse_short_radix",
                "smallint",
                "return Short.valueOf(value, 8);",
            ),
            (
                "parse_int_radix",
                "int",
                "return Integer.parseInt(value, 16);",
            ),
            (
                "parse_long_radix",
                "bigint",
                "return Long.valueOf(value, 36);",
            ),
            (
                "parse_unsigned_int",
                "int",
                "return Integer.parseUnsignedInt(value);",
            ),
            (
                "parse_unsigned_long",
                "bigint",
                "return Long.parseUnsignedLong(value);",
            ),
            (
                "parse_unsigned_int_radix",
                "int",
                "return Integer.parseUnsignedInt(value, 16);",
            ),
            (
                "parse_unsigned_long_radix",
                "bigint",
                "return Long.parseUnsignedLong(value, 36);",
            ),
            ("decode_byte", "tinyint", "return Byte.decode(value);"),
            ("decode_short", "smallint", "return Short.decode(value);"),
            ("decode_int", "int", "return Integer.decode(value);"),
            ("decode_long", "bigint", "return Long.decode(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        mgr.register_function(UdfDefinition {
            name: "parse_unsigned_int_radix_arg".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Integer.parseUnsignedInt(value, radix);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "parse_unsigned_long_radix_arg".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "int".to_string()],
            return_type: "bigint".to_string(),
            language: "java".to_string(),
            body: "return Long.parseUnsignedLong(value, radix);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        for (name, return_type, body) in [
            (
                "parse_int_literal",
                "int",
                "return Integer.parseInt(\"2A\", 16);",
            ),
            (
                "value_of_long_literal",
                "bigint",
                "return Long.valueOf(\"21I3V9\", 36);",
            ),
            (
                "parse_unsigned_int_literal",
                "int",
                "return Integer.parseUnsignedInt(\"FFFFFFFF\", 16);",
            ),
            (
                "parse_unsigned_long_literal",
                "bigint",
                "return Long.parseUnsignedLong(\"18446744073709551615\");",
            ),
            (
                "decode_int_literal",
                "int",
                "return Integer.decode(\"-0X80000000\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let parsed_byte = mgr
            .execute_function(
                "ks",
                "parse_byte",
                &["text".to_string()],
                &[Some(b"-12".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i8::from_be_bytes(parsed_byte.try_into().unwrap()), -12);

        let parsed_short = mgr
            .execute_function(
                "ks",
                "parse_short",
                &["text".to_string()],
                &[Some(b"32000".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i16::from_be_bytes(parsed_short.try_into().unwrap()), 32_000);

        let parsed_int = mgr
            .execute_function(
                "ks",
                "parse_int",
                &["text".to_string()],
                &[Some(b"-42".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(parsed_int.try_into().unwrap()), -42);

        let parsed_long = mgr
            .execute_function(
                "ks",
                "parse_long",
                &["text".to_string()],
                &[Some(b"9000000000".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_long.try_into().unwrap()),
            9_000_000_000
        );

        let value_of_int = mgr
            .execute_function(
                "ks",
                "value_of_int",
                &["text".to_string()],
                &[Some(b"42".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(value_of_int.try_into().unwrap()), 42);

        let parsed_byte_radix = mgr
            .execute_function(
                "ks",
                "parse_byte_radix",
                &["text".to_string()],
                &[Some(b"7f".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i8::from_be_bytes(parsed_byte_radix.try_into().unwrap()),
            127
        );

        let parsed_short_radix = mgr
            .execute_function(
                "ks",
                "parse_short_radix",
                &["text".to_string()],
                &[Some(b"77777".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i16::from_be_bytes(parsed_short_radix.try_into().unwrap()),
            32_767
        );

        let parsed_int_radix = mgr
            .execute_function(
                "ks",
                "parse_int_radix",
                &["text".to_string()],
                &[Some(b"-2a".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_int_radix.try_into().unwrap()),
            -42
        );

        let parsed_long_radix = mgr
            .execute_function(
                "ks",
                "parse_long_radix",
                &["text".to_string()],
                &[Some(b"21i3v9".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_long_radix.try_into().unwrap()),
            123_456_789
        );

        let parsed_unsigned_int = mgr
            .execute_function(
                "ks",
                "parse_unsigned_int",
                &["text".to_string()],
                &[Some(b"4294967295".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_unsigned_int.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_long = mgr
            .execute_function(
                "ks",
                "parse_unsigned_long",
                &["text".to_string()],
                &[Some(b"18446744073709551615".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_unsigned_long.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_int_radix = mgr
            .execute_function(
                "ks",
                "parse_unsigned_int_radix",
                &["text".to_string()],
                &[Some(b"ffffffff".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_unsigned_int_radix.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_long_radix = mgr
            .execute_function(
                "ks",
                "parse_unsigned_long_radix",
                &["text".to_string()],
                &[Some(b"3w5e11264sgsf".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_unsigned_long_radix.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_int_radix_arg = mgr
            .execute_function(
                "ks",
                "parse_unsigned_int_radix_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"+ffffffff".to_vec()),
                    Some(16i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_unsigned_int_radix_arg.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_long_radix_arg = mgr
            .execute_function(
                "ks",
                "parse_unsigned_long_radix_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"3w5e11264sgsf".to_vec()),
                    Some(36i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_unsigned_long_radix_arg.try_into().unwrap()),
            -1
        );

        let decoded_byte = mgr
            .execute_function(
                "ks",
                "decode_byte",
                &["text".to_string()],
                &[Some(b"-0x80".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i8::from_be_bytes(decoded_byte.try_into().unwrap()), i8::MIN);

        let decoded_short = mgr
            .execute_function(
                "ks",
                "decode_short",
                &["text".to_string()],
                &[Some(b"077777".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i16::from_be_bytes(decoded_short.try_into().unwrap()),
            i16::MAX
        );

        let decoded_int = mgr
            .execute_function(
                "ks",
                "decode_int",
                &["text".to_string()],
                &[Some(b"-0x80000000".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(decoded_int.try_into().unwrap()),
            i32::MIN
        );

        let decoded_long = mgr
            .execute_function(
                "ks",
                "decode_long",
                &["text".to_string()],
                &[Some(b"#7fffffffffffffff".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(decoded_long.try_into().unwrap()),
            i64::MAX
        );

        let parsed_int_literal = mgr
            .execute_function("ks", "parse_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_int_literal.try_into().unwrap()),
            42
        );

        let value_of_long_literal = mgr
            .execute_function("ks", "value_of_long_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(value_of_long_literal.try_into().unwrap()),
            123_456_789
        );

        let parsed_unsigned_int_literal = mgr
            .execute_function("ks", "parse_unsigned_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(parsed_unsigned_int_literal.try_into().unwrap()),
            -1
        );

        let parsed_unsigned_long_literal = mgr
            .execute_function("ks", "parse_unsigned_long_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(parsed_unsigned_long_literal.try_into().unwrap()),
            -1
        );

        let decoded_int_literal = mgr
            .execute_function("ks", "decode_int_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(decoded_int_literal.try_into().unwrap()),
            i32::MIN
        );
    }

    #[test]
    fn java_integer_parse_body_reports_invalid_input() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "parse_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Integer.parseInt(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "parse_unsigned_int".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return Integer.parseUnsignedInt(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "parse_int",
                &["text".to_string()],
                &[Some(b"not-an-int".to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Integer.parseInt"));

        let err = mgr
            .execute_function(
                "ks",
                "parse_unsigned_int",
                &["text".to_string()],
                &[Some(b"4294967296".to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Integer.parseUnsignedInt"));
    }

    #[test]
    fn java_floating_parse_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("parse_float", "float", "return Float.parseFloat(value);"),
            (
                "parse_double",
                "double",
                "return Double.parseDouble(value);",
            ),
            ("value_of_float", "float", "return Float.valueOf(value);"),
            ("value_of_double", "double", "return Double.valueOf(value);"),
            (
                "parse_float_literal",
                "float",
                "return Float.parseFloat(\"-3.5\");",
            ),
            (
                "value_of_double_literal",
                "double",
                "return Double.valueOf(\"6.25e2\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: if name.ends_with("_literal") {
                    vec![]
                } else {
                    vec!["text".to_string()]
                },
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let parsed_float = mgr
            .execute_function(
                "ks",
                "parse_float",
                &["text".to_string()],
                &[Some(b"-3.5".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(parsed_float.try_into().unwrap()), -3.5);

        let parsed_double = mgr
            .execute_function(
                "ks",
                "parse_double",
                &["text".to_string()],
                &[Some(b"6.25e2".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(parsed_double.try_into().unwrap()), 625.0);

        let value_of_double = mgr
            .execute_function(
                "ks",
                "value_of_double",
                &["text".to_string()],
                &[Some(b"-7.25".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(value_of_double.try_into().unwrap()),
            -7.25
        );

        let parsed_float_literal = mgr
            .execute_function("ks", "parse_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(parsed_float_literal.try_into().unwrap()),
            -3.5
        );

        let value_of_double_literal = mgr
            .execute_function("ks", "value_of_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(value_of_double_literal.try_into().unwrap()),
            625.0
        );
    }

    #[test]
    fn java_floating_parse_body_reports_invalid_input() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "parse_float".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "float".to_string(),
            language: "java".to_string(),
            body: "return Float.parseFloat(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let err = mgr
            .execute_function(
                "ks",
                "parse_float",
                &["text".to_string()],
                &[Some(b"not-a-float".to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Float.parseFloat"));
    }

    #[test]
    fn java_boolean_parse_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("parse_bool", "return Boolean.parseBoolean(value);"),
            ("value_of_bool", "return Boolean.valueOf(value);"),
            (
                "parse_bool_literal_true",
                "return Boolean.parseBoolean(\"TRUE\");",
            ),
            (
                "java_lang_parse_bool_literal_true",
                "return java.lang.Boolean.parseBoolean(\"TRUE\");",
            ),
            (
                "value_of_bool_literal_false",
                "return Boolean.valueOf(\"not-true\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: if name.contains("_literal_") {
                    vec![]
                } else {
                    vec!["text".to_string()]
                },
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "parse_bool",
                &["text".to_string()],
                &[Some(b"TRUE".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "parse_bool",
                &["text".to_string()],
                &[Some(b"not-true".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "value_of_bool",
                &["text".to_string()],
                &[Some(b"true".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "parse_bool_literal_true", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "java_lang_parse_bool_literal_true", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "value_of_bool_literal_false", &[], &[])
                .unwrap()
                .unwrap(),
            vec![0]
        );
    }

    #[test]
    fn java_primitive_to_string_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("byte_to_string", "tinyint", "return Byte.toString(value);"),
            (
                "byte_instance_to_string",
                "tinyint",
                "return value.toString();",
            ),
            (
                "short_to_string",
                "smallint",
                "return Short.toString(value);",
            ),
            (
                "short_instance_to_string",
                "smallint",
                "return value.toString();",
            ),
            ("int_to_string", "int", "return Integer.toString(value);"),
            ("int_instance_to_string", "int", "return value.toString();"),
            ("long_to_string", "bigint", "return Long.toString(value);"),
            (
                "long_instance_to_string",
                "bigint",
                "return value.toString();",
            ),
            ("float_to_string", "float", "return Float.toString(value);"),
            (
                "float_instance_to_string",
                "float",
                "return value.toString();",
            ),
            (
                "double_to_string",
                "double",
                "return Double.toString(value);",
            ),
            (
                "double_instance_to_string",
                "double",
                "return value.toString();",
            ),
            (
                "boolean_to_string",
                "boolean",
                "return Boolean.toString(value);",
            ),
            (
                "boolean_instance_to_string",
                "boolean",
                "return value.toString();",
            ),
            (
                "string_value_of_int",
                "int",
                "return String.valueOf(value);",
            ),
            (
                "string_value_of_float",
                "float",
                "return String.valueOf(value);",
            ),
            (
                "string_value_of_double",
                "double",
                "return String.valueOf(value);",
            ),
            (
                "string_value_of_bool",
                "boolean",
                "return String.valueOf(value);",
            ),
            (
                "string_value_of_text",
                "text",
                "return String.valueOf(value);",
            ),
            (
                "objects_to_string_int",
                "int",
                "return Objects.toString(value);",
            ),
            (
                "objects_to_string_bool",
                "boolean",
                "return java.util.Objects.toString(value);",
            ),
            (
                "objects_to_string_text",
                "text",
                "return Objects.toString(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        mgr.register_function(UdfDefinition {
            name: "int_to_string_hex_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Integer.toString(value, 16);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "long_to_string_base36_args".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string(), "int".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Long.toString(value, radix);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "int_to_string_bad_radix".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Integer.toString(value, 99);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        for (name, body) in [
            (
                "string_value_of_text_literal",
                "return String.valueOf(\"Ada\");",
            ),
            (
                "string_value_of_bool_literal",
                "return String.valueOf(true);",
            ),
            (
                "string_value_of_char_literal",
                "return String.valueOf('\\n');",
            ),
            (
                "java_lang_string_value_of_int_literal",
                "return java.lang.String.valueOf(42);",
            ),
            (
                "string_value_of_float_literal",
                "return String.valueOf(3.5f);",
            ),
            ("int_to_string_literal", "return Integer.toString(255, 16);"),
            (
                "long_to_string_literal",
                "return Long.toString(123456789, 36);",
            ),
            ("float_to_string_literal", "return Float.toString(3.5);"),
            ("double_to_string_literal", "return Double.toString(625.0);"),
            ("bool_to_string_literal", "return Boolean.toString(false);"),
            ("char_to_string_literal", "return Character.toString('A');"),
            (
                "boxed_int_to_string_literal",
                "return Integer.valueOf(42).toString();",
            ),
            (
                "boxed_byte_to_string_literal",
                "return Byte.valueOf(-12).toString();",
            ),
            (
                "boxed_double_to_string_literal",
                "return Double.valueOf(6.25).toString();",
            ),
            (
                "boxed_float_nan_to_string_literal",
                "return Float.valueOf(Float.NaN).toString();",
            ),
            (
                "boxed_boolean_to_string_literal",
                "return Boolean.valueOf(true).toString();",
            ),
            (
                "boxed_boolean_value_literal",
                "return Boolean.valueOf(false).booleanValue();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: if name == "boxed_boolean_value_literal" {
                    "boolean"
                } else {
                    "text"
                }
                .to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "byte_to_string",
                &["tinyint".to_string()],
                &[Some((-12i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-12"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "byte_instance_to_string",
                &["tinyint".to_string()],
                &[Some((-12i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-12"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "short_to_string",
                &["smallint".to_string()],
                &[Some(32_000i16.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"32000"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "short_instance_to_string",
                &["smallint".to_string()],
                &[Some(32_000i16.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"32000"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_string",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-42"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_instance_to_string",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-42"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_string_hex_literal",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-2a"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_string_bad_radix",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"42"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_string",
                &["bigint".to_string()],
                &[Some(9_000_000_000i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"9000000000"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_instance_to_string",
                &["bigint".to_string()],
                &[Some(9_000_000_000i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"9000000000"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_string_base36_args",
                &["bigint".to_string(), "int".to_string()],
                &[
                    Some(123_456_789i64.to_be_bytes().to_vec()),
                    Some(36i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            b"21i3v9"
        );
        assert_eq!(
            mgr.execute_function("ks", "string_value_of_text_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function("ks", "string_value_of_bool_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"true"
        );
        assert_eq!(
            mgr.execute_function("ks", "string_value_of_char_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"\n"
        );
        assert_eq!(
            mgr.execute_function("ks", "java_lang_string_value_of_int_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"42"
        );
        assert_eq!(
            mgr.execute_function("ks", "string_value_of_float_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"3.5"
        );
        assert_eq!(
            mgr.execute_function("ks", "int_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"ff"
        );
        assert_eq!(
            mgr.execute_function("ks", "long_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"21i3v9"
        );
        assert_eq!(
            mgr.execute_function("ks", "float_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"3.5"
        );
        assert_eq!(
            mgr.execute_function("ks", "double_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"625.0"
        );
        assert_eq!(
            mgr.execute_function("ks", "bool_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"false"
        );
        assert_eq!(
            mgr.execute_function("ks", "char_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"A"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_int_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"42"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_byte_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"-12"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_double_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"6.25"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_float_nan_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"NaN"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_boolean_to_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"true"
        );
        assert_eq!(
            mgr.execute_function("ks", "boxed_boolean_value_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_to_string",
                &["float".to_string()],
                &[Some(1.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"1.0"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_instance_to_string",
                &["float".to_string()],
                &[Some(1.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"1.0"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_to_string",
                &["double".to_string()],
                &[Some(f64::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Infinity"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_instance_to_string",
                &["double".to_string()],
                &[Some(f64::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Infinity"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "boolean_to_string",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            b"true"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "boolean_instance_to_string",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            b"true"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "string_value_of_int",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"42"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "string_value_of_float",
                &["float".to_string()],
                &[Some(f32::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"NaN"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "string_value_of_double",
                &["double".to_string()],
                &[Some((-0.0f64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-0.0"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "string_value_of_bool",
                &["boolean".to_string()],
                &[Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            b"false"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "string_value_of_text",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-db-1"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_int",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"-42"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_bool",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            b"true"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_text",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-db-1"
        );
    }

    #[test]
    fn java_primitive_compare_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("byte_compare", "tinyint", "return Byte.compare(a, b);"),
            ("short_compare", "smallint", "return Short.compare(a, b);"),
            ("int_compare", "int", "return Integer.compare(a, b);"),
            (
                "int_compare_unsigned",
                "int",
                "return Integer.compareUnsigned(a, b);",
            ),
            ("long_compare", "bigint", "return Long.compare(a, b);"),
            (
                "long_compare_unsigned",
                "bigint",
                "return Long.compareUnsigned(a, b);",
            ),
            (
                "boolean_compare",
                "boolean",
                "return Boolean.compare(a, b);",
            ),
            ("float_compare", "float", "return Float.compare(a, b);"),
            ("double_compare", "double", "return Double.compare(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, body) in [
            ("byte_compare_literal", "return Byte.compare(-12, 9);"),
            (
                "short_compare_literal",
                "return Short.compare(32000, 31999);",
            ),
            ("int_compare_literal", "return Integer.compare(42, 42);"),
            (
                "int_compare_unsigned_literal",
                "return Integer.compareUnsigned(-1, 1);",
            ),
            (
                "long_compare_literal",
                "return Long.compare(9000000000, 7);",
            ),
            (
                "long_compare_unsigned_literal",
                "return Long.compareUnsigned(-1, 1);",
            ),
            (
                "boolean_compare_literal",
                "return Boolean.compare(false, true);",
            ),
            (
                "character_compare_literal",
                "return Character.compare('A', 'B');",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let byte_compare = mgr
            .execute_function(
                "ks",
                "byte_compare",
                &["tinyint".to_string(), "tinyint".to_string()],
                &[
                    Some((-12i8).to_be_bytes().to_vec()),
                    Some(9i8.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(byte_compare.try_into().unwrap()), -1);

        let short_compare = mgr
            .execute_function(
                "ks",
                "short_compare",
                &["smallint".to_string(), "smallint".to_string()],
                &[
                    Some(32_000i16.to_be_bytes().to_vec()),
                    Some(31_999i16.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(short_compare.try_into().unwrap()), 1);

        let int_compare = mgr
            .execute_function(
                "ks",
                "int_compare",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(42i32.to_be_bytes().to_vec()),
                    Some(42i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_compare.try_into().unwrap()), 0);

        let int_compare_unsigned = mgr
            .execute_function(
                "ks",
                "int_compare_unsigned",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-1i32).to_be_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_compare_unsigned.try_into().unwrap()),
            1
        );

        let long_compare = mgr
            .execute_function(
                "ks",
                "long_compare",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(9_000_000_000i64.to_be_bytes().to_vec()),
                    Some(7i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_compare.try_into().unwrap()), 1);

        let long_compare_unsigned = mgr
            .execute_function(
                "ks",
                "long_compare_unsigned",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some((-1i64).to_be_bytes().to_vec()),
                    Some(1i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(long_compare_unsigned.try_into().unwrap()),
            1
        );

        let boolean_compare = mgr
            .execute_function(
                "ks",
                "boolean_compare",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![0]), Some(vec![1])],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(boolean_compare.try_into().unwrap()), -1);

        let float_compare_negative_zero = mgr
            .execute_function(
                "ks",
                "float_compare",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-0.0f32).to_be_bytes().to_vec()),
                    Some(0.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(float_compare_negative_zero.try_into().unwrap()),
            -1
        );

        let float_compare_nan = mgr
            .execute_function(
                "ks",
                "float_compare",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(f32::NAN.to_be_bytes().to_vec()),
                    Some(f32::INFINITY.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(float_compare_nan.try_into().unwrap()), 1);

        let double_compare = mgr
            .execute_function(
                "ks",
                "double_compare",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(1.5f64.to_be_bytes().to_vec()),
                    Some(2.5f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(double_compare.try_into().unwrap()), -1);

        let double_compare_nan = mgr
            .execute_function(
                "ks",
                "double_compare",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(f64::NAN.to_be_bytes().to_vec()),
                    Some(f64::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_compare_nan.try_into().unwrap()),
            0
        );
        for (name, expected) in [
            ("byte_compare_literal", -1),
            ("short_compare_literal", 1),
            ("int_compare_literal", 0),
            ("int_compare_unsigned_literal", 1),
            ("long_compare_literal", 1),
            ("long_compare_unsigned_literal", 1),
            ("boolean_compare_literal", -1),
            ("character_compare_literal", -1),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_boxed_primitive_compare_to_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [
            ("byte_compare_to", "tinyint"),
            ("short_compare_to", "smallint"),
            ("int_compare_to", "int"),
            ("long_compare_to", "bigint"),
            ("boolean_compare_to", "boolean"),
            ("float_compare_to", "float"),
            ("double_compare_to", "double"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: "return a.compareTo(b);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            (
                "int_compare_to_literal",
                "return Integer.valueOf(7).compareTo(Integer.valueOf(42));",
            ),
            (
                "boolean_compare_to_literal",
                "return Boolean.valueOf(true).compareTo(Boolean.valueOf(false));",
            ),
            (
                "float_compare_to_literal",
                "return Float.valueOf(-0.0f).compareTo(Float.valueOf(0.0f));",
            ),
            (
                "double_compare_to_literal",
                "return Double.valueOf(Double.NaN).compareTo(Double.valueOf(Double.NaN));",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [
            ("int_compare_to_literal", -1),
            ("boolean_compare_to_literal", 1),
            ("float_compare_to_literal", -1),
            ("double_compare_to_literal", 0),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }

        let byte_compare = mgr
            .execute_function(
                "ks",
                "byte_compare_to",
                &["tinyint".to_string(), "tinyint".to_string()],
                &[
                    Some((-12i8).to_be_bytes().to_vec()),
                    Some(9i8.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(byte_compare.try_into().unwrap()), -1);

        let short_compare = mgr
            .execute_function(
                "ks",
                "short_compare_to",
                &["smallint".to_string(), "smallint".to_string()],
                &[
                    Some(32_000i16.to_be_bytes().to_vec()),
                    Some(31_999i16.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(short_compare.try_into().unwrap()), 1);

        let int_compare = mgr
            .execute_function(
                "ks",
                "int_compare_to",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(42i32.to_be_bytes().to_vec()),
                    Some(42i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_compare.try_into().unwrap()), 0);

        let long_compare = mgr
            .execute_function(
                "ks",
                "long_compare_to",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(9_000_000_000i64.to_be_bytes().to_vec()),
                    Some(7i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_compare.try_into().unwrap()), 1);

        let boolean_compare = mgr
            .execute_function(
                "ks",
                "boolean_compare_to",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![0]), Some(vec![1])],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(boolean_compare.try_into().unwrap()), -1);

        let float_compare_negative_zero = mgr
            .execute_function(
                "ks",
                "float_compare_to",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-0.0f32).to_be_bytes().to_vec()),
                    Some(0.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(float_compare_negative_zero.try_into().unwrap()),
            -1
        );

        let double_compare_nan = mgr
            .execute_function(
                "ks",
                "double_compare_to",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(f64::NAN.to_be_bytes().to_vec()),
                    Some(f64::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_compare_nan.try_into().unwrap()),
            0
        );
    }

    #[test]
    fn java_primitive_hash_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("byte_hash", "tinyint", "return Byte.hashCode(value);"),
            ("short_hash", "smallint", "return Short.hashCode(value);"),
            ("int_hash", "int", "return Integer.hashCode(value);"),
            ("long_hash", "bigint", "return Long.hashCode(value);"),
            ("boolean_hash", "boolean", "return Boolean.hashCode(value);"),
            ("float_hash", "float", "return Float.hashCode(value);"),
            ("double_hash", "double", "return Double.hashCode(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, body) in [
            ("byte_hash_literal", "return Byte.hashCode(-12);"),
            ("short_hash_literal", "return Short.hashCode(32000);"),
            ("int_hash_literal", "return Integer.hashCode(-42);"),
            ("long_hash_literal", "return Long.hashCode(4294967298);"),
            ("boolean_hash_literal", "return Boolean.hashCode(true);"),
            (
                "character_hash_literal",
                "return Character.hashCode('\\u0041');",
            ),
            ("float_hash_literal", "return Float.hashCode(3.5);"),
            ("double_hash_literal", "return Double.hashCode(1.0);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let byte_hash = mgr
            .execute_function(
                "ks",
                "byte_hash",
                &["tinyint".to_string()],
                &[Some((-12i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(byte_hash.try_into().unwrap()), -12);

        let short_hash = mgr
            .execute_function(
                "ks",
                "short_hash",
                &["smallint".to_string()],
                &[Some(32_000i16.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(short_hash.try_into().unwrap()), 32_000);

        let int_hash = mgr
            .execute_function(
                "ks",
                "int_hash",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_hash.try_into().unwrap()), -42);

        let long_hash = mgr
            .execute_function(
                "ks",
                "long_hash",
                &["bigint".to_string()],
                &[Some(0x0000_0001_0000_0002i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_hash.try_into().unwrap()), 3);

        let boolean_hash = mgr
            .execute_function(
                "ks",
                "boolean_hash",
                &["boolean".to_string()],
                &[Some(vec![1])],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(boolean_hash.try_into().unwrap()), 1231);

        let float_hash = mgr
            .execute_function(
                "ks",
                "float_hash",
                &["float".to_string()],
                &[Some(f32::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(float_hash.try_into().unwrap()),
            0x7fc0_0000u32 as i32
        );

        let double_hash = mgr
            .execute_function(
                "ks",
                "double_hash",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(double_hash.try_into().unwrap()),
            0x3ff0_0000
        );
        for (name, expected) in [
            ("byte_hash_literal", -12),
            ("short_hash_literal", 32_000),
            ("int_hash_literal", -42),
            ("long_hash_literal", 3),
            ("boolean_hash_literal", 1231),
            ("character_hash_literal", 65),
            ("float_hash_literal", 0x4060_0000),
            ("double_hash_literal", 0x3ff0_0000),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_character_predicate_literal_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("char_is_digit", "return Character.isDigit('7');"),
            (
                "java_lang_char_is_digit",
                "return java.lang.Character.isDigit('7');",
            ),
            ("char_is_letter", "return Character.isLetter('A');"),
            (
                "char_is_letter_or_digit",
                "return Character.isLetterOrDigit('9');",
            ),
            ("char_is_upper_case", "return Character.isUpperCase('A');"),
            ("char_is_lower_case", "return Character.isLowerCase('z');"),
            (
                "char_is_whitespace",
                "return Character.isWhitespace('\\n');",
            ),
            ("char_is_space_char", "return Character.isSpaceChar(' ');"),
            (
                "char_is_iso_control",
                "return Character.isISOControl('\\n');",
            ),
            (
                "char_is_java_identifier_start",
                "return Character.isJavaIdentifierStart('_');",
            ),
            (
                "char_is_java_identifier_part",
                "return Character.isJavaIdentifierPart('9');",
            ),
            ("char_is_alphabetic", "return Character.isAlphabetic('Q');"),
            ("char_is_not_digit", "return Character.isDigit('x');"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for name in [
            "char_is_digit",
            "java_lang_char_is_digit",
            "char_is_letter",
            "char_is_letter_or_digit",
            "char_is_upper_case",
            "char_is_lower_case",
            "char_is_whitespace",
            "char_is_space_char",
            "char_is_iso_control",
            "char_is_java_identifier_start",
            "char_is_java_identifier_part",
            "char_is_alphabetic",
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                vec![1]
            );
        }
        assert_eq!(
            mgr.execute_function("ks", "char_is_not_digit", &[], &[])
                .unwrap()
                .unwrap(),
            vec![0]
        );
    }

    #[test]
    fn java_character_numeric_literal_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("char_digit_hex", "return Character.digit('f', 16);"),
            (
                "java_lang_char_digit_hex",
                "return java.lang.Character.digit('f', 16);",
            ),
            ("char_digit_invalid", "return Character.digit('g', 16);"),
            (
                "char_numeric_value",
                "return Character.getNumericValue('Z');",
            ),
            ("char_to_upper_case", "return Character.toUpperCase('a');"),
            ("char_to_lower_case", "return Character.toLowerCase('A');"),
            ("char_to_title_case", "return Character.toTitleCase('q');"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [
            ("char_digit_hex", 15),
            ("java_lang_char_digit_hex", 15),
            ("char_digit_invalid", -1),
            ("char_numeric_value", 35),
            ("char_to_upper_case", 65),
            ("char_to_lower_case", 97),
            ("char_to_title_case", 81),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_boxed_primitive_hash_code_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        let cases = [
            (
                "byte_instance_hash",
                "tinyint",
                "return value.hashCode();",
                (-12i8).to_be_bytes().to_vec(),
                -12,
            ),
            (
                "short_instance_hash",
                "smallint",
                "return value.hashCode();",
                32_000i16.to_be_bytes().to_vec(),
                32_000,
            ),
            (
                "int_instance_hash",
                "int",
                "return value.hashCode();",
                (-42i32).to_be_bytes().to_vec(),
                -42,
            ),
            (
                "int_objects_hash",
                "int",
                "return Objects.hashCode(value);",
                42i32.to_be_bytes().to_vec(),
                42,
            ),
            (
                "long_instance_hash",
                "bigint",
                "return value.hashCode();",
                0x0000_0001_0000_0002i64.to_be_bytes().to_vec(),
                3,
            ),
            (
                "boolean_instance_hash",
                "boolean",
                "return value.hashCode();",
                vec![1],
                1231,
            ),
            (
                "float_instance_hash",
                "float",
                "return value.hashCode();",
                f32::NAN.to_be_bytes().to_vec(),
                0x7fc0_0000u32 as i32,
            ),
            (
                "double_instance_hash",
                "double",
                "return value.hashCode();",
                1.0f64.to_be_bytes().to_vec(),
                0x3ff0_0000,
            ),
            (
                "double_objects_hash",
                "double",
                "return java.util.Objects.hashCode(value);",
                1.0f64.to_be_bytes().to_vec(),
                0x3ff0_0000,
            ),
            (
                "text_instance_hash",
                "text",
                "return value.hashCode();",
                b"Ada".to_vec(),
                65_662,
            ),
            (
                "text_objects_hash",
                "text",
                "return Objects.hashCode(value);",
                b"Ada".to_vec(),
                65_662,
            ),
        ];

        for (name, arg_type, body, _, _) in &cases {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            (
                "boxed_int_hash_literal",
                "return Integer.valueOf(42).hashCode();",
            ),
            (
                "boxed_double_hash_literal",
                "return Double.valueOf(1.0).hashCode();",
            ),
            (
                "objects_text_hash_literal",
                "return Objects.hashCode(\"Ada\");",
            ),
            (
                "objects_boolean_hash_literal",
                "return java.util.Objects.hashCode(true);",
            ),
            (
                "objects_java_lang_int_hash_literal",
                "return Objects.hashCode(java.lang.Integer.MAX_VALUE);",
            ),
            (
                "objects_null_hash_literal",
                "return Objects.hashCode(null);",
            ),
            (
                "objects_hash_literals",
                "return Objects.hash(\"Ada\", 42, null);",
            ),
            (
                "objects_hash_java_lang_literals",
                "return Objects.hash(java.lang.Boolean.TRUE, java.lang.Float.NaN, java.lang.Integer.MAX_VALUE);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, arg_type, _, value, expected) in cases {
            let result = mgr
                .execute_function("ks", name, &[arg_type.to_string()], &[Some(value)])
                .unwrap()
                .unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }

        for (name, expected) in [
            ("boxed_int_hash_literal", 42),
            ("boxed_double_hash_literal", 0x3ff0_0000),
            ("objects_text_hash_literal", 65_662),
            ("objects_boolean_hash_literal", 1231),
            ("objects_java_lang_int_hash_literal", i32::MAX),
            ("objects_null_hash_literal", 0),
            ("objects_hash_literals", 63_132_275),
            ("objects_hash_java_lang_literals", -128_810_643),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_integer_static_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            ("int_sum", "int", "int", "return Integer.sum(a, b);"),
            ("int_max", "int", "int", "return Integer.max(a, b);"),
            ("int_min", "int", "int", "return Integer.min(a, b);"),
            (
                "int_divide_unsigned",
                "int",
                "int",
                "return Integer.divideUnsigned(a, b);",
            ),
            (
                "int_remainder_unsigned",
                "int",
                "int",
                "return Integer.remainderUnsigned(a, b);",
            ),
            ("long_sum", "bigint", "bigint", "return Long.sum(a, b);"),
            ("long_max", "bigint", "bigint", "return Long.max(a, b);"),
            ("long_min", "bigint", "bigint", "return Long.min(a, b);"),
            (
                "long_divide_unsigned",
                "bigint",
                "bigint",
                "return Long.divideUnsigned(a, b);",
            ),
            (
                "long_remainder_unsigned",
                "bigint",
                "bigint",
                "return Long.remainderUnsigned(a, b);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, arg_type, body) in [
            ("int_signum", "int", "return Integer.signum(value);"),
            ("long_signum", "bigint", "return Long.signum(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "int_sum_literal",
                "int",
                "return Integer.sum(2147483647, 1);",
            ),
            ("int_max_literal", "int", "return Integer.max(-7, 42);"),
            ("int_min_literal", "int", "return Integer.min(-7, 42);"),
            (
                "int_divide_unsigned_literal",
                "int",
                "return Integer.divideUnsigned(-1, 2);",
            ),
            (
                "int_remainder_unsigned_literal",
                "int",
                "return Integer.remainderUnsigned(-1, 2);",
            ),
            ("int_signum_literal", "int", "return Integer.signum(-42);"),
            (
                "long_sum_literal",
                "bigint",
                "return Long.sum(9223372036854775807, 1);",
            ),
            (
                "long_max_literal",
                "bigint",
                "return Long.max(-9000000000, 7);",
            ),
            (
                "long_min_literal",
                "bigint",
                "return Long.min(-9000000000, 7);",
            ),
            (
                "long_divide_unsigned_literal",
                "bigint",
                "return Long.divideUnsigned(-1, 2);",
            ),
            (
                "long_remainder_unsigned_literal",
                "bigint",
                "return Long.remainderUnsigned(-1, 2);",
            ),
            (
                "long_signum_literal",
                "int",
                "return Long.signum(9000000000);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let int_sum = mgr
            .execute_function(
                "ks",
                "int_sum",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(i32::MAX.to_be_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_sum.try_into().unwrap()), i32::MIN);

        let int_max = mgr
            .execute_function(
                "ks",
                "int_max",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-7i32).to_be_bytes().to_vec()),
                    Some(42i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_max.try_into().unwrap()), 42);

        let int_min = mgr
            .execute_function(
                "ks",
                "int_min",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-7i32).to_be_bytes().to_vec()),
                    Some(42i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_min.try_into().unwrap()), -7);

        let int_divide_unsigned = mgr
            .execute_function(
                "ks",
                "int_divide_unsigned",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-1i32).to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_divide_unsigned.try_into().unwrap()),
            i32::MAX
        );

        let int_remainder_unsigned = mgr
            .execute_function(
                "ks",
                "int_remainder_unsigned",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-1i32).to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_remainder_unsigned.try_into().unwrap()),
            1
        );

        let int_signum = mgr
            .execute_function(
                "ks",
                "int_signum",
                &["int".to_string()],
                &[Some((-42i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_signum.try_into().unwrap()), -1);

        let long_sum = mgr
            .execute_function(
                "ks",
                "long_sum",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(i64::MAX.to_be_bytes().to_vec()),
                    Some(1i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(long_sum.try_into().unwrap()), i64::MIN);

        let long_max = mgr
            .execute_function(
                "ks",
                "long_max",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some((-9_000_000_000i64).to_be_bytes().to_vec()),
                    Some(7i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(long_max.try_into().unwrap()), 7);

        let long_min = mgr
            .execute_function(
                "ks",
                "long_min",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some((-9_000_000_000i64).to_be_bytes().to_vec()),
                    Some(7i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(long_min.try_into().unwrap()),
            -9_000_000_000
        );

        let long_divide_unsigned = mgr
            .execute_function(
                "ks",
                "long_divide_unsigned",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some((-1i64).to_be_bytes().to_vec()),
                    Some(2i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(long_divide_unsigned.try_into().unwrap()),
            i64::MAX
        );

        let long_remainder_unsigned = mgr
            .execute_function(
                "ks",
                "long_remainder_unsigned",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some((-1i64).to_be_bytes().to_vec()),
                    Some(2i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(long_remainder_unsigned.try_into().unwrap()),
            1
        );

        let long_signum = mgr
            .execute_function(
                "ks",
                "long_signum",
                &["bigint".to_string()],
                &[Some(9_000_000_000i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_signum.try_into().unwrap()), 1);

        let err = mgr
            .execute_function(
                "ks",
                "int_divide_unsigned",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(1i32.to_be_bytes().to_vec()),
                    Some(0i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap_err();
        assert!(err.contains("Integer.divideUnsigned"));

        let err = mgr
            .execute_function(
                "ks",
                "long_remainder_unsigned",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(1i64.to_be_bytes().to_vec()),
                    Some(0i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap_err();
        assert!(err.contains("Long.remainderUnsigned"));
        for (name, expected) in [
            ("int_sum_literal", i32::MIN),
            ("int_max_literal", 42),
            ("int_min_literal", -7),
            ("int_divide_unsigned_literal", i32::MAX),
            ("int_remainder_unsigned_literal", 1),
            ("int_signum_literal", -1),
            ("long_signum_literal", 1),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        for (name, expected) in [
            ("long_sum_literal", i64::MIN),
            ("long_max_literal", 7),
            ("long_min_literal", -9_000_000_000),
            ("long_divide_unsigned_literal", i64::MAX),
            ("long_remainder_unsigned_literal", 1),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_unsigned_conversion_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            (
                "byte_to_unsigned_int",
                "tinyint",
                "int",
                "return Byte.toUnsignedInt(value);",
            ),
            (
                "byte_to_unsigned_long",
                "tinyint",
                "bigint",
                "return Byte.toUnsignedLong(value);",
            ),
            (
                "short_to_unsigned_int",
                "smallint",
                "int",
                "return Short.toUnsignedInt(value);",
            ),
            (
                "short_to_unsigned_long",
                "smallint",
                "bigint",
                "return Short.toUnsignedLong(value);",
            ),
            (
                "int_to_unsigned_long",
                "int",
                "bigint",
                "return Integer.toUnsignedLong(value);",
            ),
            (
                "int_to_unsigned_string",
                "int",
                "text",
                "return Integer.toUnsignedString(value);",
            ),
            (
                "long_to_unsigned_string",
                "bigint",
                "text",
                "return Long.toUnsignedString(value);",
            ),
            (
                "int_to_unsigned_hex_literal",
                "int",
                "text",
                "return Integer.toUnsignedString(value, 16);",
            ),
            (
                "int_to_binary_string",
                "int",
                "text",
                "return Integer.toBinaryString(value);",
            ),
            (
                "long_to_binary_string",
                "bigint",
                "text",
                "return Long.toBinaryString(value);",
            ),
            (
                "int_to_octal_string",
                "int",
                "text",
                "return Integer.toOctalString(value);",
            ),
            (
                "long_to_octal_string",
                "bigint",
                "text",
                "return Long.toOctalString(value);",
            ),
            (
                "int_to_hex_string",
                "int",
                "text",
                "return Integer.toHexString(value);",
            ),
            (
                "long_to_hex_string",
                "bigint",
                "text",
                "return Long.toHexString(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        mgr.register_function(UdfDefinition {
            name: "long_to_unsigned_base36_args".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["bigint".to_string(), "int".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Long.toUnsignedString(value, radix);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        for (name, return_type, body) in [
            (
                "int_to_unsigned_long_literal",
                "bigint",
                "return Integer.toUnsignedLong(-1);",
            ),
            (
                "int_to_unsigned_string_literal",
                "text",
                "return Integer.toUnsignedString(-1);",
            ),
            (
                "int_to_unsigned_hex_literal_value",
                "text",
                "return Integer.toUnsignedString(-1, 16);",
            ),
            (
                "long_to_unsigned_string_literal",
                "text",
                "return Long.toUnsignedString(-1);",
            ),
            (
                "long_to_unsigned_base36_literal",
                "text",
                "return Long.toUnsignedString(-1, 36);",
            ),
            (
                "int_to_hex_string_literal",
                "text",
                "return Integer.toHexString(-1);",
            ),
            (
                "long_to_binary_string_literal",
                "text",
                "return Long.toBinaryString(-1);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let unsigned_byte_int = mgr
            .execute_function(
                "ks",
                "byte_to_unsigned_int",
                &["tinyint".to_string()],
                &[Some((-1i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(unsigned_byte_int.try_into().unwrap()),
            255
        );

        let unsigned_byte_long = mgr
            .execute_function(
                "ks",
                "byte_to_unsigned_long",
                &["tinyint".to_string()],
                &[Some((-1i8).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(unsigned_byte_long.try_into().unwrap()),
            255
        );

        let unsigned_short_int = mgr
            .execute_function(
                "ks",
                "short_to_unsigned_int",
                &["smallint".to_string()],
                &[Some((-1i16).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(unsigned_short_int.try_into().unwrap()),
            65_535
        );

        let unsigned_short_long = mgr
            .execute_function(
                "ks",
                "short_to_unsigned_long",
                &["smallint".to_string()],
                &[Some((-1i16).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(unsigned_short_long.try_into().unwrap()),
            65_535
        );

        let unsigned_long = mgr
            .execute_function(
                "ks",
                "int_to_unsigned_long",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(unsigned_long.try_into().unwrap()),
            4_294_967_295
        );

        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_unsigned_string",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"4294967295"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_unsigned_string",
                &["bigint".to_string()],
                &[Some((-1i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"18446744073709551615"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_unsigned_hex_literal",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ffffffff"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_unsigned_base36_args",
                &["bigint".to_string(), "int".to_string()],
                &[
                    Some((-1i64).to_be_bytes().to_vec()),
                    Some(36i32.to_be_bytes().to_vec())
                ],
            )
            .unwrap()
            .unwrap(),
            b"3w5e11264sgsf"
        );

        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_binary_string",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"11111111111111111111111111111111"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_binary_string",
                &["bigint".to_string()],
                &[Some((-1i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_octal_string",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"37777777777"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_octal_string",
                &["bigint".to_string()],
                &[Some((-1i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"1777777777777777777777"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_to_hex_string",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ffffffff"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_to_hex_string",
                &["bigint".to_string()],
                &[Some((-1i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ffffffffffffffff"
        );
        let unsigned_long_literal = mgr
            .execute_function("ks", "int_to_unsigned_long_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(unsigned_long_literal.try_into().unwrap()),
            4_294_967_295
        );
        assert_eq!(
            mgr.execute_function("ks", "int_to_unsigned_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"4294967295"
        );
        assert_eq!(
            mgr.execute_function("ks", "int_to_unsigned_hex_literal_value", &[], &[])
                .unwrap()
                .unwrap(),
            b"ffffffff"
        );
        assert_eq!(
            mgr.execute_function("ks", "long_to_unsigned_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"18446744073709551615"
        );
        assert_eq!(
            mgr.execute_function("ks", "long_to_unsigned_base36_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"3w5e11264sgsf"
        );
        assert_eq!(
            mgr.execute_function("ks", "int_to_hex_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"ffffffff"
        );
        assert_eq!(
            mgr.execute_function("ks", "long_to_binary_string_literal", &[], &[])
                .unwrap()
                .unwrap(),
            b"1111111111111111111111111111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn java_bit_count_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            (
                "int_bit_count",
                "int",
                "int",
                "return Integer.bitCount(value);",
            ),
            (
                "int_leading_zeros",
                "int",
                "int",
                "return Integer.numberOfLeadingZeros(value);",
            ),
            (
                "int_trailing_zeros",
                "int",
                "int",
                "return Integer.numberOfTrailingZeros(value);",
            ),
            (
                "int_highest_one_bit",
                "int",
                "int",
                "return Integer.highestOneBit(value);",
            ),
            (
                "int_lowest_one_bit",
                "int",
                "int",
                "return Integer.lowestOneBit(value);",
            ),
            (
                "int_reverse",
                "int",
                "int",
                "return Integer.reverse(value);",
            ),
            (
                "int_reverse_bytes",
                "int",
                "int",
                "return Integer.reverseBytes(value);",
            ),
            (
                "long_bit_count",
                "bigint",
                "int",
                "return Long.bitCount(value);",
            ),
            (
                "long_leading_zeros",
                "bigint",
                "int",
                "return Long.numberOfLeadingZeros(value);",
            ),
            (
                "long_trailing_zeros",
                "bigint",
                "int",
                "return Long.numberOfTrailingZeros(value);",
            ),
            (
                "long_highest_one_bit",
                "bigint",
                "bigint",
                "return Long.highestOneBit(value);",
            ),
            (
                "long_lowest_one_bit",
                "bigint",
                "bigint",
                "return Long.lowestOneBit(value);",
            ),
            (
                "long_reverse",
                "bigint",
                "bigint",
                "return Long.reverse(value);",
            ),
            (
                "long_reverse_bytes",
                "bigint",
                "bigint",
                "return Long.reverseBytes(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "int_bit_count_literal",
                "int",
                "return Integer.bitCount(-1);",
            ),
            (
                "int_leading_zeros_literal",
                "int",
                "return Integer.numberOfLeadingZeros(240);",
            ),
            (
                "int_trailing_zeros_literal",
                "int",
                "return Integer.numberOfTrailingZeros(240);",
            ),
            (
                "int_highest_one_bit_literal",
                "int",
                "return Integer.highestOneBit(240);",
            ),
            (
                "int_reverse_bytes_literal",
                "int",
                "return Integer.reverseBytes(305419896);",
            ),
            ("long_bit_count_literal", "int", "return Long.bitCount(-1);"),
            (
                "long_highest_one_bit_literal",
                "bigint",
                "return Long.highestOneBit(240);",
            ),
            (
                "long_reverse_bytes_literal",
                "bigint",
                "return Long.reverseBytes(81985529216486895);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let int_count = mgr
            .execute_function(
                "ks",
                "int_bit_count",
                &["int".to_string()],
                &[Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(int_count.try_into().unwrap()), 32);

        let int_leading_zeros = mgr
            .execute_function(
                "ks",
                "int_leading_zeros",
                &["int".to_string()],
                &[Some(0x0000_00f0i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_leading_zeros.try_into().unwrap()),
            24
        );

        let int_trailing_zeros = mgr
            .execute_function(
                "ks",
                "int_trailing_zeros",
                &["int".to_string()],
                &[Some(0x0000_00f0i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_trailing_zeros.try_into().unwrap()),
            4
        );

        let int_highest_one_bit = mgr
            .execute_function(
                "ks",
                "int_highest_one_bit",
                &["int".to_string()],
                &[Some(0x0000_00f0i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_highest_one_bit.try_into().unwrap()),
            128
        );

        let int_lowest_one_bit = mgr
            .execute_function(
                "ks",
                "int_lowest_one_bit",
                &["int".to_string()],
                &[Some(0x0000_00f0i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(int_lowest_one_bit.try_into().unwrap()),
            16
        );

        let int_reverse = mgr
            .execute_function(
                "ks",
                "int_reverse",
                &["int".to_string()],
                &[Some(0x0000_00f0i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(int_reverse.try_into().unwrap()),
            0x0f00_0000
        );

        let int_reverse_bytes = mgr
            .execute_function(
                "ks",
                "int_reverse_bytes",
                &["int".to_string()],
                &[Some(0x1234_5678i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(int_reverse_bytes.try_into().unwrap()),
            0x7856_3412
        );

        let long_count = mgr
            .execute_function(
                "ks",
                "long_bit_count",
                &["bigint".to_string()],
                &[Some((-1i64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(long_count.try_into().unwrap()), 64);

        let long_leading_zeros = mgr
            .execute_function(
                "ks",
                "long_leading_zeros",
                &["bigint".to_string()],
                &[Some(0x0000_0000_0000_00f0i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(long_leading_zeros.try_into().unwrap()),
            56
        );

        let long_trailing_zeros = mgr
            .execute_function(
                "ks",
                "long_trailing_zeros",
                &["bigint".to_string()],
                &[Some(0x0000_0000_0000_00f0i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(long_trailing_zeros.try_into().unwrap()),
            4
        );

        let long_highest_one_bit = mgr
            .execute_function(
                "ks",
                "long_highest_one_bit",
                &["bigint".to_string()],
                &[Some(0x0000_0000_0000_00f0i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(long_highest_one_bit.try_into().unwrap()),
            128
        );

        let long_lowest_one_bit = mgr
            .execute_function(
                "ks",
                "long_lowest_one_bit",
                &["bigint".to_string()],
                &[Some(0x0000_0000_0000_00f0i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(long_lowest_one_bit.try_into().unwrap()),
            16
        );

        let long_reverse = mgr
            .execute_function(
                "ks",
                "long_reverse",
                &["bigint".to_string()],
                &[Some(0x0000_0000_0000_00f0i64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(long_reverse.try_into().unwrap()),
            0x0f00_0000_0000_0000
        );

        let long_reverse_bytes = mgr
            .execute_function(
                "ks",
                "long_reverse_bytes",
                &["bigint".to_string()],
                &[Some(
                    (0x0123_4567_89ab_cdefu64 as i64).to_be_bytes().to_vec(),
                )],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(long_reverse_bytes.try_into().unwrap()),
            0xefcd_ab89_6745_2301
        );
        for (name, expected) in [
            ("int_bit_count_literal", 32),
            ("int_leading_zeros_literal", 24),
            ("int_trailing_zeros_literal", 4),
            ("int_highest_one_bit_literal", 128),
            ("int_reverse_bytes_literal", 0x7856_3412u32 as i32),
            ("long_bit_count_literal", 64),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        for (name, expected) in [
            ("long_highest_one_bit_literal", 128i64),
            (
                "long_reverse_bytes_literal",
                0xefcd_ab89_6745_2301u64 as i64,
            ),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_bit_rotate_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_types, return_type, body) in [
            (
                "int_rotate_left",
                vec!["int", "int"],
                "int",
                "return Integer.rotateLeft(value, distance);",
            ),
            (
                "int_rotate_right",
                vec!["int", "int"],
                "int",
                "return Integer.rotateRight(value, distance);",
            ),
            (
                "long_rotate_left",
                vec!["bigint", "int"],
                "bigint",
                "return Long.rotateLeft(value, distance);",
            ),
            (
                "long_rotate_right",
                vec!["bigint", "int"],
                "bigint",
                "return Long.rotateRight(value, distance);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: arg_types.into_iter().map(str::to_string).collect(),
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "int_rotate_left_literal",
                "int",
                "return Integer.rotateLeft(305419896, 40);",
            ),
            (
                "int_rotate_right_literal",
                "int",
                "return Integer.rotateRight(305419896, 8);",
            ),
            (
                "long_rotate_left_literal",
                "bigint",
                "return Long.rotateLeft(81985529216486895, 72);",
            ),
            (
                "long_rotate_right_literal",
                "bigint",
                "return Long.rotateRight(81985529216486895, 8);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let int_left = mgr
            .execute_function(
                "ks",
                "int_rotate_left",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(0x1234_5678i32.to_be_bytes().to_vec()),
                    Some(40i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(int_left.try_into().unwrap()),
            0x3456_7812
        );

        let int_right = mgr
            .execute_function(
                "ks",
                "int_rotate_right",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(0x1234_5678i32.to_be_bytes().to_vec()),
                    Some(8i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(int_right.try_into().unwrap()),
            0x7812_3456
        );

        let long_left = mgr
            .execute_function(
                "ks",
                "long_rotate_left",
                &["bigint".to_string(), "int".to_string()],
                &[
                    Some((0x0123_4567_89ab_cdefu64 as i64).to_be_bytes().to_vec()),
                    Some(72i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(long_left.try_into().unwrap()),
            0x2345_6789_abcd_ef01
        );

        let long_right = mgr
            .execute_function(
                "ks",
                "long_rotate_right",
                &["bigint".to_string(), "int".to_string()],
                &[
                    Some((0x0123_4567_89ab_cdefu64 as i64).to_be_bytes().to_vec()),
                    Some(8i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(long_right.try_into().unwrap()),
            0xef01_2345_6789_abcd
        );
        for (name, expected) in [
            ("int_rotate_left_literal", 0x3456_7812u32 as i32),
            ("int_rotate_right_literal", 0x7812_3456u32 as i32),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        for (name, expected) in [
            ("long_rotate_left_literal", 0x2345_6789_abcd_ef01u64 as i64),
            ("long_rotate_right_literal", 0xef01_2345_6789_abcdu64 as i64),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), expected);
        }
    }

    #[test]
    fn java_float_bit_conversion_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            (
                "float_to_int_bits",
                "float",
                "int",
                "return Float.floatToIntBits(value);",
            ),
            (
                "float_to_raw_int_bits",
                "float",
                "int",
                "return Float.floatToRawIntBits(value);",
            ),
            (
                "int_bits_to_float",
                "int",
                "float",
                "return Float.intBitsToFloat(value);",
            ),
            (
                "double_to_long_bits",
                "double",
                "bigint",
                "return Double.doubleToLongBits(value);",
            ),
            (
                "double_to_raw_long_bits",
                "double",
                "bigint",
                "return Double.doubleToRawLongBits(value);",
            ),
            (
                "long_bits_to_double",
                "bigint",
                "double",
                "return Double.longBitsToDouble(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "float_to_int_bits_literal",
                "int",
                "return Float.floatToIntBits(3.5);",
            ),
            (
                "float_to_raw_int_bits_literal",
                "int",
                "return Float.floatToRawIntBits(3.5);",
            ),
            (
                "int_bits_to_float_literal",
                "float",
                "return Float.intBitsToFloat(1080033280);",
            ),
            (
                "double_to_long_bits_literal",
                "bigint",
                "return Double.doubleToLongBits(1.0);",
            ),
            (
                "double_to_raw_long_bits_literal",
                "bigint",
                "return Double.doubleToRawLongBits(1.0);",
            ),
            (
                "long_bits_to_double_literal",
                "double",
                "return Double.longBitsToDouble(4607182418800017408);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let raw_float_nan = f32::from_bits(0x7fa0_0001);
        let float_to_int_bits = mgr
            .execute_function(
                "ks",
                "float_to_int_bits",
                &["float".to_string()],
                &[Some(raw_float_nan.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(float_to_int_bits.try_into().unwrap()),
            0x7fc0_0000
        );

        let float_to_raw_int_bits = mgr
            .execute_function(
                "ks",
                "float_to_raw_int_bits",
                &["float".to_string()],
                &[Some(raw_float_nan.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(float_to_raw_int_bits.try_into().unwrap()),
            0x7fa0_0001
        );

        let int_bits_to_float = mgr
            .execute_function(
                "ks",
                "int_bits_to_float",
                &["int".to_string()],
                &[Some((0x7fa0_0001u32 as i32).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(int_bits_to_float.try_into().unwrap()),
            0x7fa0_0001
        );

        let raw_double_nan = f64::from_bits(0x7ff0_0000_0000_0001);
        let double_to_long_bits = mgr
            .execute_function(
                "ks",
                "double_to_long_bits",
                &["double".to_string()],
                &[Some(raw_double_nan.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(double_to_long_bits.try_into().unwrap()),
            0x7ff8_0000_0000_0000
        );

        let double_to_raw_long_bits = mgr
            .execute_function(
                "ks",
                "double_to_raw_long_bits",
                &["double".to_string()],
                &[Some(raw_double_nan.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(double_to_raw_long_bits.try_into().unwrap()),
            0x7ff0_0000_0000_0001
        );

        let long_bits_to_double = mgr
            .execute_function(
                "ks",
                "long_bits_to_double",
                &["bigint".to_string()],
                &[Some(
                    (0x7ff0_0000_0000_0001u64 as i64).to_be_bytes().to_vec(),
                )],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(long_bits_to_double.try_into().unwrap()),
            0x7ff0_0000_0000_0001
        );
        for (name, expected) in [
            ("float_to_int_bits_literal", 0x4060_0000u32 as i32),
            ("float_to_raw_int_bits_literal", 0x4060_0000u32 as i32),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        let int_bits_to_float_literal = mgr
            .execute_function("ks", "int_bits_to_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(int_bits_to_float_literal.try_into().unwrap()),
            3.5
        );
        for (name, expected) in [
            (
                "double_to_long_bits_literal",
                0x3ff0_0000_0000_0000u64 as i64,
            ),
            (
                "double_to_raw_long_bits_literal",
                0x3ff0_0000_0000_0000u64 as i64,
            ),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), expected);
        }
        let long_bits_to_double_literal = mgr
            .execute_function("ks", "long_bits_to_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(long_bits_to_double_literal.try_into().unwrap()),
            1.0
        );
    }

    #[test]
    fn java_float_constant_bodies_map_to_native_constants() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("float_nan", "float", "return Float.NaN;"),
            (
                "float_positive_infinity",
                "float",
                "return Float.POSITIVE_INFINITY;",
            ),
            (
                "float_negative_infinity",
                "float",
                "return Float.NEGATIVE_INFINITY;",
            ),
            ("float_max_value", "float", "return Float.MAX_VALUE;"),
            ("float_min_value", "float", "return Float.MIN_VALUE;"),
            ("float_min_normal", "float", "return Float.MIN_NORMAL;"),
            ("double_nan", "double", "return Double.NaN;"),
            (
                "double_positive_infinity",
                "double",
                "return Double.POSITIVE_INFINITY;",
            ),
            (
                "double_negative_infinity",
                "double",
                "return Double.NEGATIVE_INFINITY;",
            ),
            ("double_max_value", "double", "return Double.MAX_VALUE;"),
            ("double_min_value", "double", "return Double.MIN_VALUE;"),
            ("double_min_normal", "double", "return Double.MIN_NORMAL;"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected_bits) in [
            ("float_nan", 0x7fc0_0000),
            ("float_positive_infinity", 0x7f80_0000),
            ("float_negative_infinity", 0xff80_0000),
            ("float_max_value", 0x7f7f_ffff),
            ("float_min_value", 0x0000_0001),
            ("float_min_normal", 0x0080_0000),
        ] {
            let value = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(u32::from_be_bytes(value.try_into().unwrap()), expected_bits);
        }

        for (name, expected_bits) in [
            ("double_nan", 0x7ff8_0000_0000_0000),
            ("double_positive_infinity", 0x7ff0_0000_0000_0000),
            ("double_negative_infinity", 0xfff0_0000_0000_0000),
            ("double_max_value", 0x7fef_ffff_ffff_ffff),
            ("double_min_value", 0x0000_0000_0000_0001),
            ("double_min_normal", 0x0010_0000_0000_0000),
        ] {
            let value = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(u64::from_be_bytes(value.try_into().unwrap()), expected_bits);
        }
    }

    #[test]
    fn java_float_and_double_predicate_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("float_nan", "float", "return Float.isNaN(value);"),
            ("double_nan", "double", "return Double.isNaN(value);"),
            ("float_infinite", "float", "return Float.isInfinite(value);"),
            (
                "double_infinite",
                "double",
                "return Double.isInfinite(value);",
            ),
            ("float_finite", "float", "return Float.isFinite(value);"),
            ("double_finite", "double", "return Double.isFinite(value);"),
            ("float_instance_nan", "float", "return value.isNaN();"),
            ("double_instance_nan", "double", "return value.isNaN();"),
            (
                "float_instance_infinite",
                "float",
                "return value.isInfinite();",
            ),
            (
                "double_instance_infinite",
                "double",
                "return value.isInfinite();",
            ),
            ("float_instance_finite", "float", "return value.isFinite();"),
            (
                "double_instance_finite",
                "double",
                "return value.isFinite();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("float_nan_literal", "return Float.isNaN(Float.NaN);"),
            (
                "java_lang_float_nan_literal",
                "return java.lang.Float.isNaN(java.lang.Float.NaN);",
            ),
            (
                "double_infinite_literal",
                "return Double.isInfinite(Double.NEGATIVE_INFINITY);",
            ),
            ("float_finite_literal", "return Float.isFinite(12.5f);"),
            ("double_nan_false_literal", "return Double.isNaN(1.0);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for name in [
            "float_nan_literal",
            "java_lang_float_nan_literal",
            "double_infinite_literal",
            "float_finite_literal",
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                vec![1]
            );
        }
        assert_eq!(
            mgr.execute_function("ks", "double_nan_false_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![0]
        );

        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_nan",
                &["float".to_string()],
                &[Some(f32::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_nan",
                &["double".to_string()],
                &[Some(f64::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_infinite",
                &["float".to_string()],
                &[Some(f32::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_infinite",
                &["double".to_string()],
                &[Some(f64::NEG_INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_finite",
                &["float".to_string()],
                &[Some(12.5f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_finite",
                &["double".to_string()],
                &[Some((-9.25f64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_instance_nan",
                &["float".to_string()],
                &[Some(f32::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_instance_nan",
                &["double".to_string()],
                &[Some(f64::NAN.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_instance_infinite",
                &["float".to_string()],
                &[Some(f32::NEG_INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_instance_infinite",
                &["double".to_string()],
                &[Some(f64::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_instance_finite",
                &["float".to_string()],
                &[Some(12.5f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_instance_finite",
                &["double".to_string()],
                &[Some((-9.25f64).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_math_min_max_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("max_int", "int", "return Math.max(a, b);"),
            ("min_bigint", "bigint", "return Math.min(a, b);"),
            ("max_float", "float", "return Math.max(a, b);"),
            ("min_double", "double", "return Math.min(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, return_type, body) in [
            ("max_int_literal", "int", "return Math.max(-42, 7);"),
            ("min_int_literal", "int", "return Math.min(9001, -12);"),
            (
                "max_float_literal",
                "float",
                "return Math.max(-3.5f, 2.25f);",
            ),
            (
                "min_double_literal",
                "double",
                "return Math.min(9.25, -1.5);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, expected) in [("max_int_literal", 7), ("min_int_literal", -12)] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }

        let max_float_literal = mgr
            .execute_function("ks", "max_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(max_float_literal.try_into().unwrap()),
            2.25
        );

        let min_double_literal = mgr
            .execute_function("ks", "min_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(min_double_literal.try_into().unwrap()),
            -1.5
        );

        let max_int = mgr
            .execute_function(
                "ks",
                "max_int",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-42i32).to_be_bytes().to_vec()),
                    Some(7i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(max_int.try_into().unwrap()), 7);

        let min_bigint = mgr
            .execute_function(
                "ks",
                "min_bigint",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(9001i64.to_be_bytes().to_vec()),
                    Some((-12i64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(min_bigint.try_into().unwrap()), -12);

        let max_float = mgr
            .execute_function(
                "ks",
                "max_float",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-3.5f32).to_be_bytes().to_vec()),
                    Some(2.25f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(max_float.try_into().unwrap()), 2.25);

        let min_double = mgr
            .execute_function(
                "ks",
                "min_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(9.25f64.to_be_bytes().to_vec()),
                    Some((-1.5f64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(min_double.try_into().unwrap()), -1.5);
    }

    #[test]
    fn java_math_copysign_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [("copysign_float", "float"), ("copysign_double", "double")] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: "return Math.copySign(magnitude, sign);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, return_type, body) in [
            (
                "copysign_float_literal",
                "float",
                "return Math.copySign(3.5f, -1.0f);",
            ),
            (
                "copysign_double_literal",
                "double",
                "return Math.copySign(-9.25, 1.0);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_literal = mgr
            .execute_function("ks", "copysign_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_literal.try_into().unwrap()), -3.5);

        let double_literal = mgr
            .execute_function("ks", "copysign_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_literal.try_into().unwrap()), 9.25);

        let float_result = mgr
            .execute_function(
                "ks",
                "copysign_float",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(3.5f32.to_be_bytes().to_vec()),
                    Some((-1.0f32).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_result.try_into().unwrap()), -3.5);

        let double_result = mgr
            .execute_function(
                "ks",
                "copysign_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some((-9.25f64).to_be_bytes().to_vec()),
                    Some(1.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_result.try_into().unwrap()), 9.25);
    }

    #[test]
    fn java_math_next_after_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type) in [
            ("next_after_float", "float", "float"),
            ("next_after_double", "double", "double"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), "double".to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: "return Math.nextAfter(start, direction);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, return_type, body) in [
            (
                "next_after_float_literal",
                "float",
                "return Math.nextAfter(1.0f, 2.0);",
            ),
            (
                "next_after_double_literal",
                "double",
                "return Math.nextAfter(1.0, 0.0);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_literal = mgr
            .execute_function("ks", "next_after_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_literal.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() + 1)
        );

        let double_literal = mgr
            .execute_function("ks", "next_after_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_literal.try_into().unwrap()),
            f64::from_bits(1.0f64.to_bits() - 1)
        );

        let float_result = mgr
            .execute_function(
                "ks",
                "next_after_float",
                &["float".to_string(), "double".to_string()],
                &[
                    Some(1.0f32.to_be_bytes().to_vec()),
                    Some(2.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(float_result.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() + 1)
        );

        let double_result = mgr
            .execute_function(
                "ks",
                "next_after_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(1.0f64.to_be_bytes().to_vec()),
                    Some(0.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(double_result.try_into().unwrap()),
            f64::from_bits(1.0f64.to_bits() - 1)
        );

        let negative_zero_result = mgr
            .execute_function(
                "ks",
                "next_after_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(0.0f64.to_be_bytes().to_vec()),
                    Some((-1.0f64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(negative_zero_result.try_into().unwrap()).to_bits(),
            (-f64::from_bits(1)).to_bits()
        );
    }

    #[test]
    fn java_math_fma_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, arg_type) in [("fma_float", "float"), ("fma_double", "double")] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![
                    arg_type.to_string(),
                    arg_type.to_string(),
                    arg_type.to_string(),
                ],
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: "return Math.fma(multiplicand, multiplier, addend);".to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, return_type, body) in [
            (
                "fma_float_literal",
                "float",
                "return Math.fma(2.5f, 4.0f, 0.5f);",
            ),
            (
                "fma_double_literal",
                "double",
                "return Math.fma(1.25, 8.0, -0.75);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let float_literal = mgr
            .execute_function("ks", "fma_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_literal.try_into().unwrap()), 10.5);

        let double_literal = mgr
            .execute_function("ks", "fma_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_literal.try_into().unwrap()), 9.25);

        let float_result = mgr
            .execute_function(
                "ks",
                "fma_float",
                &[
                    "float".to_string(),
                    "float".to_string(),
                    "float".to_string(),
                ],
                &[
                    Some(2.5f32.to_be_bytes().to_vec()),
                    Some(4.0f32.to_be_bytes().to_vec()),
                    Some(0.5f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(float_result.try_into().unwrap()), 10.5);

        let double_result = mgr
            .execute_function(
                "ks",
                "fma_double",
                &[
                    "double".to_string(),
                    "double".to_string(),
                    "double".to_string(),
                ],
                &[
                    Some(1.25f64.to_be_bytes().to_vec()),
                    Some(8.0f64.to_be_bytes().to_vec()),
                    Some((-0.75f64).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(double_result.try_into().unwrap()), 9.25);
    }

    #[test]
    fn java_math_float_unary_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("next_up_float", "return Math.nextUp(value);"),
            ("next_down_float", "return Math.nextDown(value);"),
            ("ulp_float", "return Math.ulp(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["float".to_string()],
                return_type: "float".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("next_up_float_literal", "return Math.nextUp(1.0f);"),
            ("next_down_float_literal", "return Math.nextDown(1.0f);"),
            ("ulp_float_literal", "return Math.ulp(1.0f);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "float".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let next_up_literal = mgr
            .execute_function("ks", "next_up_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(next_up_literal.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() + 1)
        );

        let next_down_literal = mgr
            .execute_function("ks", "next_down_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(next_down_literal.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() - 1)
        );

        let ulp_literal = mgr
            .execute_function("ks", "ulp_float_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(ulp_literal.try_into().unwrap()),
            f32::EPSILON
        );

        let next_up = mgr
            .execute_function(
                "ks",
                "next_up_float",
                &["float".to_string()],
                &[Some(1.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(next_up.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() + 1)
        );

        let next_down = mgr
            .execute_function(
                "ks",
                "next_down_float",
                &["float".to_string()],
                &[Some(1.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f32::from_be_bytes(next_down.try_into().unwrap()),
            f32::from_bits(1.0f32.to_bits() - 1)
        );

        let ulp = mgr
            .execute_function(
                "ks",
                "ulp_float",
                &["float".to_string()],
                &[Some(1.0f32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f32::from_be_bytes(ulp.try_into().unwrap()), f32::EPSILON);
    }

    #[test]
    fn java_math_double_unary_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("sqrt_double", "return Math.sqrt(value);"),
            ("floor_double", "return Math.floor(value);"),
            ("ceil_double", "return Math.ceil(value);"),
            ("rint_double", "return Math.rint(value);"),
            ("next_up_double", "return Math.nextUp(value);"),
            ("next_down_double", "return Math.nextDown(value);"),
            ("ulp_double", "return Math.ulp(value);"),
            ("log_double", "return Math.log(value);"),
            ("log10_double", "return Math.log10(value);"),
            ("exp_double", "return Math.exp(value);"),
            ("expm1_double", "return Math.expm1(value);"),
            ("log1p_double", "return Math.log1p(value);"),
            ("sin_double", "return Math.sin(value);"),
            ("cos_double", "return Math.cos(value);"),
            ("tan_double", "return Math.tan(value);"),
            ("sinh_double", "return Math.sinh(value);"),
            ("cosh_double", "return Math.cosh(value);"),
            ("tanh_double", "return Math.tanh(value);"),
            ("asin_double", "return Math.asin(value);"),
            ("acos_double", "return Math.acos(value);"),
            ("atan_double", "return Math.atan(value);"),
            ("cbrt_double", "return Math.cbrt(value);"),
            ("radians_double", "return Math.toRadians(value);"),
            ("degrees_double", "return Math.toDegrees(value);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["double".to_string()],
                return_type: "double".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("sqrt_double_literal", "return Math.sqrt(81.0);"),
            ("floor_double_literal", "return Math.floor(9.75);"),
            ("next_up_double_literal", "return Math.nextUp(1.0);"),
            ("ulp_double_literal", "return Math.ulp(1.0);"),
            ("log10_double_literal", "return Math.log10(1000.0);"),
            (
                "degrees_double_literal",
                "return Math.toDegrees(1.5707963267948966);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "double".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let sqrt_double_literal = mgr
            .execute_function("ks", "sqrt_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(sqrt_double_literal.try_into().unwrap()),
            9.0
        );

        let floor_double_literal = mgr
            .execute_function("ks", "floor_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(floor_double_literal.try_into().unwrap()),
            9.0
        );

        let next_up_double_literal = mgr
            .execute_function("ks", "next_up_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(next_up_double_literal.try_into().unwrap()),
            f64::from_bits(1.0f64.to_bits() + 1)
        );

        let ulp_double_literal = mgr
            .execute_function("ks", "ulp_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(ulp_double_literal.try_into().unwrap()),
            f64::EPSILON
        );

        let log10_double_literal = mgr
            .execute_function("ks", "log10_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(log10_double_literal.try_into().unwrap()) - 3.0).abs() < 1e-12);

        let degrees_double_literal = mgr
            .execute_function("ks", "degrees_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(degrees_double_literal.try_into().unwrap()) - 90.0).abs() < 1e-12
        );

        let sqrt_double = mgr
            .execute_function(
                "ks",
                "sqrt_double",
                &["double".to_string()],
                &[Some(81.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(sqrt_double.try_into().unwrap()), 9.0);

        let floor_double = mgr
            .execute_function(
                "ks",
                "floor_double",
                &["double".to_string()],
                &[Some(9.75f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(floor_double.try_into().unwrap()), 9.0);

        let ceil_double = mgr
            .execute_function(
                "ks",
                "ceil_double",
                &["double".to_string()],
                &[Some(9.25f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(ceil_double.try_into().unwrap()), 10.0);

        let rint_double = mgr
            .execute_function(
                "ks",
                "rint_double",
                &["double".to_string()],
                &[Some(2.5f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(rint_double.try_into().unwrap()), 2.0);

        let next_up_double = mgr
            .execute_function(
                "ks",
                "next_up_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(next_up_double.try_into().unwrap()),
            f64::from_bits(1.0f64.to_bits() + 1)
        );

        let next_down_double = mgr
            .execute_function(
                "ks",
                "next_down_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(next_down_double.try_into().unwrap()),
            f64::from_bits(1.0f64.to_bits() - 1)
        );

        let ulp_double = mgr
            .execute_function(
                "ks",
                "ulp_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(ulp_double.try_into().unwrap()),
            f64::EPSILON
        );

        let log_double = mgr
            .execute_function(
                "ks",
                "log_double",
                &["double".to_string()],
                &[Some(std::f64::consts::E.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(log_double.try_into().unwrap()) - 1.0).abs() < 1e-12);

        let log10_double = mgr
            .execute_function(
                "ks",
                "log10_double",
                &["double".to_string()],
                &[Some(1000.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(log10_double.try_into().unwrap()) - 3.0).abs() < 1e-12);

        let exp_double = mgr
            .execute_function(
                "ks",
                "exp_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(exp_double.try_into().unwrap()) - std::f64::consts::E).abs()
                < 1e-12
        );

        let expm1_double = mgr
            .execute_function(
                "ks",
                "expm1_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(expm1_double.try_into().unwrap()) - std::f64::consts::E + 1.0)
                .abs()
                < 1e-12
        );

        let log1p_double = mgr
            .execute_function(
                "ks",
                "log1p_double",
                &["double".to_string()],
                &[Some((std::f64::consts::E - 1.0).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(log1p_double.try_into().unwrap()) - 1.0).abs() < 1e-12);

        let sin_double = mgr
            .execute_function(
                "ks",
                "sin_double",
                &["double".to_string()],
                &[Some((std::f64::consts::PI / 2.0).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(sin_double.try_into().unwrap()) - 1.0).abs() < 1e-12);

        let cos_double = mgr
            .execute_function(
                "ks",
                "cos_double",
                &["double".to_string()],
                &[Some(std::f64::consts::PI.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(cos_double.try_into().unwrap()) + 1.0).abs() < 1e-12);

        let tan_double = mgr
            .execute_function(
                "ks",
                "tan_double",
                &["double".to_string()],
                &[Some((std::f64::consts::PI / 4.0).to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(tan_double.try_into().unwrap()) - 1.0).abs() < 1e-12);

        let sinh_double = mgr
            .execute_function(
                "ks",
                "sinh_double",
                &["double".to_string()],
                &[Some(0.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(sinh_double.try_into().unwrap()), 0.0);

        let cosh_double = mgr
            .execute_function(
                "ks",
                "cosh_double",
                &["double".to_string()],
                &[Some(0.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(cosh_double.try_into().unwrap()), 1.0);

        let tanh_double = mgr
            .execute_function(
                "ks",
                "tanh_double",
                &["double".to_string()],
                &[Some(0.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(tanh_double.try_into().unwrap()), 0.0);

        let asin_double = mgr
            .execute_function(
                "ks",
                "asin_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(asin_double.try_into().unwrap()) - std::f64::consts::PI / 2.0)
                .abs()
                < 1e-12
        );

        let acos_double = mgr
            .execute_function(
                "ks",
                "acos_double",
                &["double".to_string()],
                &[Some(0.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(acos_double.try_into().unwrap()) - std::f64::consts::PI / 2.0)
                .abs()
                < 1e-12
        );

        let atan_double = mgr
            .execute_function(
                "ks",
                "atan_double",
                &["double".to_string()],
                &[Some(1.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(atan_double.try_into().unwrap()) - std::f64::consts::PI / 4.0)
                .abs()
                < 1e-12
        );

        let cbrt_double = mgr
            .execute_function(
                "ks",
                "cbrt_double",
                &["double".to_string()],
                &[Some(27.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(cbrt_double.try_into().unwrap()) - 3.0).abs() < 1e-12);

        let radians_double = mgr
            .execute_function(
                "ks",
                "radians_double",
                &["double".to_string()],
                &[Some(180.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(radians_double.try_into().unwrap()) - std::f64::consts::PI).abs()
                < 1e-12
        );

        let degrees_double = mgr
            .execute_function(
                "ks",
                "degrees_double",
                &["double".to_string()],
                &[Some(std::f64::consts::PI.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(degrees_double.try_into().unwrap()) - 180.0).abs() < 1e-12);
    }

    #[test]
    fn java_math_ulp_body_handles_double_edge_cases() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "ulp_double".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["double".to_string()],
            return_type: "double".to_string(),
            language: "java".to_string(),
            body: "return Math.ulp(value);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let zero_ulp = mgr
            .execute_function(
                "ks",
                "ulp_double",
                &["double".to_string()],
                &[Some(0.0f64.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(zero_ulp.try_into().unwrap()),
            f64::from_bits(1)
        );

        let max_ulp = mgr
            .execute_function(
                "ks",
                "ulp_double",
                &["double".to_string()],
                &[Some(f64::MAX.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(max_ulp.try_into().unwrap()),
            f64::from_bits((1994_u64) << 52)
        );

        let infinite_ulp = mgr
            .execute_function(
                "ks",
                "ulp_double",
                &["double".to_string()],
                &[Some(f64::INFINITY.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            f64::from_be_bytes(infinite_ulp.try_into().unwrap()),
            f64::INFINITY
        );
    }

    #[test]
    fn java_math_pow_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "pow_double".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["double".to_string(), "double".to_string()],
            return_type: "double".to_string(),
            language: "java".to_string(),
            body: "return Math.pow(base, exponent);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        mgr.register_function(UdfDefinition {
            name: "pow_double_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec![],
            return_type: "double".to_string(),
            language: "java".to_string(),
            body: "return Math.pow(2.5, 3.0);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "strict_pow_double_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec![],
            return_type: "double".to_string(),
            language: "java".to_string(),
            body: "return StrictMath.pow(2.5, 3.0);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        let literal_result = mgr
            .execute_function("ks", "pow_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(literal_result.try_into().unwrap()) - 15.625).abs() < 1e-12);
        let strict_literal_result = mgr
            .execute_function("ks", "strict_pow_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(strict_literal_result.try_into().unwrap()) - 15.625).abs() < 1e-12
        );

        let result = mgr
            .execute_function(
                "ks",
                "pow_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(2.5f64.to_be_bytes().to_vec()),
                    Some(3.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert!((f64::from_be_bytes(result.try_into().unwrap()) - 15.625).abs() < 1e-12);
    }

    #[test]
    fn java_math_double_binary_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("hypot_double", "return Math.hypot(a, b);"),
            ("atan2_double", "return Math.atan2(a, b);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["double".to_string(), "double".to_string()],
                return_type: "double".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            ("hypot_double_literal", "return Math.hypot(3.0, 4.0);"),
            ("atan2_double_literal", "return Math.atan2(1.0, 1.0);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "double".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let hypot_literal = mgr
            .execute_function("ks", "hypot_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(hypot_literal.try_into().unwrap()), 5.0);

        let atan2_literal = mgr
            .execute_function("ks", "atan2_double_literal", &[], &[])
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(atan2_literal.try_into().unwrap()) - std::f64::consts::PI / 4.0)
                .abs()
                < 1e-12
        );

        let hypot = mgr
            .execute_function(
                "ks",
                "hypot_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(3.0f64.to_be_bytes().to_vec()),
                    Some(4.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(f64::from_be_bytes(hypot.try_into().unwrap()), 5.0);

        let atan2 = mgr
            .execute_function(
                "ks",
                "atan2_double",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(1.0f64.to_be_bytes().to_vec()),
                    Some(1.0f64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert!(
            (f64::from_be_bytes(atan2.try_into().unwrap()) - std::f64::consts::PI / 4.0).abs()
                < 1e-12
        );
    }

    #[test]
    fn java_text_equals_argument_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("text_eq_args", "return a.equals(b);"),
            ("text_ne_args", "return !a.equals(b);"),
            ("text_content_eq_args", "return a.contentEquals(b);"),
            ("text_content_ne_args", "return !a.contentEquals(b);"),
            ("text_objects_eq_args", "return Objects.equals(a, b);"),
            (
                "text_objects_ne_args",
                "return !java.util.Objects.equals(a, b);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string(), "text".to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_eq_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_ne_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Grace".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_content_eq_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_content_ne_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Grace".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_objects_eq_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_objects_ne_args",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"Grace".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_objects_null_check_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("objects_is_null", "text", "return Objects.isNull(value);"),
            (
                "objects_non_null",
                "int",
                "return java.util.Objects.nonNull(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: true,
            })
            .unwrap();
        }

        for (name, body) in [
            ("objects_is_null_literal", "return Objects.isNull(null);"),
            (
                "objects_non_null_literal",
                "return java.util.Objects.nonNull(\"Ada\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: true,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function("ks", "objects_is_null_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "objects_non_null_literal", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );

        assert_eq!(
            mgr.execute_function("ks", "objects_is_null", &["text".to_string()], &[None])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_is_null",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function("ks", "objects_non_null", &["int".to_string()], &[None])
                .unwrap()
                .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_non_null",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_objects_require_non_null_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, return_type, body) in [
            (
                "objects_require_non_null_text",
                "text",
                "text",
                "return Objects.requireNonNull(value);",
            ),
            (
                "qualified_objects_require_non_null_int",
                "int",
                "int",
                "return java.util.Objects.requireNonNull(value);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: true,
            })
            .unwrap();
        }
        mgr.register_function(UdfDefinition {
            name: "objects_require_non_null_text_message_arg".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Objects.requireNonNull(value, message);".to_string(),
            called_on_null_input: true,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "qualified_objects_require_non_null_text_message_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return java.util.Objects.requireNonNull(value, \"missing value\");".to_string(),
            called_on_null_input: true,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "objects_require_non_null_text_message_comma_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Objects.requireNonNull(value, \"missing, value\");".to_string(),
            called_on_null_input: true,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_text",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "qualified_objects_require_non_null_int",
                &["int".to_string()],
                &[Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            42i32.to_be_bytes().to_vec()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_text_message_arg",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"missing value".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "qualified_objects_require_non_null_text_message_literal",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_text_message_comma_literal",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        let err = mgr
            .execute_function(
                "ks",
                "objects_require_non_null_text",
                &["text".to_string()],
                &[None],
            )
            .unwrap_err();
        assert!(err.contains("Objects.requireNonNull"));
    }

    #[test]
    fn java_objects_require_non_null_else_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            (
                "objects_require_non_null_else_text",
                "text",
                "return Objects.requireNonNullElse(value, fallback);",
            ),
            (
                "qualified_objects_require_non_null_else_int",
                "int",
                "return java.util.Objects.requireNonNullElse(value, fallback);",
            ),
            (
                "objects_require_non_null_else_literal_text",
                "text",
                "return Objects.requireNonNullElse(value, \"Missing, Value\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: if name.ends_with("_literal_text") {
                    vec![arg_type.to_string()]
                } else {
                    vec![arg_type.to_string(), arg_type.to_string()]
                },
                return_type: arg_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: true,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_else_text",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"fallback".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_else_text",
                &["text".to_string(), "text".to_string()],
                &[None, Some(b"fallback".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"fallback"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "qualified_objects_require_non_null_else_int",
                &["int".to_string(), "int".to_string()],
                &[None, Some(42i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            42i32.to_be_bytes().to_vec()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_require_non_null_else_literal_text",
                &["text".to_string()],
                &[None],
            )
            .unwrap()
            .unwrap(),
            b"Missing, Value"
        );
        let err = mgr
            .execute_function(
                "ks",
                "objects_require_non_null_else_text",
                &["text".to_string(), "text".to_string()],
                &[None, None],
            )
            .unwrap_err();
        assert!(err.contains("Objects.requireNonNullElse"));
    }

    #[test]
    fn java_objects_to_string_default_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "objects_to_string_default".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Objects.toString(value, fallback);".to_string(),
            called_on_null_input: true,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "qualified_objects_to_string_default".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return java.util.Objects.toString(value, fallback);".to_string(),
            called_on_null_input: true,
        })
        .unwrap();
        mgr.register_function(UdfDefinition {
            name: "objects_to_string_default_literal".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return Objects.toString(value, \"Missing, Value\");".to_string(),
            called_on_null_input: true,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_default",
                &["text".to_string(), "text".to_string()],
                &[Some(b"Ada".to_vec()), Some(b"fallback".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"Ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_default",
                &["text".to_string(), "text".to_string()],
                &[None, Some(b"fallback".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"fallback"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "qualified_objects_to_string_default",
                &["text".to_string(), "text".to_string()],
                &[None, Some(b"qualified".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"qualified"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "objects_to_string_default_literal",
                &["text".to_string()],
                &[None],
            )
            .unwrap()
            .unwrap(),
            b"Missing, Value"
        );
    }

    #[test]
    fn java_boxed_primitive_equals_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, arg_type, body) in [
            ("int_eq_args", "int", "return a.equals(b);"),
            ("int_objects_eq_args", "int", "return Objects.equals(a, b);"),
            ("long_ne_args", "bigint", "return !a.equals(b);"),
            ("bool_eq_args", "boolean", "return a.equals(b);"),
            (
                "bool_objects_ne_args",
                "boolean",
                "return !Objects.equals(a, b);",
            ),
            ("float_eq_args", "float", "return a.equals(b);"),
            (
                "float_objects_eq_args",
                "float",
                "return Objects.equals(a, b);",
            ),
            ("float_ne_args", "float", "return !a.equals(b);"),
            ("double_eq_args", "double", "return a.equals(b);"),
            (
                "double_objects_eq_args",
                "double",
                "return java.util.Objects.equals(a, b);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![arg_type.to_string(), arg_type.to_string()],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for (name, body) in [
            (
                "boxed_int_eq_literal",
                "return Integer.valueOf(42).equals(Integer.valueOf(42));",
            ),
            (
                "boxed_long_int_ne_literal",
                "return !Long.valueOf(42).equals(Integer.valueOf(42));",
            ),
            (
                "boxed_float_nan_eq_literal",
                "return Float.valueOf(Float.NaN).equals(Float.valueOf(Float.NaN));",
            ),
            (
                "string_literal_eq_literal",
                "return \"Ada\".equals(\"Ada\");",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        for name in [
            "boxed_int_eq_literal",
            "boxed_long_int_ne_literal",
            "boxed_float_nan_eq_literal",
            "string_literal_eq_literal",
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                vec![1]
            );
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_eq_args",
                &["int".to_string(), "int".to_string()],
                &[
                    Some(42i32.to_be_bytes().to_vec()),
                    Some(42i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "long_ne_args",
                &["bigint".to_string(), "bigint".to_string()],
                &[
                    Some(9i64.to_be_bytes().to_vec()),
                    Some(7i64.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "int_objects_eq_args",
                &["int".to_string(), "int".to_string()],
                &[
                    Some((-7i32).to_be_bytes().to_vec()),
                    Some((-7i32).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_eq_args",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![1])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "bool_objects_ne_args",
                &["boolean".to_string(), "boolean".to_string()],
                &[Some(vec![1]), Some(vec![0])],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_eq_args",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(f32::NAN.to_be_bytes().to_vec()),
                    Some(f32::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_objects_eq_args",
                &["float".to_string(), "float".to_string()],
                &[
                    Some(f32::NAN.to_be_bytes().to_vec()),
                    Some(f32::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "float_ne_args",
                &["float".to_string(), "float".to_string()],
                &[
                    Some((-0.0f32).to_be_bytes().to_vec()),
                    Some(0.0f32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_eq_args",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(f64::NAN.to_be_bytes().to_vec()),
                    Some(f64::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "double_objects_eq_args",
                &["double".to_string(), "double".to_string()],
                &[
                    Some(f64::NAN.to_be_bytes().to_vec()),
                    Some(f64::NAN.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_text_predicate_argument_bodies_map_to_boolean_results() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("contains_arg", "boolean", "return value.contains(needle);"),
            (
                "starts_with_arg",
                "boolean",
                "return value.startsWith(prefix);",
            ),
            ("ends_with_arg", "boolean", "return value.endsWith(suffix);"),
            (
                "equals_ignore_case_arg",
                "boolean",
                "return value.equalsIgnoreCase(other);",
            ),
            ("compare_to_arg", "int", "return value.compareTo(other);"),
            (
                "compare_to_ignore_case_arg",
                "int",
                "return value.compareToIgnoreCase(other);",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string(), "text".to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let text = Some(b"node-db-1".to_vec());
        assert_eq!(
            mgr.execute_function(
                "ks",
                "contains_arg",
                &["text".to_string(), "text".to_string()],
                &[text.clone(), Some(b"db".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_arg",
                &["text".to_string(), "text".to_string()],
                &[text.clone(), Some(b"node".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "ends_with_arg",
                &["text".to_string(), "text".to_string()],
                &[text.clone(), Some(b"1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "equals_ignore_case_arg",
                &["text".to_string(), "text".to_string()],
                &[text, Some(b"NODE-DB-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "equals_ignore_case_arg",
                &["text".to_string(), "text".to_string()],
                &[Some("ß".as_bytes().to_vec()), Some(b"ss".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        let compare_to_arg = mgr
            .execute_function(
                "ks",
                "compare_to_arg",
                &["text".to_string(), "text".to_string()],
                &[Some(b"node-db-1".to_vec()), Some(b"node-db-2".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(compare_to_arg.try_into().unwrap()), -1);
        let compare_to_ignore_case_arg = mgr
            .execute_function(
                "ks",
                "compare_to_ignore_case_arg",
                &["text".to_string(), "text".to_string()],
                &[Some(b"node-db-1".to_vec()), Some(b"NODE-DB-2".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(compare_to_ignore_case_arg.try_into().unwrap()),
            -1
        );
        let compare_expanding_case = mgr
            .execute_function(
                "ks",
                "compare_to_ignore_case_arg",
                &["text".to_string(), "text".to_string()],
                &[Some("ß".as_bytes().to_vec()), Some(b"ss".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(compare_expanding_case.try_into().unwrap()),
            108
        );
    }

    #[test]
    fn java_text_starts_with_from_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "starts_with_literal_from",
                "return value.startsWith(\"db\", 5);",
                vec!["text".to_string()],
            ),
            (
                "starts_with_comma_literal_from",
                "return value.startsWith(\",\", 1);",
                vec!["text".to_string()],
            ),
            (
                "starts_with_arg_from",
                "return value.startsWith(prefix, offset);",
                vec!["text".to_string(), "text".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "boolean".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let text = Some(b"node-db-1".to_vec());
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_literal_from",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_comma_literal_from",
                &["text".to_string()],
                &[Some(b"a,b".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    text.clone(),
                    Some(b"db".to_vec()),
                    Some(5i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    text,
                    Some(b"db".to_vec()),
                    Some(6i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some(b"b".to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some(b"b".to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
    }

    #[test]
    fn java_text_index_argument_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body) in [
            ("index_of_arg", "return value.indexOf(needle);"),
            ("last_index_of_arg", "return value.lastIndexOf(needle);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string(), "text".to_string()],
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let text = Some(b"node-db-db".to_vec());
        let first_index = mgr
            .execute_function(
                "ks",
                "index_of_arg",
                &["text".to_string(), "text".to_string()],
                &[text.clone(), Some(b"db".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(first_index.try_into().unwrap()), 5);

        let last_index = mgr
            .execute_function(
                "ks",
                "last_index_of_arg",
                &["text".to_string(), "text".to_string()],
                &[text, Some(b"db".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(last_index.try_into().unwrap()), 8);

        let after_surrogate_pair = mgr
            .execute_function(
                "ks",
                "index_of_arg",
                &["text".to_string(), "text".to_string()],
                &[Some("a😀b".as_bytes().to_vec()), Some(b"b".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(after_surrogate_pair.try_into().unwrap()),
            3
        );
    }

    #[test]
    fn java_text_index_from_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "index_literal_from",
                "return value.indexOf(\"db\", 6);",
                vec!["text".to_string()],
            ),
            (
                "index_comma_literal_from",
                "return value.indexOf(\",\", 0);",
                vec!["text".to_string()],
            ),
            (
                "last_index_literal_from",
                "return value.lastIndexOf(\"db\", 7);",
                vec!["text".to_string()],
            ),
            (
                "index_arg_from",
                "return value.indexOf(needle, fromIndex);",
                vec!["text".to_string(), "text".to_string(), "int".to_string()],
            ),
            (
                "last_index_arg_from",
                "return value.lastIndexOf(needle, fromIndex);",
                vec!["text".to_string(), "text".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let text = Some(b"node-db-db".to_vec());
        let index_literal = mgr
            .execute_function(
                "ks",
                "index_literal_from",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(index_literal.try_into().unwrap()), 8);

        let index_comma_literal = mgr
            .execute_function(
                "ks",
                "index_comma_literal_from",
                &["text".to_string()],
                &[Some(b"a,b".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(index_comma_literal.try_into().unwrap()),
            1
        );

        let last_index_literal = mgr
            .execute_function(
                "ks",
                "last_index_literal_from",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(last_index_literal.try_into().unwrap()),
            5
        );

        let index_arg = mgr
            .execute_function(
                "ks",
                "index_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    text.clone(),
                    Some(b"db".to_vec()),
                    Some(6i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(index_arg.try_into().unwrap()), 8);

        let last_index_arg = mgr
            .execute_function(
                "ks",
                "last_index_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    text,
                    Some(b"db".to_vec()),
                    Some(7i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(last_index_arg.try_into().unwrap()), 5);

        let index_from_low_surrogate = mgr
            .execute_function(
                "ks",
                "index_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some(b"b".to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(index_from_low_surrogate.try_into().unwrap()),
            3
        );

        let last_index_from_low_surrogate = mgr
            .execute_function(
                "ks",
                "last_index_arg_from",
                &["text".to_string(), "text".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some("😀".as_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(last_index_from_low_surrogate.try_into().unwrap()),
            1
        );
    }

    #[test]
    fn java_text_replace_argument_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "replace_args".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "text".to_string(), "text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return value.replace(target, replacement);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_args",
                &["text".to_string(), "text".to_string(), "text".to_string()],
                &[
                    Some(b"node-old-old".to_vec()),
                    Some(b"old".to_vec()),
                    Some(b"new".to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            b"node-new-new"
        );
    }

    #[test]
    fn java_text_repeat_argument_body_maps_to_native_op() {
        let mut mgr = UdfManager::new();
        mgr.register_function(UdfDefinition {
            name: "repeat_arg".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string(), "int".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return value.repeat(count);".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

        assert_eq!(
            mgr.execute_function(
                "ks",
                "repeat_arg",
                &["text".to_string(), "int".to_string()],
                &[Some(b"ab".to_vec()), Some(3i32.to_be_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ababab"
        );

        let err = mgr
            .execute_function(
                "ks",
                "repeat_arg",
                &["text".to_string(), "int".to_string()],
                &[Some(b"ab".to_vec()), Some((-1i32).to_be_bytes().to_vec())],
            )
            .unwrap_err();
        assert!(err.contains("Negative repeat count"));
    }

    #[test]
    fn java_text_concat_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "concat_arg",
                "return value.concat(suffix);",
                vec!["text".to_string(), "text".to_string()],
            ),
            (
                "concat_literal",
                "return value.concat(\"-suffix\");",
                vec!["text".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "concat_arg",
                &["text".to_string(), "text".to_string()],
                &[Some(b"node".to_vec()), Some(b"-db".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-db"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "concat_literal",
                &["text".to_string()],
                &[Some(b"node".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-suffix"
        );
    }

    #[test]
    fn java_text_substring_argument_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "substring_from_arg",
                "return value.substring(begin);",
                vec!["text".to_string(), "int".to_string()],
            ),
            (
                "substring_range_args",
                "return value.substring(begin, end);",
                vec!["text".to_string(), "int".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        assert_eq!(
            mgr.execute_function(
                "ks",
                "substring_from_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"node-db-1".to_vec()),
                    Some(5i32.to_be_bytes().to_vec())
                ],
            )
            .unwrap()
            .unwrap(),
            b"db-1"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "substring_range_args",
                &["text".to_string(), "int".to_string(), "int".to_string()],
                &[
                    Some(b"node-db-1".to_vec()),
                    Some(5i32.to_be_bytes().to_vec()),
                    Some(7i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            b"db"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "substring_range_args",
                &["text".to_string(), "int".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap(),
            "😀".as_bytes()
        );
    }

    #[test]
    fn java_text_char_at_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "char_at_literal",
                "return value.charAt(5);",
                vec!["text".to_string()],
            ),
            (
                "char_at_arg",
                "return value.charAt(index);",
                vec!["text".to_string(), "int".to_string()],
            ),
            (
                "char_at_surrogate",
                "return value.charAt(0);",
                vec!["text".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let literal = mgr
            .execute_function(
                "ks",
                "char_at_literal",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(literal.try_into().unwrap()), 'd' as i32);

        let arg = mgr
            .execute_function(
                "ks",
                "char_at_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"node-db-1".to_vec()),
                    Some(6i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(arg.try_into().unwrap()), 'b' as i32);

        let surrogate = mgr
            .execute_function(
                "ks",
                "char_at_surrogate",
                &["text".to_string()],
                &[Some("😀".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(surrogate.try_into().unwrap()), 0xD83D);
    }

    #[test]
    fn java_text_code_point_at_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "code_point_at_literal",
                "return value.codePointAt(0);",
                vec!["text".to_string()],
            ),
            (
                "code_point_at_arg",
                "return value.codePointAt(index);",
                vec!["text".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let emoji = mgr
            .execute_function(
                "ks",
                "code_point_at_literal",
                &["text".to_string()],
                &[Some("😀".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(emoji.try_into().unwrap()), 0x1F600);

        let ascii = mgr
            .execute_function(
                "ks",
                "code_point_at_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"node-db-1".to_vec()),
                    Some(5i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(ascii.try_into().unwrap()), 'd' as i32);

        let low_surrogate = mgr
            .execute_function(
                "ks",
                "code_point_at_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some("😀".as_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(low_surrogate.try_into().unwrap()),
            0xDE00
        );
    }

    #[test]
    fn java_text_code_point_before_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "code_point_before_literal",
                "return value.codePointBefore(2);",
                vec!["text".to_string()],
            ),
            (
                "code_point_before_arg",
                "return value.codePointBefore(index);",
                vec!["text".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let emoji = mgr
            .execute_function(
                "ks",
                "code_point_before_literal",
                &["text".to_string()],
                &[Some("😀".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(emoji.try_into().unwrap()), 0x1F600);

        let ascii = mgr
            .execute_function(
                "ks",
                "code_point_before_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some(b"node-db-1".to_vec()),
                    Some(6i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(ascii.try_into().unwrap()), 'd' as i32);

        let high_surrogate = mgr
            .execute_function(
                "ks",
                "code_point_before_arg",
                &["text".to_string(), "int".to_string()],
                &[
                    Some("😀".as_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(high_surrogate.try_into().unwrap()),
            0xD83D
        );
    }

    #[test]
    fn java_text_code_point_count_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "code_point_count_literal",
                "return value.codePointCount(0, 4);",
                vec!["text".to_string()],
            ),
            (
                "code_point_count_args",
                "return value.codePointCount(begin, end);",
                vec!["text".to_string(), "int".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let with_surrogate_pair = mgr
            .execute_function(
                "ks",
                "code_point_count_literal",
                &["text".to_string()],
                &[Some("a😀b".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(with_surrogate_pair.try_into().unwrap()),
            3
        );

        let partial_pair = mgr
            .execute_function(
                "ks",
                "code_point_count_args",
                &["text".to_string(), "int".to_string(), "int".to_string()],
                &[
                    Some("a😀b".as_bytes().to_vec()),
                    Some(1i32.to_be_bytes().to_vec()),
                    Some(2i32.to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(partial_pair.try_into().unwrap()), 1);
    }

    #[test]
    fn java_text_offset_by_code_points_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, body, arg_types) in [
            (
                "offset_by_code_points_literal",
                "return value.offsetByCodePoints(1, 1);",
                vec!["text".to_string()],
            ),
            (
                "offset_by_code_points_args",
                "return value.offsetByCodePoints(index, offset);",
                vec!["text".to_string(), "int".to_string(), "int".to_string()],
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types,
                return_type: "int".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let forward_over_pair = mgr
            .execute_function(
                "ks",
                "offset_by_code_points_literal",
                &["text".to_string()],
                &[Some("a😀bc".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(forward_over_pair.try_into().unwrap()), 3);

        let backward_over_pair = mgr
            .execute_function(
                "ks",
                "offset_by_code_points_args",
                &["text".to_string(), "int".to_string(), "int".to_string()],
                &[
                    Some("a😀bc".as_bytes().to_vec()),
                    Some(3i32.to_be_bytes().to_vec()),
                    Some((-1i32).to_be_bytes().to_vec()),
                ],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(backward_over_pair.try_into().unwrap()),
            1
        );
    }

    #[test]
    fn java_text_method_literal_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("contains_db", "boolean", "return value.contains(\"db\");"),
            (
                "starts_with_node",
                "boolean",
                "return value.startsWith(\"node\");",
            ),
            ("ends_with_one", "boolean", "return value.endsWith(\"1\");"),
            (
                "equals_ignore_case",
                "boolean",
                "return value.equalsIgnoreCase(\"NODE-DB-1\");",
            ),
            (
                "content_equals_literal",
                "boolean",
                "return value.contentEquals(\"node-db-1\");",
            ),
            (
                "compare_to_literal",
                "int",
                "return value.compareTo(\"node-db-0\");",
            ),
            (
                "compare_to_ignore_case_literal",
                "int",
                "return value.compareToIgnoreCase(\"NODE-DB-0\");",
            ),
            ("index_of_dash", "int", "return value.indexOf(\"-\");"),
            (
                "contains_escaped_newline",
                "boolean",
                "return value.contains(\"\\n\");",
            ),
            (
                "last_index_of_dash",
                "int",
                "return value.lastIndexOf(\"-\");",
            ),
            ("substring_from", "text", "return value.substring(5);"),
            ("substring_range", "text", "return value.substring(5, 7);"),
            (
                "replace_literal",
                "text",
                "return value.replace(\"old\", \"new\");",
            ),
            (
                "replace_colon_literal",
                "text",
                "return value.replace(\":\", \"-\");",
            ),
            (
                "replace_comma_literal",
                "text",
                "return value.replace(\",\", \":\");",
            ),
            (
                "replace_char_literal",
                "text",
                "return value.replace('_', '-');",
            ),
            (
                "replace_escaped_char_literal",
                "text",
                "return value.replace('\\n', ' ');",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            (
                "literal_contains_db",
                "boolean",
                "return \"node-db-1\".contains(\"db\");",
            ),
            (
                "literal_content_equals",
                "boolean",
                "return \"node-db-1\".contentEquals(\"node-db-1\");",
            ),
            (
                "literal_starts_with_from",
                "boolean",
                "return \"node-db-1\".startsWith(\"db\", 5);",
            ),
            (
                "literal_ends_with_one",
                "boolean",
                "return \"node-db-1\".endsWith(\"1\");",
            ),
            (
                "literal_equals_ignore_case",
                "boolean",
                "return \"node-db-1\".equalsIgnoreCase(\"NODE-DB-1\");",
            ),
            (
                "literal_compare_to",
                "int",
                "return \"node-db-1\".compareTo(\"node-db-0\");",
            ),
            (
                "literal_compare_to_ignore_case",
                "int",
                "return \"node-db-1\".compareToIgnoreCase(\"NODE-DB-0\");",
            ),
            (
                "literal_index_of_dash",
                "int",
                "return \"node-db-1\".indexOf(\"-\");",
            ),
            (
                "literal_last_index_of_dash",
                "int",
                "return \"node-db-1\".lastIndexOf(\"-\");",
            ),
            (
                "literal_index_of_from",
                "int",
                "return \"node-db-1\".indexOf(\"-\", 5);",
            ),
            ("literal_concat", "text", "return \"node\".concat(\"-db\");"),
            ("literal_char_at", "int", "return \"node\".charAt(1);"),
            (
                "literal_code_point_at",
                "int",
                "return \"a😀\".codePointAt(1);",
            ),
            (
                "literal_code_point_before",
                "int",
                "return \"a😀\".codePointBefore(3);",
            ),
            (
                "literal_code_point_count",
                "int",
                "return \"a😀b\".codePointCount(0, 4);",
            ),
            (
                "literal_offset_by_code_points",
                "int",
                "return \"a😀bc\".offsetByCodePoints(1, 1);",
            ),
            (
                "literal_substring_from",
                "text",
                "return \"node-db-1\".substring(5);",
            ),
            (
                "literal_substring_range",
                "text",
                "return \"node-db-1\".substring(5, 7);",
            ),
            (
                "literal_replace",
                "text",
                "return \"old_path\".replace(\"old\", \"new\");",
            ),
            (
                "literal_replace_char",
                "text",
                "return \"old_path\".replace('_', '-');",
            ),
            ("literal_repeat", "text", "return \"ha\".repeat(3);"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let text = Some(b"node-db-1".to_vec());
        assert_eq!(
            mgr.execute_function(
                "ks",
                "contains_db",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "starts_with_node",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "ends_with_one",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "content_equals_literal",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "equals_ignore_case",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        let compare_to_literal = mgr
            .execute_function(
                "ks",
                "compare_to_literal",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(compare_to_literal.try_into().unwrap()),
            1
        );
        let compare_to_ignore_case_literal = mgr
            .execute_function(
                "ks",
                "compare_to_ignore_case_literal",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::from_be_bytes(compare_to_ignore_case_literal.try_into().unwrap()),
            1
        );

        let first_index = mgr
            .execute_function(
                "ks",
                "index_of_dash",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(first_index.try_into().unwrap()), 4);

        let last_index = mgr
            .execute_function(
                "ks",
                "last_index_of_dash",
                &["text".to_string()],
                std::slice::from_ref(&text),
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(last_index.try_into().unwrap()), 7);

        assert_eq!(
            mgr.execute_function(
                "ks",
                "substring_from",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"db-1"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "substring_range",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"db"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "contains_escaped_newline",
                &["text".to_string()],
                &[Some(b"node\ndb".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );

        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_literal",
                &["text".to_string()],
                &[Some(b"old_path".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"new_path"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_colon_literal",
                &["text".to_string()],
                &[Some(b"old:path".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"old-path"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_comma_literal",
                &["text".to_string()],
                &[Some(b"a,b,c".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"a:b:c"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_char_literal",
                &["text".to_string()],
                &[Some(b"old_path".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"old-path"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "replace_escaped_char_literal",
                &["text".to_string()],
                &[Some(b"old\npath".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"old path"
        );
        for name in [
            "literal_contains_db",
            "literal_content_equals",
            "literal_starts_with_from",
            "literal_ends_with_one",
            "literal_equals_ignore_case",
        ] {
            assert_eq!(
                mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap(),
                vec![1]
            );
        }
        for (name, expected) in [
            ("literal_compare_to", 1),
            ("literal_compare_to_ignore_case", 1),
            ("literal_index_of_dash", 4),
            ("literal_last_index_of_dash", 7),
            ("literal_index_of_from", 7),
            ("literal_char_at", 111),
            ("literal_code_point_at", 128_512),
            ("literal_code_point_before", 128_512),
            ("literal_code_point_count", 3),
            ("literal_offset_by_code_points", 3),
        ] {
            let result = mgr.execute_function("ks", name, &[], &[]).unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), expected);
        }
        assert_eq!(
            mgr.execute_function("ks", "literal_concat", &[], &[])
                .unwrap()
                .unwrap(),
            b"node-db"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_substring_from", &[], &[])
                .unwrap()
                .unwrap(),
            b"db-1"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_substring_range", &[], &[])
                .unwrap()
                .unwrap(),
            b"db"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_replace", &[], &[])
                .unwrap()
                .unwrap(),
            b"new_path"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_replace_char", &[], &[])
                .unwrap()
                .unwrap(),
            b"old-path"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_repeat", &[], &[])
                .unwrap()
                .unwrap(),
            b"hahaha"
        );
    }

    #[test]
    fn java_no_arg_text_method_bodies_map_to_native_ops() {
        let mut mgr = UdfManager::new();
        for (name, return_type, body) in [
            ("text_length", "int", "return value.length();"),
            ("text_to_string", "text", "return value.toString();"),
            ("text_intern", "text", "return value.intern();"),
            ("text_trim", "text", "return value.trim();"),
            ("text_strip", "text", "return value.strip();"),
            ("text_strip_leading", "text", "return value.stripLeading();"),
            (
                "text_strip_trailing",
                "text",
                "return value.stripTrailing();",
            ),
            ("text_is_empty", "boolean", "return value.isEmpty();"),
            ("text_is_blank", "boolean", "return value.isBlank();"),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string()],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }
        for (name, return_type, body) in [
            ("literal_length", "int", "return \"😀\".length();"),
            (
                "literal_to_string",
                "text",
                "return \"node-db-1\".toString();",
            ),
            ("literal_intern", "text", "return \"node-db-1\".intern();"),
            ("literal_lower", "text", "return \"ADA\".toLowerCase();"),
            ("literal_upper", "text", "return \"åda\".toUpperCase();"),
            ("literal_trim", "text", "return \"  ada  \".trim();"),
            (
                "literal_strip",
                "text",
                "return \"\\u2003ada\\u2003\".strip();",
            ),
            (
                "literal_strip_leading",
                "text",
                "return \"  ada  \".stripLeading();",
            ),
            (
                "literal_strip_trailing",
                "text",
                "return \"  ada  \".stripTrailing();",
            ),
            ("literal_is_empty", "boolean", "return \"\".isEmpty();"),
            (
                "literal_is_blank",
                "boolean",
                "return \" \\t\\n\".isBlank();",
            ),
        ] {
            mgr.register_function(UdfDefinition {
                name: name.to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec![],
                return_type: return_type.to_string(),
                language: "java".to_string(),
                body: body.to_string(),
                called_on_null_input: false,
            })
            .unwrap();
        }

        let length = mgr
            .execute_function(
                "ks",
                "text_length",
                &["text".to_string()],
                &[Some("åda".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(length.try_into().unwrap()), 3);

        let surrogate_length = mgr
            .execute_function(
                "ks",
                "text_length",
                &["text".to_string()],
                &[Some("😀".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(surrogate_length.try_into().unwrap()), 2);

        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_to_string",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-db-1"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_intern",
                &["text".to_string()],
                &[Some(b"node-db-1".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"node-db-1"
        );

        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_trim",
                &["text".to_string()],
                &[Some(b"  ada  ".to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_trim",
                &["text".to_string()],
                &[Some("\u{2003}ada\u{2003}".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            "\u{2003}ada\u{2003}".as_bytes()
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_strip",
                &["text".to_string()],
                &[Some("\u{2003}ada\u{2003}".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_strip_leading",
                &["text".to_string()],
                &[Some("  ada  ".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"ada  "
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_strip_trailing",
                &["text".to_string()],
                &[Some("  ada  ".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            b"  ada"
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_is_empty",
                &["text".to_string()],
                &[Some(Vec::new())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_is_blank",
                &["text".to_string()],
                &[Some(" \t\n".as_bytes().to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function(
                "ks",
                "text_is_blank",
                &["text".to_string()],
                &[Some(b" ada ".to_vec())],
            )
            .unwrap()
            .unwrap(),
            vec![0]
        );
        let literal_length = mgr
            .execute_function("ks", "literal_length", &[], &[])
            .unwrap()
            .unwrap();
        assert_eq!(i32::from_be_bytes(literal_length.try_into().unwrap()), 2);
        assert_eq!(
            mgr.execute_function("ks", "literal_to_string", &[], &[])
                .unwrap()
                .unwrap(),
            b"node-db-1"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_intern", &[], &[])
                .unwrap()
                .unwrap(),
            b"node-db-1"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_lower", &[], &[])
                .unwrap()
                .unwrap(),
            b"ada"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_upper", &[], &[])
                .unwrap()
                .unwrap(),
            "ÅDA".as_bytes()
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_trim", &[], &[])
                .unwrap()
                .unwrap(),
            b"ada"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_strip", &[], &[])
                .unwrap()
                .unwrap(),
            b"ada"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_strip_leading", &[], &[])
                .unwrap()
                .unwrap(),
            b"ada  "
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_strip_trailing", &[], &[])
                .unwrap()
                .unwrap(),
            b"  ada"
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_is_empty", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mgr.execute_function("ks", "literal_is_blank", &[], &[])
                .unwrap()
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn java_unsafe_body_still_rejected() {
        let mut mgr = UdfManager::new();
        assert!(
            mgr.register_function(UdfDefinition {
                name: "unsafe_body".to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string()],
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: "System.exit(1);".to_string(),
                called_on_null_input: false,
            })
            .is_err()
        );
    }
}
