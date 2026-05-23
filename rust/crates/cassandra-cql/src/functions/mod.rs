// Licensed under Apache License, Version 2.0.

//! Built-in CQL function infrastructure and registry.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.NativeFunction`
//! - `org.apache.cassandra.cql3.functions.FunctionResolver`

pub mod aggregates;
pub mod cluster_metadata;
pub mod collection;
pub mod decimal_math;
pub mod format;
pub mod masking;
pub mod math_json;
pub mod operation;
pub mod registry;
pub mod time_uuid;
pub mod token_cast_blob;
pub mod vector_similarity;

pub use registry::{CqlFunction, FunctionName, FunctionRegistry};
