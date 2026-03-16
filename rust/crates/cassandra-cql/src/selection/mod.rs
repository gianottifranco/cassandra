// Licensed under Apache License, Version 2.0.

//! Selection engine: selector evaluation, aggregation, and post-processing.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.selection.Selection`
//! - `org.apache.cassandra.cql3.selection.Selector`

pub mod aggregation;
pub mod post_process;
pub mod selector_eval;

pub use selector_eval::SelectorEvaluator;
