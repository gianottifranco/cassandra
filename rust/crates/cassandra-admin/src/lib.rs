// Licensed under Apache License, Version 2.0.

//! # cassandra-admin
//!
//! Observability, diagnostics, and administration tools.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tools.NodeTool`
//! - `org.apache.cassandra.service.StorageService` (JMX interface)
//!
//! ## Modules
//!
//! - [`metrics`] — aggregated metrics facade
//! - [`operations`] — operation tracking and management

pub mod metrics;
pub mod operations;

pub use metrics::AdminMetrics;
pub use operations::{OperationTracker, OperationStatus, OperationType};
