// Licensed under Apache License, Version 2.0.

//! # cassandra-admin
//!
//! Observability, diagnostics, and administration tools.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tools.NodeTool`
//! - `org.apache.cassandra.service.StorageService` (JMX interface)
//! - `org.apache.cassandra.metrics.*`
//! - `org.apache.cassandra.db.virtual.*`
//!
//! ## Modules
//!
//! - [`metrics`] — aggregated metrics facade (streaming, repair, topology)
//! - [`operations`] — operation tracking and management
//! - [`prometheus_metrics`] — Prometheus-native metrics registry
//! - [`virtual_tables`] — virtual tables framework and built-in system views
//! - [`http_admin`] — HTTP admin API server

pub mod diagnostics;
pub mod http_admin;
pub mod http_diagnostics;
pub mod http_tracing_audit;
pub mod metrics;
pub mod operations;
pub mod prometheus_metrics;
pub mod route_table;
pub mod virtual_tables;
pub mod virtual_tables_metrics;
pub mod virtual_tables_operations;

// Domain handler modules
pub mod handlers_cache_hints;
pub mod handlers_cluster;
pub mod handlers_compaction;
pub mod handlers_config;
pub mod handlers_logging;
pub mod handlers_snapshots;
pub mod handlers_stats;
pub mod handlers_topology;

pub use diagnostics::{DiagnosticEvent, DiagnosticEventService, DiagnosticEventType};
pub use http_admin::{AdminState, start_admin_server};
pub use metrics::AdminMetrics;
pub use operations::{OperationStatus, OperationTracker, OperationType};
pub use prometheus_metrics::MetricsRegistry;
pub use virtual_tables::{VirtualTable, VirtualTableRegistry};
