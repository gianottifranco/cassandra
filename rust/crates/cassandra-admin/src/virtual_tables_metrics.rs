// Licensed under Apache License, Version 2.0.

//! Metrics-populated virtual tables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.virtual.CqlMetricsTable`
//! - `org.apache.cassandra.db.virtual.StorageMetricsTable`
//!
//! Exposes CQL and storage metrics as virtual tables in `system_views`,
//! readable via standard CQL queries.

use std::collections::HashMap;
use std::sync::Arc;

use crate::prometheus_metrics::MetricsRegistry;
use crate::virtual_tables::{VirtualColumn, VirtualTable};

// ─── Metrics-Populated Virtual Table ─────────────────────────────────────────

/// A virtual table whose rows are populated from a [`MetricsRegistry`].
///
/// Each instance is configured with a table name and a row-builder closure
/// that extracts the relevant metrics at query time.
pub struct MetricsPopulatedCqlTable {
    table_name: String,
    metrics: Arc<MetricsRegistry>,
    column_defs: Vec<VirtualColumn>,
    row_builder: fn(&MetricsRegistry) -> Vec<HashMap<String, String>>,
}

impl MetricsPopulatedCqlTable {
    /// Create a new metrics-populated virtual table.
    pub fn new(
        name: String,
        metrics: Arc<MetricsRegistry>,
        column_defs: Vec<VirtualColumn>,
        row_builder: fn(&MetricsRegistry) -> Vec<HashMap<String, String>>,
    ) -> Self {
        Self {
            table_name: name,
            metrics,
            column_defs,
            row_builder,
        }
    }
}

impl VirtualTable for MetricsPopulatedCqlTable {
    fn keyspace(&self) -> &str {
        "system_views"
    }

    fn name(&self) -> &str {
        &self.table_name
    }

    fn columns(&self) -> Vec<VirtualColumn> {
        self.column_defs.clone()
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        (self.row_builder)(&self.metrics)
    }
}

// ─── Helper ──────────────────────────────────────────────────────────────────

fn name_value_columns() -> Vec<VirtualColumn> {
    vec![
        VirtualColumn {
            name: "name".to_string(),
            cql_type: "text".to_string(),
        },
        VirtualColumn {
            name: "value".to_string(),
            cql_type: "text".to_string(),
        },
    ]
}

fn metric_row(name: &str, value: String) -> HashMap<String, String> {
    let mut row = HashMap::new();
    row.insert("name".to_string(), name.to_string());
    row.insert("value".to_string(), value);
    row
}

// ─── Factory Functions ───────────────────────────────────────────────────────

/// Create the `cql_metrics` virtual table.
///
/// Exposes: read_count, write_count, tombstone_scanned, key_cache_hit_rate.
pub fn create_cql_metrics_table(metrics: Arc<MetricsRegistry>) -> Box<dyn VirtualTable> {
    Box::new(MetricsPopulatedCqlTable::new(
        "cql_metrics".to_string(),
        metrics,
        name_value_columns(),
        |m| {
            vec![
                metric_row("read_count", m.read_count.get().to_string()),
                metric_row("write_count", m.write_count.get().to_string()),
                metric_row("tombstone_scanned", m.tombstone_scanned.get().to_string()),
                metric_row("key_cache_hit_rate", m.key_cache_hit_rate.get().to_string()),
            ]
        },
    ))
}

/// Create the `storage_metrics` virtual table.
///
/// Exposes: live_sstable_count, pending_compactions, storage_load_bytes,
/// connected_native_clients.
pub fn create_storage_metrics_table(metrics: Arc<MetricsRegistry>) -> Box<dyn VirtualTable> {
    Box::new(MetricsPopulatedCqlTable::new(
        "storage_metrics".to_string(),
        metrics,
        name_value_columns(),
        |m| {
            vec![
                metric_row("live_sstable_count", m.live_sstable_count.get().to_string()),
                metric_row(
                    "pending_compactions",
                    m.pending_compactions.get().to_string(),
                ),
                metric_row(
                    "storage_load_bytes",
                    m.storage_load_bytes.get().to_string(),
                ),
                metric_row(
                    "connected_native_clients",
                    m.connected_native_clients.get().to_string(),
                ),
            ]
        },
    ))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry() -> Arc<MetricsRegistry> {
        Arc::new(MetricsRegistry::new())
    }

    #[test]
    fn cql_metrics_table_metadata() {
        let table = create_cql_metrics_table(test_registry());
        assert_eq!(table.keyspace(), "system_views");
        assert_eq!(table.name(), "cql_metrics");

        let cols = table.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "name");
        assert_eq!(cols[0].cql_type, "text");
        assert_eq!(cols[1].name, "value");
        assert_eq!(cols[1].cql_type, "text");
    }

    #[test]
    fn cql_metrics_table_rows_reflect_registry() {
        let metrics = test_registry();
        metrics.read_count.inc();
        metrics.read_count.inc();
        metrics.write_count.inc();
        metrics.tombstone_scanned.inc();
        metrics.key_cache_hit_rate.set(0.85);

        let table = create_cql_metrics_table(Arc::clone(&metrics));
        let rows = table.rows();

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["name"], "read_count");
        assert_eq!(rows[0]["value"], "2");
        assert_eq!(rows[1]["name"], "write_count");
        assert_eq!(rows[1]["value"], "1");
        assert_eq!(rows[2]["name"], "tombstone_scanned");
        assert_eq!(rows[2]["value"], "1");
        assert_eq!(rows[3]["name"], "key_cache_hit_rate");
        assert_eq!(rows[3]["value"], "0.85");
    }

    #[test]
    fn storage_metrics_table_metadata() {
        let table = create_storage_metrics_table(test_registry());
        assert_eq!(table.keyspace(), "system_views");
        assert_eq!(table.name(), "storage_metrics");
        assert_eq!(table.columns().len(), 2);
    }

    #[test]
    fn storage_metrics_table_rows_reflect_registry() {
        let metrics = test_registry();
        metrics.live_sstable_count.set(42);
        metrics.pending_compactions.set(3);
        metrics.storage_load_bytes.set(1_000_000);
        metrics.connected_native_clients.set(10);

        let table = create_storage_metrics_table(Arc::clone(&metrics));
        let rows = table.rows();

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["name"], "live_sstable_count");
        assert_eq!(rows[0]["value"], "42");
        assert_eq!(rows[1]["name"], "pending_compactions");
        assert_eq!(rows[1]["value"], "3");
        assert_eq!(rows[2]["name"], "storage_load_bytes");
        assert_eq!(rows[2]["value"], "1000000");
        assert_eq!(rows[3]["name"], "connected_native_clients");
        assert_eq!(rows[3]["value"], "10");
    }

    #[test]
    fn rows_update_when_metrics_change() {
        let metrics = test_registry();
        let table = create_cql_metrics_table(Arc::clone(&metrics));

        let rows_before = table.rows();
        assert_eq!(rows_before[0]["value"], "0");

        metrics.read_count.inc();
        let rows_after = table.rows();
        assert_eq!(rows_after[0]["value"], "1");
    }
}
