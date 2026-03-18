// Licensed under Apache License, Version 2.0.

//! Virtual table data population for the operations group.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.virtual.SSTableTasksTable`
//! - `org.apache.cassandra.db.virtual.InternodeOutboundTable` (operations view)
//!
//! Provides `LiveOperationsTable` (backed by `OperationTracker`) and
//! `SstableTasksPopulatedTable` (backed by an `SstableTaskProvider`).

use std::collections::HashMap;
use std::sync::Arc;

use crate::operations::OperationTracker;
use crate::virtual_tables::{VirtualColumn, VirtualTable};

// ─── SSTable Task Provider ───────────────────────────────────────────────────

/// Information about a single SSTable background task.
#[derive(Debug, Clone)]
pub struct SstableTaskInfo {
    pub task_id: String,
    pub keyspace: String,
    pub table: String,
    pub task_type: String, // "compaction", "cleanup", "scrub"
    pub progress: u32,     // 0-100
}

/// Provides a snapshot of active SSTable tasks (compaction, cleanup, scrub).
pub trait SstableTaskProvider: Send + Sync {
    fn active_tasks(&self) -> Vec<SstableTaskInfo>;
}

// ─── LiveOperationsTable ─────────────────────────────────────────────────────

/// Virtual table exposing live topology / repair / streaming operations.
pub struct LiveOperationsTable {
    tracker: Arc<OperationTracker>,
}

impl LiveOperationsTable {
    pub fn new(tracker: Arc<OperationTracker>) -> Self {
        Self { tracker }
    }
}

impl VirtualTable for LiveOperationsTable {
    fn keyspace(&self) -> &str {
        "system_views"
    }

    fn name(&self) -> &str {
        "operations"
    }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn {
                name: "id".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "type".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "status".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "progress".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "description".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "elapsed_secs".to_string(),
                cql_type: "text".to_string(),
            },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.tracker
            .list_operations()
            .into_iter()
            .map(|op| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), op.id.to_string());
                row.insert("type".to_string(), op.operation_type.to_string());
                row.insert("status".to_string(), op.status);
                row.insert("progress".to_string(), op.progress.to_string());
                row.insert("description".to_string(), op.description);
                row.insert("elapsed_secs".to_string(), op.elapsed_secs.to_string());
                row
            })
            .collect()
    }
}

// ─── SstableTasksPopulatedTable ──────────────────────────────────────────────

/// Virtual table exposing active SSTable background tasks.
pub struct SstableTasksPopulatedTable {
    provider: Arc<dyn SstableTaskProvider>,
}

impl SstableTasksPopulatedTable {
    pub fn new(provider: Arc<dyn SstableTaskProvider>) -> Self {
        Self { provider }
    }
}

impl VirtualTable for SstableTasksPopulatedTable {
    fn keyspace(&self) -> &str {
        "system_views"
    }

    fn name(&self) -> &str {
        "sstable_tasks"
    }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn {
                name: "task_id".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "keyspace".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "table".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "task_type".to_string(),
                cql_type: "text".to_string(),
            },
            VirtualColumn {
                name: "progress".to_string(),
                cql_type: "text".to_string(),
            },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.provider
            .active_tasks()
            .into_iter()
            .map(|task| {
                let mut row = HashMap::new();
                row.insert("task_id".to_string(), task.task_id);
                row.insert("keyspace".to_string(), task.keyspace);
                row.insert("table".to_string(), task.table);
                row.insert("task_type".to_string(), task.task_type);
                row.insert("progress".to_string(), task.progress.to_string());
                row
            })
            .collect()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::OperationType;

    struct MockTaskProvider {
        tasks: Vec<SstableTaskInfo>,
    }

    impl SstableTaskProvider for MockTaskProvider {
        fn active_tasks(&self) -> Vec<SstableTaskInfo> {
            self.tasks.clone()
        }
    }

    #[test]
    fn live_operations_table_metadata() {
        let tracker = Arc::new(OperationTracker::new());
        let table = LiveOperationsTable::new(tracker);

        assert_eq!(table.keyspace(), "system_views");
        assert_eq!(table.name(), "operations");
        assert_eq!(table.columns().len(), 6);
    }

    #[test]
    fn live_operations_table_rows_from_tracker() {
        let tracker = Arc::new(OperationTracker::new());
        let _id = tracker.register(OperationType::Repair, "repair keyspace ks1");
        tracker.register(OperationType::Bootstrap, "bootstrap node 5");

        let table = LiveOperationsTable::new(Arc::clone(&tracker));
        let rows = table.rows();

        assert_eq!(rows.len(), 2);
        // All rows must have the expected columns.
        for row in &rows {
            assert!(row.contains_key("id"));
            assert!(row.contains_key("type"));
            assert!(row.contains_key("status"));
            assert!(row.contains_key("progress"));
            assert!(row.contains_key("description"));
            assert!(row.contains_key("elapsed_secs"));
        }
    }

    #[test]
    fn live_operations_table_empty_tracker() {
        let tracker = Arc::new(OperationTracker::new());
        let table = LiveOperationsTable::new(tracker);
        assert!(table.rows().is_empty());
    }

    #[test]
    fn sstable_tasks_table_metadata() {
        let provider = Arc::new(MockTaskProvider { tasks: vec![] });
        let table = SstableTasksPopulatedTable::new(provider);

        assert_eq!(table.keyspace(), "system_views");
        assert_eq!(table.name(), "sstable_tasks");
        assert_eq!(table.columns().len(), 5);
    }

    #[test]
    fn sstable_tasks_table_rows() {
        let provider = Arc::new(MockTaskProvider {
            tasks: vec![
                SstableTaskInfo {
                    task_id: "t1".to_string(),
                    keyspace: "ks1".to_string(),
                    table: "cf1".to_string(),
                    task_type: "compaction".to_string(),
                    progress: 42,
                },
                SstableTaskInfo {
                    task_id: "t2".to_string(),
                    keyspace: "ks2".to_string(),
                    table: "cf2".to_string(),
                    task_type: "cleanup".to_string(),
                    progress: 100,
                },
            ],
        });

        let table = SstableTasksPopulatedTable::new(provider);
        let rows = table.rows();

        assert_eq!(rows.len(), 2);

        let t1 = rows.iter().find(|r| r["task_id"] == "t1").unwrap();
        assert_eq!(t1["keyspace"], "ks1");
        assert_eq!(t1["table"], "cf1");
        assert_eq!(t1["task_type"], "compaction");
        assert_eq!(t1["progress"], "42");

        let t2 = rows.iter().find(|r| r["task_id"] == "t2").unwrap();
        assert_eq!(t2["progress"], "100");
    }

    #[test]
    fn sstable_tasks_table_empty_provider() {
        let provider = Arc::new(MockTaskProvider { tasks: vec![] });
        let table = SstableTasksPopulatedTable::new(provider);
        assert!(table.rows().is_empty());
    }
}
