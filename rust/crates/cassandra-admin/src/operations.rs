// Licensed under Apache License, Version 2.0.

//! Operation tracking: status of in-flight topology, repair, and streaming operations.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::RwLock;
use serde::Serialize;
use uuid::Uuid;

/// Type of tracked operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OperationType {
    Bootstrap,
    Decommission,
    Replace,
    RemoveNode,
    Rebuild,
    Repair,
    Stream,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap => write!(f, "BOOTSTRAP"),
            Self::Decommission => write!(f, "DECOMMISSION"),
            Self::Replace => write!(f, "REPLACE"),
            Self::RemoveNode => write!(f, "REMOVENODE"),
            Self::Rebuild => write!(f, "REBUILD"),
            Self::Repair => write!(f, "REPAIR"),
            Self::Stream => write!(f, "STREAM"),
        }
    }
}

/// Status of a single operation.
#[derive(Debug, Clone, Serialize)]
pub struct OperationStatus {
    pub id: Uuid,
    pub operation_type: OperationType,
    pub status: String,
    pub progress: u32,
    pub description: String,
    #[serde(skip)]
    pub started_at: Instant,
    pub elapsed_secs: u64,
}

/// Tracks all in-flight operations for admin visibility.
pub struct OperationTracker {
    operations: RwLock<HashMap<Uuid, OperationEntry>>,
}

struct OperationEntry {
    operation_type: OperationType,
    status: String,
    progress: u32,
    description: String,
    started_at: Instant,
}

impl OperationTracker {
    pub fn new() -> Self {
        Self {
            operations: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new operation.
    pub fn register(
        &self,
        operation_type: OperationType,
        description: impl Into<String>,
    ) -> Uuid {
        let id = Uuid::new_v4();
        self.operations.write().insert(
            id,
            OperationEntry {
                operation_type,
                status: "STARTED".to_string(),
                progress: 0,
                description: description.into(),
                started_at: Instant::now(),
            },
        );
        id
    }

    /// Update the status and progress of an operation.
    pub fn update(&self, id: &Uuid, status: impl Into<String>, progress: u32) {
        if let Some(entry) = self.operations.write().get_mut(id) {
            entry.status = status.into();
            entry.progress = progress.min(100);
        }
    }

    /// Remove an operation (completed or failed).
    pub fn remove(&self, id: &Uuid) {
        self.operations.write().remove(id);
    }

    /// List all active operations.
    pub fn list_operations(&self) -> Vec<OperationStatus> {
        self.operations
            .read()
            .iter()
            .map(|(id, e)| OperationStatus {
                id: *id,
                operation_type: e.operation_type,
                status: e.status.clone(),
                progress: e.progress,
                description: e.description.clone(),
                started_at: e.started_at,
                elapsed_secs: e.started_at.elapsed().as_secs(),
            })
            .collect()
    }

    /// Number of active operations.
    pub fn count(&self) -> usize {
        self.operations.read().len()
    }
}

impl Default for OperationTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_list() {
        let tracker = OperationTracker::new();
        let id = tracker.register(OperationType::Bootstrap, "bootstrap node 4");

        let ops = tracker.list_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, id);
        assert_eq!(ops[0].operation_type, OperationType::Bootstrap);
        assert_eq!(ops[0].status, "STARTED");
    }

    #[test]
    fn update_progress() {
        let tracker = OperationTracker::new();
        let id = tracker.register(OperationType::Repair, "repair ks");

        tracker.update(&id, "STREAMING", 50);
        let ops = tracker.list_operations();
        assert_eq!(ops[0].progress, 50);
        assert_eq!(ops[0].status, "STREAMING");
    }

    #[test]
    fn remove_operation() {
        let tracker = OperationTracker::new();
        let id = tracker.register(OperationType::Decommission, "test");
        assert_eq!(tracker.count(), 1);

        tracker.remove(&id);
        assert_eq!(tracker.count(), 0);
    }
}
