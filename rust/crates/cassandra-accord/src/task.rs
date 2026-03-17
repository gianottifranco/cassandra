// Licensed under Apache License, Version 2.0.

//! Accord task — a unit of work with lifecycle states.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordTask`

use std::fmt;

use crate::types::TxnId;

/// States of an Accord task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Loading transaction data.
    Loading,
    /// Executing the transaction.
    Running,
    /// Persisting the result.
    Persisting,
    /// Task complete.
    Finished,
    /// Task failed.
    Failed,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Loading => write!(f, "Loading"),
            TaskState::Running => write!(f, "Running"),
            TaskState::Persisting => write!(f, "Persisting"),
            TaskState::Finished => write!(f, "Finished"),
            TaskState::Failed => write!(f, "Failed"),
        }
    }
}

/// A unit of work in the Accord executor.
pub struct AccordTask {
    pub txn_id: TxnId,
    pub state: TaskState,
    pub error: Option<String>,
}

impl AccordTask {
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            state: TaskState::Loading,
            error: None,
        }
    }

    /// Transition to Running state.
    pub fn start(&mut self) -> bool {
        if self.state == TaskState::Loading {
            self.state = TaskState::Running;
            true
        } else {
            false
        }
    }

    /// Transition to Persisting state.
    pub fn persist(&mut self) -> bool {
        if self.state == TaskState::Running {
            self.state = TaskState::Persisting;
            true
        } else {
            false
        }
    }

    /// Transition to Finished state.
    pub fn finish(&mut self) -> bool {
        if self.state == TaskState::Persisting {
            self.state = TaskState::Finished;
            true
        } else {
            false
        }
    }

    /// Mark the task as failed.
    pub fn fail(&mut self, error: String) {
        self.state = TaskState::Failed;
        self.error = Some(error);
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.state, TaskState::Finished | TaskState::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_txn_id() -> TxnId {
        TxnId::with_timestamp(100, Uuid::nil(), 0)
    }

    #[test]
    fn task_lifecycle() {
        let mut task = AccordTask::new(test_txn_id());
        assert_eq!(task.state, TaskState::Loading);
        assert!(!task.is_complete());

        assert!(task.start());
        assert_eq!(task.state, TaskState::Running);

        assert!(task.persist());
        assert_eq!(task.state, TaskState::Persisting);

        assert!(task.finish());
        assert_eq!(task.state, TaskState::Finished);
        assert!(task.is_complete());
    }

    #[test]
    fn invalid_transition_returns_false() {
        let mut task = AccordTask::new(test_txn_id());
        // Can't persist from Loading
        assert!(!task.persist());
        // Can't finish from Loading
        assert!(!task.finish());
    }

    #[test]
    fn task_failure() {
        let mut task = AccordTask::new(test_txn_id());
        task.start();
        task.fail("something went wrong".to_string());
        assert_eq!(task.state, TaskState::Failed);
        assert!(task.is_complete());
        assert_eq!(task.error.as_deref(), Some("something went wrong"));
    }
}
