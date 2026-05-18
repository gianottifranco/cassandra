// Licensed under Apache License, Version 2.0.

//! Compaction controller — strategy wrapper that mediates between the chosen
//! compaction strategy and task creation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionStrategyManager`
//! - `org.apache.cassandra.db.compaction.AbstractCompactionTask`

use crate::compaction::errors::{CompactionReason, CompactionType};
use std::collections::HashMap;

use crate::compaction::{
    CompactionStrategy, CompactionStrategyType, SSTableMetadata, create_strategy,
    create_strategy_with_options,
};
use crate::sstable::format::SSTableId;

// ─── TaskPriority ───────────────────────────────────────────────────────────

/// Priority of a compaction task, used for scheduling order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low,
    Normal,
    High,
    Urgent,
}

// ─── TaskSpec ───────────────────────────────────────────────────────────────

/// A compaction task specification produced by the controller for the executor
/// to run.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    /// SSTable IDs that participate in this compaction.
    pub sstable_ids: Vec<SSTableId>,
    /// The kind of compaction operation.
    pub compaction_type: CompactionType,
    /// Why this compaction was triggered.
    pub reason: CompactionReason,
    /// Scheduling priority.
    pub priority: TaskPriority,
}

// ─── CompactionController ───────────────────────────────────────────────────

/// Wraps a [`CompactionStrategy`] and translates its output into executable
/// [`TaskSpec`] values.
pub struct CompactionController {
    strategy: Box<dyn CompactionStrategy>,
    strategy_type: CompactionStrategyType,
}

impl CompactionController {
    /// Create a new controller for the given strategy type.
    pub fn new(strategy_type: CompactionStrategyType) -> Self {
        Self {
            strategy: create_strategy(strategy_type),
            strategy_type,
        }
    }

    /// Create a controller for the given strategy type and Java option map.
    pub fn with_options(
        strategy_type: CompactionStrategyType,
        options: &HashMap<String, String>,
    ) -> Result<Self, String> {
        Ok(Self {
            strategy: create_strategy_with_options(strategy_type, options)?,
            strategy_type,
        })
    }

    /// Ask the strategy for the next set of background compaction tasks.
    ///
    /// The first group gets [`TaskPriority::High`]; subsequent groups get
    /// [`TaskPriority::Normal`].
    pub fn get_next_background_tasks(&self, sstables: &[SSTableMetadata]) -> Vec<TaskSpec> {
        let groups = self.strategy.pick_compaction(sstables);

        groups
            .into_iter()
            .enumerate()
            .map(|(idx, ids)| TaskSpec {
                sstable_ids: ids,
                compaction_type: CompactionType::Compaction,
                reason: CompactionReason::Normal,
                priority: if idx == 0 {
                    TaskPriority::High
                } else {
                    TaskPriority::Normal
                },
            })
            .collect()
    }

    /// Return the strategy type in use.
    pub fn strategy_type(&self) -> CompactionStrategyType {
        self.strategy_type
    }

    /// Create a user-defined (manual) compaction task with [`TaskPriority::Urgent`].
    pub fn create_user_defined_task(&self, sstable_ids: Vec<SSTableId>) -> TaskSpec {
        TaskSpec {
            sstable_ids,
            compaction_type: CompactionType::Compaction,
            reason: CompactionReason::UserDefined,
            priority: TaskPriority::Urgent,
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: u64, size: u64) -> SSTableMetadata {
        SSTableMetadata {
            id,
            data_size: size,
            partition_count: 100,
            min_timestamp: 0,
            max_timestamp: 1000,
        }
    }

    // ── STCS picks correct groups and wraps in TaskSpecs ────────────────

    #[test]
    fn stcs_picks_correct_groups() {
        let ctrl = CompactionController::new(CompactionStrategyType::SizeTiered);

        // Create enough similarly-sized SSTables to trigger STCS (default min_threshold=4).
        let sstables: Vec<SSTableMetadata> = (1..=5).map(|id| make_meta(id, 100)).collect();

        let tasks = ctrl.get_next_background_tasks(&sstables);
        assert!(!tasks.is_empty(), "STCS should produce at least one task");

        let first = &tasks[0];
        assert_eq!(first.compaction_type, CompactionType::Compaction);
        assert_eq!(first.reason, CompactionReason::Normal);
        assert_eq!(first.priority, TaskPriority::High);
        assert!(
            first.sstable_ids.len() >= 4,
            "first group should contain at least min_threshold SSTables"
        );
    }

    // ── Empty input returns empty ───────────────────────────────────────

    #[test]
    fn empty_input_returns_empty() {
        let ctrl = CompactionController::new(CompactionStrategyType::SizeTiered);
        let tasks = ctrl.get_next_background_tasks(&[]);
        assert!(tasks.is_empty());
    }

    // ── User-defined task has correct priority and reason ───────────────

    #[test]
    fn user_defined_task_has_correct_priority_and_reason() {
        let ctrl = CompactionController::new(CompactionStrategyType::SizeTiered);
        let task = ctrl.create_user_defined_task(vec![10, 20, 30]);

        assert_eq!(task.compaction_type, CompactionType::Compaction);
        assert_eq!(task.reason, CompactionReason::UserDefined);
        assert_eq!(task.priority, TaskPriority::Urgent);
        assert_eq!(task.sstable_ids, vec![10, 20, 30]);
    }

    #[test]
    fn option_aware_controller_uses_java_thresholds() {
        let options = HashMap::from([("min_threshold".to_string(), "2".to_string())]);
        let ctrl = CompactionController::with_options(CompactionStrategyType::SizeTiered, &options)
            .unwrap();
        let tasks = ctrl.get_next_background_tasks(&[make_meta(1, 100), make_meta(2, 110)]);

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].sstable_ids, vec![1, 2]);
    }

    // ── LCS returns leveled tasks ───────────────────────────────────────

    #[test]
    fn lcs_returns_leveled_tasks() {
        let ctrl = CompactionController::new(CompactionStrategyType::Leveled);

        // LCS via the trait interface treats everything as L0 and triggers when
        // count >= l0_threshold (default 4).
        let sstables: Vec<SSTableMetadata> = (1..=5).map(|id| make_meta(id, 100)).collect();

        let tasks = ctrl.get_next_background_tasks(&sstables);
        assert!(!tasks.is_empty(), "LCS should produce at least one task");

        let first = &tasks[0];
        assert_eq!(first.compaction_type, CompactionType::Compaction);
        assert_eq!(first.priority, TaskPriority::High);
        // LCS via trait compacts all L0 SSTables together.
        assert_eq!(first.sstable_ids.len(), 5);
    }

    // ── Strategy type accessor ──────────────────────────────────────────

    #[test]
    fn strategy_type_accessor() {
        let ctrl = CompactionController::new(CompactionStrategyType::TimeWindow);
        assert_eq!(ctrl.strategy_type(), CompactionStrategyType::TimeWindow);
    }

    // ── Multiple groups: first is High, rest are Normal ─────────────────

    #[test]
    fn multiple_groups_priority_ordering() {
        // Use a mock strategy that returns two groups.
        struct TwoGroupStrategy;
        impl CompactionStrategy for TwoGroupStrategy {
            fn pick_compaction(&self, _sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
                vec![vec![1, 2], vec![3, 4]]
            }
        }

        let ctrl = CompactionController {
            strategy: Box::new(TwoGroupStrategy),
            strategy_type: CompactionStrategyType::SizeTiered,
        };

        let tasks = ctrl.get_next_background_tasks(&[]);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].priority, TaskPriority::High);
        assert_eq!(tasks[1].priority, TaskPriority::Normal);
    }
}
