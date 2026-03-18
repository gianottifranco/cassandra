// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

//! Cassandra Stage model for observable concurrency.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.concurrent.Stage`

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cassandra thread-pool stages matching the Java `Stage` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Read,
    Mutation,
    CounterMutation,
    ViewMutation,
    Gossip,
    RequestResponse,
    AntiEntropy,
    MigrationStage,
    Misc,
    Tracing,
    InternalResponse,
    Immediate,
    ReadRepair,
    NativeTransport,
    AuthGroup,
}

static ALL_STAGES: [Stage; 15] = [
    Stage::Read,
    Stage::Mutation,
    Stage::CounterMutation,
    Stage::ViewMutation,
    Stage::Gossip,
    Stage::RequestResponse,
    Stage::AntiEntropy,
    Stage::MigrationStage,
    Stage::Misc,
    Stage::Tracing,
    Stage::InternalResponse,
    Stage::Immediate,
    Stage::ReadRepair,
    Stage::NativeTransport,
    Stage::AuthGroup,
];

impl Stage {
    /// Java-compatible stage name.
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Read => "ReadStage",
            Stage::Mutation => "MutationStage",
            Stage::CounterMutation => "CounterMutationStage",
            Stage::ViewMutation => "ViewMutationStage",
            Stage::Gossip => "GossipStage",
            Stage::RequestResponse => "RequestResponseStage",
            Stage::AntiEntropy => "AntiEntropyStage",
            Stage::MigrationStage => "MigrationStage",
            Stage::Misc => "MiscStage",
            Stage::Tracing => "TracingStage",
            Stage::InternalResponse => "InternalResponseStage",
            Stage::Immediate => "ImmediateStage",
            Stage::ReadRepair => "ReadRepairStage",
            Stage::NativeTransport => "Native-Transport-Requests",
            Stage::AuthGroup => "AuthGroup",
        }
    }

    /// Return all stage variants.
    pub fn all() -> &'static [Stage] {
        &ALL_STAGES
    }

    /// Index into the fixed-size metrics array.
    fn index(self) -> usize {
        self as usize
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Atomic metrics for a single stage: active, pending, and completed counts.
pub struct StageMetrics {
    active: AtomicU64,
    pending: AtomicU64,
    completed: AtomicU64,
}

impl StageMetrics {
    pub fn new() -> Self {
        Self {
            active: AtomicU64::new(0),
            pending: AtomicU64::new(0),
            completed: AtomicU64::new(0),
        }
    }

    pub fn inc_active(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_active(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_pending(&self) {
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_pending(&self) {
        self.pending.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Return a snapshot of (active, pending, completed).
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.active.load(Ordering::Relaxed),
            self.pending.load(Ordering::Relaxed),
            self.completed.load(Ordering::Relaxed),
        )
    }
}

impl Default for StageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry holding one `StageMetrics` per `Stage` variant.
pub struct StageRegistry {
    metrics: Vec<StageMetrics>,
}

impl StageRegistry {
    pub fn new() -> Self {
        let metrics = (0..ALL_STAGES.len()).map(|_| StageMetrics::new()).collect();
        Self { metrics }
    }

    /// Get the metrics for a given stage.
    pub fn get(&self, stage: Stage) -> &StageMetrics {
        &self.metrics[stage.index()]
    }

    /// Snapshot all stages: returns `(Stage, active, pending, completed)` tuples.
    pub fn snapshot_all(&self) -> Vec<(Stage, u64, u64, u64)> {
        ALL_STAGES
            .iter()
            .map(|&s| {
                let (a, p, c) = self.get(s).snapshot();
                (s, a, p, c)
            })
            .collect()
    }
}

impl Default for StageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_stage_names_non_empty() {
        for stage in Stage::all() {
            assert!(!stage.name().is_empty(), "{:?} has empty name", stage);
        }
    }

    #[test]
    fn all_returns_15_variants() {
        assert_eq!(Stage::all().len(), 15);
    }

    #[test]
    fn stage_metrics_inc_dec_snapshot() {
        let m = StageMetrics::new();
        assert_eq!(m.snapshot(), (0, 0, 0));
        m.inc_active();
        m.inc_active();
        m.inc_pending();
        m.inc_completed();
        m.inc_completed();
        m.inc_completed();
        m.dec_active();
        m.dec_pending();
        assert_eq!(m.snapshot(), (1, 0, 3));
    }

    #[test]
    fn registry_per_stage_metrics() {
        let reg = StageRegistry::new();
        reg.get(Stage::Read).inc_active();
        reg.get(Stage::Mutation).inc_pending();
        assert_eq!(reg.get(Stage::Read).snapshot(), (1, 0, 0));
        assert_eq!(reg.get(Stage::Mutation).snapshot(), (0, 1, 0));
        assert_eq!(reg.get(Stage::Gossip).snapshot(), (0, 0, 0));
    }

    #[test]
    fn stage_display() {
        assert_eq!(format!("{}", Stage::Read), "ReadStage");
        assert_eq!(
            format!("{}", Stage::NativeTransport),
            "Native-Transport-Requests"
        );
    }
}
