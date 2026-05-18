// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

//! Cassandra Stage model for observable concurrency.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.concurrent.Stage`

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{self, Receiver, Sender};

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

type StageTask = Box<dyn FnOnce() + Send + 'static>;

enum StageMessage {
    Run(StageTask),
    Shutdown,
}

/// Fixed-size executor for one Cassandra stage.
pub struct StageExecutor {
    stage: Stage,
    metrics: Arc<StageRegistry>,
    sender: Sender<StageMessage>,
    workers: Vec<JoinHandle<()>>,
}

impl StageExecutor {
    pub fn new(stage: Stage, threads: usize, metrics: Arc<StageRegistry>) -> Self {
        let threads = threads.max(1);
        let (sender, receiver) = channel::unbounded();
        let workers = (0..threads)
            .map(|idx| spawn_worker(stage, idx, Arc::clone(&metrics), receiver.clone()))
            .collect();
        Self {
            stage,
            metrics,
            sender,
            workers,
        }
    }

    pub fn stage(&self) -> Stage {
        self.stage
    }

    pub fn metrics(&self) -> &Arc<StageRegistry> {
        &self.metrics
    }

    pub fn submit<F>(&self, task: F) -> Result<(), StageSubmitError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.metrics.get(self.stage).inc_pending();
        match self.sender.send(StageMessage::Run(Box::new(task))) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.metrics.get(self.stage).dec_pending();
                Err(StageSubmitError::Closed)
            }
        }
    }
}

impl Drop for StageExecutor {
    fn drop(&mut self) {
        for _ in &self.workers {
            let _ = self.sender.send(StageMessage::Shutdown);
        }
        while let Some(worker) = self.workers.pop() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageSubmitError {
    Closed,
}

impl fmt::Display for StageSubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageSubmitError::Closed => f.write_str("stage executor is closed"),
        }
    }
}

impl std::error::Error for StageSubmitError {}

fn spawn_worker(
    stage: Stage,
    idx: usize,
    metrics: Arc<StageRegistry>,
    receiver: Receiver<StageMessage>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("{}-{}", stage.name(), idx))
        .spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    StageMessage::Run(task) => {
                        let stage_metrics = metrics.get(stage);
                        stage_metrics.dec_pending();
                        stage_metrics.inc_active();
                        task();
                        stage_metrics.dec_active();
                        stage_metrics.inc_completed();
                    }
                    StageMessage::Shutdown => break,
                }
            }
        })
        .expect("stage worker thread should spawn")
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

    #[test]
    fn stage_executor_runs_tasks_and_updates_metrics() {
        let registry = Arc::new(StageRegistry::new());
        let executor = StageExecutor::new(Stage::Read, 2, Arc::clone(&registry));
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..4 {
            let tx = tx.clone();
            executor.submit(move || tx.send(i).unwrap()).unwrap();
        }
        drop(tx);

        let mut values = rx.iter().collect::<Vec<_>>();
        values.sort();
        assert_eq!(values, vec![0, 1, 2, 3]);
        drop(executor);
        assert_eq!(registry.get(Stage::Read).snapshot(), (0, 0, 4));
    }
}
