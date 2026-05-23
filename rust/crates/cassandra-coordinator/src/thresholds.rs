// Licensed under Apache License, Version 2.0.

//! Coordinator-local request threshold tracking.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.thresholds.CoordinatorWarnings`
//! - `org.apache.cassandra.service.thresholds.LocalReadSizeWarning`
//! - `org.apache.cassandra.service.thresholds.LocalWriteTimeWarning`

use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Coordinator operation classes tracked by local threshold warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorOperation {
    Read,
    RangeRead,
    Write,
    Cas,
}

impl CoordinatorOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::RangeRead => "range_read",
            Self::Write => "write",
            Self::Cas => "cas",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::RangeRead => 1,
            Self::Write => 2,
            Self::Cas => 3,
        }
    }
}

/// Threshold configuration for coordinator-local timings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorThresholdConfig {
    pub read_warn_after: Duration,
    pub read_fail_after: Option<Duration>,
    pub write_warn_after: Duration,
    pub write_fail_after: Option<Duration>,
    pub max_recent_events: usize,
}

impl Default for CoordinatorThresholdConfig {
    fn default() -> Self {
        Self {
            read_warn_after: Duration::from_millis(500),
            read_fail_after: None,
            write_warn_after: Duration::from_millis(500),
            write_fail_after: None,
            max_recent_events: 128,
        }
    }
}

impl CoordinatorThresholdConfig {
    pub fn thresholds_for(&self, operation: CoordinatorOperation) -> OperationThresholds {
        match operation {
            CoordinatorOperation::Read | CoordinatorOperation::RangeRead => OperationThresholds {
                warn_after: self.read_warn_after,
                fail_after: self.read_fail_after,
            },
            CoordinatorOperation::Write | CoordinatorOperation::Cas => OperationThresholds {
                warn_after: self.write_warn_after,
                fail_after: self.write_fail_after,
            },
        }
    }
}

/// Resolved thresholds for one operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationThresholds {
    pub warn_after: Duration,
    pub fail_after: Option<Duration>,
}

/// Classification returned for an observed coordinator operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdOutcome {
    Within,
    Warn,
    Fail,
}

impl ThresholdOutcome {
    pub fn exceeded(self) -> bool {
        !matches!(self, Self::Within)
    }
}

/// A recent threshold event retained for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThresholdEvent {
    pub operation: CoordinatorOperation,
    pub latency_us: u64,
    pub threshold_us: u64,
    pub outcome: ThresholdOutcome,
    pub occurred_at_ms: u64,
}

/// Immutable metrics snapshot for an operation class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationThresholdSnapshot {
    pub operation: CoordinatorOperation,
    pub total: u64,
    pub warned: u64,
    pub failed: u64,
    pub total_latency_us: u64,
    pub max_latency_us: u64,
    pub recent_events: Vec<ThresholdEvent>,
}

impl OperationThresholdSnapshot {
    pub fn average_latency_us(&self) -> Option<u64> {
        (self.total > 0).then_some(self.total_latency_us / self.total)
    }
}

/// Thread-safe coordinator threshold tracker.
#[derive(Debug)]
pub struct CoordinatorThresholdTracker {
    config: CoordinatorThresholdConfig,
    states: [OperationState; 4],
}

impl CoordinatorThresholdTracker {
    pub fn new(config: CoordinatorThresholdConfig) -> Self {
        Self {
            config,
            states: std::array::from_fn(|_| OperationState::new()),
        }
    }

    pub fn config(&self) -> &CoordinatorThresholdConfig {
        &self.config
    }

    /// Record an operation latency and return its threshold classification.
    pub fn record(&self, operation: CoordinatorOperation, latency: Duration) -> ThresholdOutcome {
        self.record_at(operation, latency, now_millis())
    }

    /// Record with an explicit timestamp, useful for deterministic tests.
    pub fn record_at(
        &self,
        operation: CoordinatorOperation,
        latency: Duration,
        occurred_at_ms: u64,
    ) -> ThresholdOutcome {
        let thresholds = self.config.thresholds_for(operation);
        let latency_us = duration_micros_u64(latency);
        let warn_us = duration_micros_u64(thresholds.warn_after);
        let fail_us = thresholds.fail_after.map(duration_micros_u64);

        let outcome = if fail_us.is_some_and(|threshold| latency_us >= threshold) {
            ThresholdOutcome::Fail
        } else if latency_us >= warn_us {
            ThresholdOutcome::Warn
        } else {
            ThresholdOutcome::Within
        };

        self.state(operation).record(
            operation,
            latency_us,
            warn_us,
            outcome,
            occurred_at_ms,
            self.config.max_recent_events,
        );
        outcome
    }

    pub fn snapshot(&self, operation: CoordinatorOperation) -> OperationThresholdSnapshot {
        self.state(operation).snapshot(operation)
    }

    pub fn snapshots(&self) -> Vec<OperationThresholdSnapshot> {
        [
            CoordinatorOperation::Read,
            CoordinatorOperation::RangeRead,
            CoordinatorOperation::Write,
            CoordinatorOperation::Cas,
        ]
        .into_iter()
        .map(|operation| self.snapshot(operation))
        .collect()
    }

    fn state(&self, operation: CoordinatorOperation) -> &OperationState {
        &self.states[operation.index()]
    }
}

impl Default for CoordinatorThresholdTracker {
    fn default() -> Self {
        Self::new(CoordinatorThresholdConfig::default())
    }
}

#[derive(Debug)]
struct OperationState {
    total: AtomicU64,
    warned: AtomicU64,
    failed: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    recent_events: Mutex<VecDeque<ThresholdEvent>>,
}

impl OperationState {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            warned: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            recent_events: Mutex::new(VecDeque::new()),
        }
    }

    fn record(
        &self,
        operation: CoordinatorOperation,
        latency_us: u64,
        threshold_us: u64,
        outcome: ThresholdOutcome,
        occurred_at_ms: u64,
        max_recent_events: usize,
    ) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.max_latency_us.fetch_max(latency_us, Ordering::Relaxed);

        match outcome {
            ThresholdOutcome::Within => {}
            ThresholdOutcome::Warn => {
                self.warned.fetch_add(1, Ordering::Relaxed);
                self.push_event(
                    ThresholdEvent {
                        operation,
                        latency_us,
                        threshold_us,
                        outcome,
                        occurred_at_ms,
                    },
                    max_recent_events,
                );
            }
            ThresholdOutcome::Fail => {
                self.warned.fetch_add(1, Ordering::Relaxed);
                self.failed.fetch_add(1, Ordering::Relaxed);
                self.push_event(
                    ThresholdEvent {
                        operation,
                        latency_us,
                        threshold_us,
                        outcome,
                        occurred_at_ms,
                    },
                    max_recent_events,
                );
            }
        }
    }

    fn push_event(&self, event: ThresholdEvent, max_recent_events: usize) {
        if max_recent_events == 0 {
            return;
        }
        let mut events = self
            .recent_events
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        events.push_back(event);
        while events.len() > max_recent_events {
            events.pop_front();
        }
    }

    fn snapshot(&self, operation: CoordinatorOperation) -> OperationThresholdSnapshot {
        let recent_events = self
            .recent_events
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .cloned()
            .collect();
        OperationThresholdSnapshot {
            operation,
            total: self.total.load(Ordering::Relaxed),
            warned: self.warned.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            total_latency_us: self.total_latency_us.load(Ordering::Relaxed),
            max_latency_us: self.max_latency_us.load(Ordering::Relaxed),
            recent_events,
        }
    }
}

fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tracker() -> CoordinatorThresholdTracker {
        CoordinatorThresholdTracker::new(CoordinatorThresholdConfig {
            read_warn_after: Duration::from_millis(10),
            read_fail_after: Some(Duration::from_millis(20)),
            write_warn_after: Duration::from_millis(30),
            write_fail_after: Some(Duration::from_millis(60)),
            max_recent_events: 2,
        })
    }

    #[test]
    fn classifies_read_thresholds() {
        let tracker = test_tracker();

        assert_eq!(
            tracker.record_at(CoordinatorOperation::Read, Duration::from_millis(9), 1),
            ThresholdOutcome::Within
        );
        assert_eq!(
            tracker.record_at(CoordinatorOperation::Read, Duration::from_millis(10), 2),
            ThresholdOutcome::Warn
        );
        assert_eq!(
            tracker.record_at(CoordinatorOperation::Read, Duration::from_millis(20), 3),
            ThresholdOutcome::Fail
        );

        let snapshot = tracker.snapshot(CoordinatorOperation::Read);
        assert_eq!(snapshot.total, 3);
        assert_eq!(snapshot.warned, 2);
        assert_eq!(snapshot.failed, 1);
        assert_eq!(snapshot.max_latency_us, 20_000);
        assert_eq!(snapshot.average_latency_us(), Some(13_000));
        assert_eq!(snapshot.recent_events.len(), 2);
    }

    #[test]
    fn uses_write_thresholds_for_writes_and_cas() {
        let tracker = test_tracker();

        assert_eq!(
            tracker.record_at(CoordinatorOperation::Write, Duration::from_millis(20), 1),
            ThresholdOutcome::Within
        );
        assert_eq!(
            tracker.record_at(CoordinatorOperation::Write, Duration::from_millis(30), 2),
            ThresholdOutcome::Warn
        );
        assert_eq!(
            tracker.record_at(CoordinatorOperation::Cas, Duration::from_millis(60), 3),
            ThresholdOutcome::Fail
        );

        assert_eq!(tracker.snapshot(CoordinatorOperation::Write).warned, 1);
        assert_eq!(tracker.snapshot(CoordinatorOperation::Cas).failed, 1);
    }

    #[test]
    fn caps_recent_events() {
        let tracker = test_tracker();
        tracker.record_at(
            CoordinatorOperation::RangeRead,
            Duration::from_millis(11),
            1,
        );
        tracker.record_at(
            CoordinatorOperation::RangeRead,
            Duration::from_millis(12),
            2,
        );
        tracker.record_at(
            CoordinatorOperation::RangeRead,
            Duration::from_millis(13),
            3,
        );

        let events = tracker
            .snapshot(CoordinatorOperation::RangeRead)
            .recent_events;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].occurred_at_ms, 2);
        assert_eq!(events[1].occurred_at_ms, 3);
    }

    #[test]
    fn can_disable_recent_events() {
        let tracker = CoordinatorThresholdTracker::new(CoordinatorThresholdConfig {
            max_recent_events: 0,
            ..CoordinatorThresholdConfig::default()
        });
        assert!(
            tracker
                .record_at(CoordinatorOperation::Read, Duration::from_secs(1), 1)
                .exceeded()
        );
        assert!(
            tracker
                .snapshot(CoordinatorOperation::Read)
                .recent_events
                .is_empty()
        );
    }
}
