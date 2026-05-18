// Licensed under Apache License, Version 2.0.

//! # Stream Coordinator
//!
//! Takes a [`StreamPlan`] and drives all sessions to completion.
//! Returns [`StreamResultFuture`] handles for monitoring progress.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamCoordinator`
//! - `org.apache.cassandra.streaming.StreamResultFuture`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::manager::StreamManager;
use crate::metrics::StreamingMetrics;
use crate::plan::StreamPlan;
use crate::protocol::{StreamCompleteMessage, StreamInitMessage};
use crate::sender::StreamSender;
use crate::session::StreamSessionState;
use crate::transport::StreamTransport;

/// Result of coordinating all streaming sessions.
#[derive(Debug, Clone)]
pub struct CoordinatorResult {
    pub sessions_completed: u32,
    pub sessions_failed: u32,
    pub bytes_transferred: u64,
    pub duration: Duration,
    pub errors: Vec<String>,
}

/// A future that monitors a single streaming session's progress.
pub struct StreamResultFuture {
    pub session_id: uuid::Uuid,
    rx: watch::Receiver<StreamSessionState>,
}

impl StreamResultFuture {
    /// Create a new result future with a watch channel.
    pub fn new(session_id: uuid::Uuid) -> (Self, watch::Sender<StreamSessionState>) {
        let (tx, rx) = watch::channel(StreamSessionState::Initialized);
        (Self { session_id, rx }, tx)
    }

    /// Wait until the session reaches a terminal state.
    pub async fn await_completion(&mut self) -> StreamSessionState {
        loop {
            if self.rx.changed().await.is_err() {
                // Sender dropped — treat as complete
                return *self.rx.borrow();
            }
            let state = *self.rx.borrow();
            match state {
                StreamSessionState::Complete | StreamSessionState::Failed => {
                    return state;
                }
                _ => continue,
            }
        }
    }

    /// Current state without blocking.
    pub fn current_state(&self) -> StreamSessionState {
        *self.rx.borrow()
    }
}

/// Coordinates execution of a streaming plan across multiple sessions.
pub struct StreamCoordinator {
    _manager: Arc<StreamManager>,
    metrics: Arc<StreamingMetrics>,
    data_dir: PathBuf,
    use_compression: bool,
}

impl StreamCoordinator {
    pub fn new(
        manager: Arc<StreamManager>,
        metrics: Arc<StreamingMetrics>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            _manager: manager,
            metrics,
            data_dir: data_dir.into(),
            use_compression: true,
        }
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.use_compression = enabled;
        self
    }

    /// Execute a streaming plan, returning futures for each session.
    pub async fn execute_plan(
        &self,
        plan: StreamPlan,
        transport: Arc<StreamTransport>,
    ) -> Result<Vec<StreamResultFuture>, CoordinatorError> {
        let operation = plan.operation;
        let sessions = plan.build();
        if sessions.is_empty() {
            return Ok(vec![]);
        }

        info!(
            sessions = sessions.len(),
            operation = %operation,
            "executing stream plan"
        );

        let mut futures = Vec::with_capacity(sessions.len());

        for mut session in sessions {
            let session_id = session.id;
            let peer = session.peer.addr();
            let description = session.description.clone();

            // Build init message before registering (need access to session data)
            let init_msg = StreamInitMessage {
                session_id,
                operation,
                description: description.clone(),
                keyspaces: session
                    .outgoing
                    .values()
                    .map(|t| t.keyspace.clone())
                    .collect(),
                ranges: session
                    .outgoing
                    .values()
                    .flat_map(|t| t.ranges.iter().map(|(s, e)| (s.0, e.0)))
                    .collect(),
            };

            let accepted = match transport.send_init(peer, init_msg).await {
                Ok(resp) => resp.accepted,
                Err(e) => {
                    error!(%session_id, "StreamInit failed: {e}");
                    let (future, tx) = StreamResultFuture::new(session_id);
                    let _ = tx.send(StreamSessionState::Failed);
                    futures.push(future);
                    continue;
                }
            };

            if !accepted {
                let (future, tx) = StreamResultFuture::new(session_id);
                let _ = tx.send(StreamSessionState::Failed);
                futures.push(future);
                continue;
            }

            // Transition session state
            let _ = session.prepare();
            let _ = session.start_streaming();

            let (future, tx) = StreamResultFuture::new(session_id);
            let _ = tx.send(StreamSessionState::Streaming);

            // Spawn background task for sending
            let transport = Arc::clone(&transport);
            let metrics = Arc::clone(&self.metrics);
            let data_dir = self.data_dir.clone();
            let use_compression = self.use_compression;

            tokio::spawn(async move {
                let result = StreamSender::send_session_outgoing(
                    &transport,
                    &mut session,
                    &data_dir,
                    &metrics,
                    use_compression,
                )
                .await;

                match result {
                    Ok(()) => {
                        debug!(%session_id, "session streaming complete, sending complete");
                        let complete_msg = StreamCompleteMessage {
                            session_id,
                            success: true,
                            error: None,
                        };
                        let _ = transport.send_complete(peer, complete_msg).await;
                        let _ = tx.send(StreamSessionState::Complete);
                    }
                    Err(e) => {
                        error!(%session_id, "session streaming failed: {e}");
                        let complete_msg = StreamCompleteMessage {
                            session_id,
                            success: false,
                            error: Some(e.to_string()),
                        };
                        let _ = transport.send_complete(peer, complete_msg).await;
                        let _ = tx.send(StreamSessionState::Failed);
                    }
                }
            });

            futures.push(future);
        }

        Ok(futures)
    }

    /// Wait for all futures to complete and aggregate results.
    pub async fn await_all(futures: &mut [StreamResultFuture]) -> CoordinatorResult {
        let start = Instant::now();
        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut errors = Vec::new();

        for future in futures.iter_mut() {
            let state = future.await_completion().await;
            match state {
                StreamSessionState::Complete => completed += 1,
                StreamSessionState::Failed => {
                    failed += 1;
                    errors.push(format!("session {} failed", future.session_id));
                }
                other => {
                    failed += 1;
                    errors.push(format!(
                        "session {} ended in unexpected state: {other}",
                        future.session_id
                    ));
                }
            }
        }

        CoordinatorResult {
            sessions_completed: completed,
            sessions_failed: failed,
            bytes_transferred: 0, // Tracked by metrics
            duration: start.elapsed(),
            errors,
        }
    }
}

/// Errors from the coordinator.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("plan execution failed: {0}")]
    PlanExecution(String),

    #[error("transport error: {0}")]
    Transport(#[from] crate::transport::StreamTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn result_future_completes_on_terminal_state() {
        let (mut future, tx) = StreamResultFuture::new(uuid::Uuid::new_v4());
        assert_eq!(future.current_state(), StreamSessionState::Initialized);

        tx.send(StreamSessionState::Streaming).unwrap();
        // Not terminal yet

        tx.send(StreamSessionState::Complete).unwrap();
        let state = future.await_completion().await;
        assert_eq!(state, StreamSessionState::Complete);
    }

    #[tokio::test]
    async fn result_future_detects_failure() {
        let (mut future, tx) = StreamResultFuture::new(uuid::Uuid::new_v4());
        tx.send(StreamSessionState::Failed).unwrap();
        let state = future.await_completion().await;
        assert_eq!(state, StreamSessionState::Failed);
    }

    #[tokio::test]
    async fn result_future_handles_sender_drop() {
        let (mut future, tx) = StreamResultFuture::new(uuid::Uuid::new_v4());
        tx.send(StreamSessionState::Streaming).unwrap();
        drop(tx);
        let state = future.await_completion().await;
        assert_eq!(state, StreamSessionState::Streaming);
    }

    #[tokio::test]
    async fn await_all_aggregates_results() {
        let (mut f1, tx1) = StreamResultFuture::new(uuid::Uuid::new_v4());
        let (mut f2, tx2) = StreamResultFuture::new(uuid::Uuid::new_v4());
        let (mut f3, tx3) = StreamResultFuture::new(uuid::Uuid::new_v4());

        tx1.send(StreamSessionState::Complete).unwrap();
        tx2.send(StreamSessionState::Failed).unwrap();
        tx3.send(StreamSessionState::Complete).unwrap();

        let mut futures = vec![f1, f2, f3];
        let result = StreamCoordinator::await_all(&mut futures).await;
        assert_eq!(result.sessions_completed, 2);
        assert_eq!(result.sessions_failed, 1);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn empty_plan_produces_no_sessions() {
        let plan = StreamPlan::new(crate::plan::StreamOperation::Bootstrap);
        let sessions = plan.build();
        assert!(sessions.is_empty());
    }
}
