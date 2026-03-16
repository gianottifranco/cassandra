// Licensed under Apache License, Version 2.0.

//! Request dispatcher with separate concurrency pools for auth and query requests.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.Dispatcher`
//! - `org.apache.cassandra.transport.CQLMessageHandler`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, error};

use crate::executor::{QueryExecutor, QueryResult};

/// Categorisation of incoming requests for pool routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    /// Authentication-related requests (AUTH_RESPONSE).
    Auth,
    /// Regular query/prepare/execute/batch requests.
    Query,
    /// Schema-changing queries (DDL).
    SchemaChange,
}

/// A request dispatched for execution.
pub struct DispatchRequest {
    pub request_type: RequestType,
    pub plan: cassandra_cql::planner::QueryPlan,
    pub user: Option<String>,
    pub reply: oneshot::Sender<Result<QueryResult, crate::executor::ExecutorError>>,
}

/// Metrics for the dispatcher.
#[derive(Debug, Default)]
pub struct DispatcherMetrics {
    pub dispatched_total: AtomicU64,
    pub dispatched_auth: AtomicU64,
    pub dispatched_query: AtomicU64,
    pub dispatched_schema: AtomicU64,
    pub rejected: AtomicU64,
}

impl DispatcherMetrics {
    pub fn snapshot(&self) -> DispatcherMetricsSnapshot {
        DispatcherMetricsSnapshot {
            dispatched_total: self.dispatched_total.load(Ordering::Relaxed),
            dispatched_auth: self.dispatched_auth.load(Ordering::Relaxed),
            dispatched_query: self.dispatched_query.load(Ordering::Relaxed),
            dispatched_schema: self.dispatched_schema.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DispatcherMetricsSnapshot {
    pub dispatched_total: u64,
    pub dispatched_auth: u64,
    pub dispatched_query: u64,
    pub dispatched_schema: u64,
    pub rejected: u64,
}

/// Dispatcher with separate concurrency pools.
pub struct Dispatcher {
    /// Concurrency bound for query execution.
    query_semaphore: Arc<Semaphore>,
    /// Concurrency bound for auth requests.
    auth_semaphore: Arc<Semaphore>,
    /// Request sender.
    tx: mpsc::Sender<DispatchRequest>,
    /// Dispatcher metrics.
    pub metrics: Arc<DispatcherMetrics>,
}

impl Dispatcher {
    /// Create a new dispatcher and spawn the processing loop.
    ///
    /// - `query_concurrency`: max concurrent query executions
    /// - `auth_concurrency`: max concurrent auth operations
    /// - `queue_size`: mpsc channel buffer size
    pub fn new(
        executor: Arc<QueryExecutor>,
        query_concurrency: usize,
        auth_concurrency: usize,
        queue_size: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<DispatchRequest>(queue_size);
        let query_semaphore = Arc::new(Semaphore::new(query_concurrency));
        let auth_semaphore = Arc::new(Semaphore::new(auth_concurrency));
        let metrics = Arc::new(DispatcherMetrics::default());

        let dispatcher = Self {
            query_semaphore: Arc::clone(&query_semaphore),
            auth_semaphore: Arc::clone(&auth_semaphore),
            tx,
            metrics: Arc::clone(&metrics),
        };

        // Spawn the processing loop
        Self::spawn_processor(rx, executor, query_semaphore, auth_semaphore, metrics);

        dispatcher
    }

    fn spawn_processor(
        mut rx: mpsc::Receiver<DispatchRequest>,
        executor: Arc<QueryExecutor>,
        query_sem: Arc<Semaphore>,
        auth_sem: Arc<Semaphore>,
        metrics: Arc<DispatcherMetrics>,
    ) {
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let sem = match req.request_type {
                    RequestType::Auth => Arc::clone(&auth_sem),
                    RequestType::Query | RequestType::SchemaChange => Arc::clone(&query_sem),
                };

                let executor = Arc::clone(&executor);
                let metrics = Arc::clone(&metrics);

                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    metrics.dispatched_total.fetch_add(1, Ordering::Relaxed);
                    match req.request_type {
                        RequestType::Auth => {
                            metrics.dispatched_auth.fetch_add(1, Ordering::Relaxed);
                        }
                        RequestType::Query => {
                            metrics.dispatched_query.fetch_add(1, Ordering::Relaxed);
                        }
                        RequestType::SchemaChange => {
                            metrics.dispatched_schema.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    let result = executor.execute(&req.plan, req.user.as_deref());
                    if req.reply.send(result).is_err() {
                        debug!("Dispatch reply channel closed (client disconnected)");
                    }
                });
            }
        });
    }

    /// Dispatch a request for execution. Returns a oneshot receiver for the result.
    pub async fn dispatch(
        &self,
        request_type: RequestType,
        plan: cassandra_cql::planner::QueryPlan,
        user: Option<String>,
    ) -> Option<oneshot::Receiver<Result<QueryResult, crate::executor::ExecutorError>>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        let req = DispatchRequest {
            request_type,
            plan,
            user,
            reply: reply_tx,
        };

        match self.tx.send(req).await {
            Ok(()) => Some(reply_rx),
            Err(_) => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                error!("Dispatcher channel full or closed");
                None
            }
        }
    }

    /// Current available query permits.
    pub fn available_query_permits(&self) -> usize {
        self.query_semaphore.available_permits()
    }

    /// Current available auth permits.
    pub fn available_auth_permits(&self) -> usize {
        self.auth_semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: Full dispatcher tests require a running QueryExecutor with
    // storage engine, which is tested at the integration level. Here we
    // test the metrics and request type routing logic.

    #[test]
    fn request_type_equality() {
        assert_eq!(RequestType::Auth, RequestType::Auth);
        assert_ne!(RequestType::Auth, RequestType::Query);
        assert_ne!(RequestType::Query, RequestType::SchemaChange);
    }

    #[test]
    fn metrics_snapshot() {
        let m = DispatcherMetrics::default();
        m.dispatched_total.store(10, Ordering::Relaxed);
        m.dispatched_auth.store(2, Ordering::Relaxed);
        m.dispatched_query.store(7, Ordering::Relaxed);
        m.dispatched_schema.store(1, Ordering::Relaxed);
        m.rejected.store(3, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.dispatched_total, 10);
        assert_eq!(snap.dispatched_auth, 2);
        assert_eq!(snap.dispatched_query, 7);
        assert_eq!(snap.dispatched_schema, 1);
        assert_eq!(snap.rejected, 3);
    }
}
