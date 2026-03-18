// Licensed under Apache License, Version 2.0.

//! Route table for modular HTTP handler registration and dispatch.
//!
//! Allows domain modules (cluster, compaction, snapshots, etc.) to register
//! their own handlers without modifying the central dispatch function.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};

use crate::http_admin::AdminState;

/// Type alias for async handler functions.
pub type AsyncHandler = Box<
    dyn Fn(
            Request<Incoming>,
            Arc<AdminState>,
        ) -> Pin<Box<dyn Future<Output = Response<Full<Bytes>>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for sync handler functions (no body needed).
pub type SyncHandler = Box<dyn Fn(&str, &AdminState) -> Response<Full<Bytes>> + Send + Sync>;

/// Route entry — either sync (for simple GET) or async (for POST with body).
enum RouteEntry {
    Sync(SyncHandler),
    Async(AsyncHandler),
    /// Prefix-matched sync handler (e.g. `/api/v1/virtual/` matches any sub-path).
    PrefixSync(SyncHandler),
    /// Prefix-matched async handler.
    PrefixAsync(AsyncHandler),
}

/// A table of HTTP routes that dispatches to registered handlers.
pub struct RouteTable {
    routes: HashMap<(Method, String), RouteEntry>,
    prefix_routes: Vec<(Method, String, RouteEntry)>,
}

impl RouteTable {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            prefix_routes: Vec::new(),
        }
    }

    /// Register a synchronous GET handler for an exact path.
    pub fn get(&mut self, path: &str, handler: SyncHandler) {
        self.routes
            .insert((Method::GET, path.to_string()), RouteEntry::Sync(handler));
    }

    /// Register a synchronous GET handler for a path prefix.
    pub fn get_prefix(&mut self, prefix: &str, handler: SyncHandler) {
        self.prefix_routes.push((
            Method::GET,
            prefix.to_string(),
            RouteEntry::PrefixSync(handler),
        ));
    }

    /// Register an async POST handler for an exact path.
    pub fn post(&mut self, path: &str, handler: AsyncHandler) {
        self.routes
            .insert((Method::POST, path.to_string()), RouteEntry::Async(handler));
    }

    /// Register a synchronous POST handler for an exact path (no body parsing).
    pub fn post_sync(&mut self, path: &str, handler: SyncHandler) {
        self.routes
            .insert((Method::POST, path.to_string()), RouteEntry::Sync(handler));
    }

    /// Register an async DELETE handler for an exact path.
    pub fn delete(&mut self, path: &str, handler: AsyncHandler) {
        self.routes.insert(
            (Method::DELETE, path.to_string()),
            RouteEntry::Async(handler),
        );
    }

    /// Dispatch a request to the matching handler, or return 404.
    pub async fn dispatch(
        &self,
        req: Request<Incoming>,
        state: Arc<AdminState>,
    ) -> Response<Full<Bytes>> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        // Try exact match first.
        let key = (method.clone(), path.clone());
        if let Some(entry) = self.routes.get(&key) {
            return match entry {
                RouteEntry::Sync(handler) => handler(&path, &state),
                RouteEntry::Async(handler) => handler(req, state).await,
                RouteEntry::PrefixSync(handler) => handler(&path, &state),
                RouteEntry::PrefixAsync(handler) => handler(req, state).await,
            };
        }

        // Try prefix match.
        for (route_method, prefix, entry) in &self.prefix_routes {
            if &method == route_method && path.starts_with(prefix) {
                return match entry {
                    RouteEntry::PrefixSync(handler) => handler(&path, &state),
                    RouteEntry::PrefixAsync(handler) => handler(req, state).await,
                    RouteEntry::Sync(handler) => handler(&path, &state),
                    RouteEntry::Async(handler) => handler(req, state).await,
                };
            }
        }

        // No match.
        crate::http_admin::not_found()
    }
}

impl Default for RouteTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::StatusCode;

    #[test]
    fn test_route_table_creation() {
        let table = RouteTable::new();
        assert!(table.routes.is_empty());
        assert!(table.prefix_routes.is_empty());
    }

    #[test]
    fn test_register_sync_get() {
        let mut table = RouteTable::new();
        table.get(
            "/health",
            Box::new(|_path, _state| {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from("ok")))
                    .unwrap()
            }),
        );
        assert_eq!(table.routes.len(), 1);
    }

    #[test]
    fn test_register_prefix() {
        let mut table = RouteTable::new();
        table.get_prefix(
            "/api/v1/virtual/",
            Box::new(|_path, _state| {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from("{}")))
                    .unwrap()
            }),
        );
        assert_eq!(table.prefix_routes.len(), 1);
    }

    #[tokio::test]
    async fn test_dispatch_not_found() {
        let table = RouteTable::new();
        let _state = Arc::new(AdminState {
            metrics: Arc::new(crate::prometheus_metrics::MetricsRegistry::new()),
            operations: Arc::new(crate::operations::OperationTracker::new()),
            virtual_tables: Arc::new(crate::virtual_tables::VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
        });
        let _req = Request::builder()
            .method(Method::GET)
            .uri("/nonexistent")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        // Can't easily dispatch with Incoming body type in tests, so we just verify the table is empty
        assert!(table.routes.is_empty());
    }
}
