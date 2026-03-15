// Licensed under Apache License, Version 2.0.

//! HTTP admin API for metrics, health checks, and virtual table queries.
//!
//! ## Design
//! Lightweight HTTP server using Hyper on a configurable admin port (default 9090).
//! Endpoints:
//! - `GET /metrics` — Prometheus text exposition
//! - `GET /health` — JSON health check
//! - `GET /api/v1/virtual/<keyspace>/<table>` — virtual table data (JSON)
//! - `GET /api/v1/operations` — active operations (JSON)
//!
//! No external framework; just hyper + http-body-util.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::prometheus_metrics::MetricsRegistry;
use crate::operations::OperationTracker;
use crate::virtual_tables::VirtualTableRegistry;

/// Shared state for the admin HTTP server.
pub struct AdminState {
    pub metrics: Arc<MetricsRegistry>,
    pub operations: Arc<OperationTracker>,
    pub virtual_tables: Arc<VirtualTableRegistry>,
}

/// Start the admin HTTP server.
pub async fn start_admin_server(
    bind_addr: SocketAddr,
    state: Arc<AdminState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("Admin HTTP server listening on {}", bind_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = Arc::clone(&state);
                async move { handle_request(req, state).await }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                error!("Admin HTTP connection error: {}", e);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<AdminState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let response = match (method, path.as_str()) {
        (Method::GET, "/metrics") => handle_metrics(&state),
        (Method::GET, "/health") => handle_health(),
        (Method::GET, "/api/v1/operations") => handle_operations(&state),
        (Method::GET, p) if p.starts_with("/api/v1/virtual/") => {
            handle_virtual_table(p, &state)
        }
        _ => not_found(),
    };

    Ok(response)
}

fn handle_metrics(state: &AdminState) -> Response<Full<Bytes>> {
    let body = state.metrics.gather_text();
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn handle_health() -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "status": "UP",
        "version": env!("CARGO_PKG_VERSION"),
    });
    json_response(StatusCode::OK, &body)
}

fn handle_operations(state: &AdminState) -> Response<Full<Bytes>> {
    let ops = state.operations.list_operations();
    let body = serde_json::to_value(&ops).unwrap_or(serde_json::Value::Array(vec![]));
    json_response(StatusCode::OK, &body)
}

fn handle_virtual_table(path: &str, state: &AdminState) -> Response<Full<Bytes>> {
    // Parse /api/v1/virtual/<keyspace>/<table>
    let parts: Vec<&str> = path.trim_start_matches("/api/v1/virtual/").split('/').collect();
    if parts.len() != 2 {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "expected /api/v1/virtual/<keyspace>/<table>"}),
        );
    }

    let keyspace = parts[0];
    let table_name = parts[1];

    match state.virtual_tables.get(keyspace, table_name) {
        Some(table) => {
            let columns = table.columns();
            let rows = table.rows();
            let body = serde_json::json!({
                "keyspace": keyspace,
                "table": table_name,
                "columns": columns,
                "rows": rows,
            });
            json_response(StatusCode::OK, &body)
        }
        None => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": format!("virtual table {}.{} not found", keyspace, table_name)}),
        ),
    }
}

fn not_found() -> Response<Full<Bytes>> {
    json_response(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"error": "not found"}),
    )
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_string(body).unwrap_or_default(),
        )))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> Arc<AdminState> {
        Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
        })
    }

    #[test]
    fn health_response() {
        let resp = handle_health();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn metrics_response() {
        let state = test_state();
        let resp = handle_metrics(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn operations_response() {
        let state = test_state();
        let resp = handle_operations(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn virtual_table_found() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/system_views/local", &state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn virtual_table_not_found() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/system_views/nonexistent", &state);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn virtual_table_bad_path() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/only_one_part", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn not_found_response() {
        let resp = not_found();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
