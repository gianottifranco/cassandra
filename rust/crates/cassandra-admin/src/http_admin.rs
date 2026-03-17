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

use crate::operations::OperationTracker;
use crate::prometheus_metrics::MetricsRegistry;
use crate::virtual_tables::VirtualTableRegistry;

/// Shared state for the admin HTTP server.
pub struct AdminState {
    pub metrics: Arc<MetricsRegistry>,
    pub operations: Arc<OperationTracker>,
    pub virtual_tables: Arc<VirtualTableRegistry>,
    pub repair_coordinator: Option<Arc<cassandra_repair::RepairCoordinator>>,
    pub storage_engine: Option<Arc<cassandra_storage::engine::StorageEngine>>,
    pub schema_catalog: Option<Arc<parking_lot::RwLock<cassandra_schema::SchemaCatalog>>>,
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
        (Method::POST, "/api/v1/operations/repair") => handle_repair_request(req, &state).await,
        (Method::GET, p) if p.starts_with("/api/v1/virtual/") => handle_virtual_table(p, &state),
        (Method::POST, "/api/v1/operations/rebuild_index") => {
            handle_rebuild_index(req, &state).await
        }
        (Method::GET, "/admin/indexes") => handle_list_indexes(&state),
        (Method::GET, p) if p.starts_with("/admin/indexes/") => {
            handle_index_detail(p, &state)
        }
        _ => not_found(),
    };

    Ok(response)
}

fn handle_metrics(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(ref coordinator) = state.repair_coordinator {
        state
            .metrics
            .sync_from_repair_metrics(&coordinator.metrics.snapshot());
    }
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
    let parts: Vec<&str> = path
        .trim_start_matches("/api/v1/virtual/")
        .split('/')
        .collect();
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

fn handle_list_indexes(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(ref engine) = state.storage_engine {
        let mgrs = engine.index_managers.read();
        let mut indexes = Vec::new();
        for (cf_name, mgr) in mgrs.iter() {
            for idx_name in mgr.list_names() {
                if let Some(def) = mgr.get_definition(&idx_name) {
                    indexes.push(serde_json::json!({
                        "name": idx_name,
                        "column_family": cf_name,
                        "column": def.column,
                        "type": format!("{:?}", def.index_type),
                        "status": format!("{:?}", mgr.get_status(&idx_name)),
                    }));
                }
            }
        }
        json_response(StatusCode::OK, &serde_json::json!({"indexes": indexes}))
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "storage engine not configured"}),
        )
    }
}

fn handle_index_detail(path: &str, state: &AdminState) -> Response<Full<Bytes>> {
    let index_name = path
        .trim_start_matches("/admin/indexes/")
        .to_string();

    if index_name.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "index name required"}),
        );
    }

    if let Some(ref engine) = state.storage_engine {
        let mgrs = engine.index_managers.read();
        for (cf_name, mgr) in mgrs.iter() {
            if let Some(def) = mgr.get_definition(&index_name) {
                let body = serde_json::json!({
                    "name": index_name,
                    "column_family": cf_name,
                    "column": def.column,
                    "type": format!("{:?}", def.index_type),
                    "status": format!("{:?}", mgr.get_status(&index_name)),
                    "keyspace": def.keyspace,
                    "table": def.table,
                    "options": def.options,
                });
                return json_response(StatusCode::OK, &body);
            }
        }
        json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": format!("index '{}' not found", index_name)}),
        )
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "storage engine not configured"}),
        )
    }
}

fn not_found() -> Response<Full<Bytes>> {
    json_response(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"error": "not found"}),
    )
}

async fn handle_repair_request(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct RepairRequestBody {
        keyspace: String,
        #[serde(default)]
        tables: Vec<String>,
        #[serde(default)]
        full: bool,
        #[serde(default)]
        preview: bool,
    }

    let payload: RepairRequestBody = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if let Some(ref coordinator) = state.repair_coordinator {
        let repair_type = if payload.preview {
            cassandra_repair::RepairType::Preview
        } else if payload.full {
            cassandra_repair::RepairType::Full
        } else {
            cassandra_repair::RepairType::Incremental
        };

        // Stub ranges to the whole ring, and local endpoint for now
        let local_ep = cassandra_cluster_metadata::Endpoint::new("127.0.0.1:7000".parse().unwrap());
        let ranges = vec![(
            cassandra_common::Token::from_raw(i64::MIN),
            cassandra_common::Token::from_raw(i64::MAX),
        )];

        match coordinator.start_repair(
            repair_type,
            &payload.keyspace,
            &payload.tables,
            &ranges,
            &[local_ep],
        ) {
            Ok(id) => {
                let op_id = state.operations.register(
                    crate::operations::OperationType::Repair,
                    format!("Repair keyspace: {}", payload.keyspace),
                );
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({ "repair_id": id.to_string(), "operation_id": op_id.to_string() }),
                )
            }
            Err(e) => json_response(
                StatusCode::CONFLICT,
                &serde_json::json!({"error": e.to_string()}),
            ),
        }
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "repair coordinator not configured"}),
        )
    }
}

async fn handle_rebuild_index(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct RebuildIndexRequestBody {
        keyspace: String,
        table: String,
        index_name: String,
    }

    let payload: RebuildIndexRequestBody = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if let Some(ref catalog) = state.schema_catalog {
        let snap = catalog.read().snapshot();
        let table_meta = match snap.table(&payload.keyspace, &payload.table) {
            Some(t) => t,
            None => {
                let msg = format!("Table {}.{} not found", payload.keyspace, payload.table);
                return json_response(StatusCode::NOT_FOUND, &serde_json::json!({"error": msg}));
            }
        };

        let index_meta = match table_meta.index(&payload.index_name) {
            Some(i) => i,
            None => {
                let msg = format!(
                    "Index {} not found on {}.{}",
                    payload.index_name, payload.keyspace, payload.table
                );
                return json_response(StatusCode::NOT_FOUND, &serde_json::json!({"error": msg}));
            }
        };

        if let Some(ref engine) = state.storage_engine {
            let column_name = match index_meta.target_column() {
                Some(c) => c.clone(),
                None => {
                    return json_response(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({"error": "Index target column not defined"}),
                    );
                }
            };

            // Map IndexKind to IndexType
            let index_type = match index_meta.kind {
                cassandra_schema::IndexKind::Keys => cassandra_storage::index::IndexType::Legacy,
                cassandra_schema::IndexKind::Custom => {
                    let class = index_meta
                        .options
                        .get("class_name")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    if class.contains("SASIIndex") {
                        #[cfg(feature = "sasi")]
                        {
                            cassandra_storage::index::IndexType::Sasi
                        }
                        #[cfg(not(feature = "sasi"))]
                        {
                            return json_response(
                                StatusCode::NOT_IMPLEMENTED,
                                &serde_json::json!({"error": "SASI index support not enabled"}),
                            );
                        }
                    } else if class.contains("StorageAttachedIndex") {
                        cassandra_storage::index::IndexType::Sai
                    } else {
                        return json_response(
                            StatusCode::BAD_REQUEST,
                            &serde_json::json!({"error": format!("Unsupported custom index class: {}", class)}),
                        );
                    }
                }
                cassandra_schema::IndexKind::Composites => cassandra_storage::index::IndexType::Legacy,
            };

            let definition = cassandra_storage::index::IndexDefinition {
                name: index_meta.name.clone(),
                keyspace: payload.keyspace.clone(),
                table: payload.table.clone(),
                column: column_name,
                index_type,
                options: index_meta.options.clone(),
            };

            let cf_name = format!("{}.{}", payload.keyspace, payload.table);
            match engine.rebuild_index(&cf_name, definition) {
                Ok(_) => {
                    let msg = format!(
                        "Rebuild of index {} completed successfully",
                        payload.index_name
                    );
                    json_response(StatusCode::OK, &serde_json::json!({"status": msg}))
                }
                Err(e) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &serde_json::json!({"error": e.to_string()}),
                ),
            }
        } else {
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &serde_json::json!({"error": "storage engine not configured"}),
            )
        }
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "schema catalog not configured"}),
        )
    }
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
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
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
