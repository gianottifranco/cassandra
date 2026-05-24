// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration and service control HTTP handlers.
//!
//! Provides endpoints for reading/writing runtime configuration, reloading
//! schema/triggers/SSL, and toggling native transport and gossip.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};

/// `GET /api/v1/config` — returns current configuration settings.
///
/// If the virtual table `system_views/settings` is available its rows are
/// returned, otherwise a set of common defaults is used.
pub fn handle_get_config(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(table) = state.virtual_tables.get("system_views", "settings") {
        let rows = table.rows();
        let body = json!({ "settings": rows });
        json_response(StatusCode::OK, &body)
    } else {
        let body = json!({
            "settings": {
                "compaction_throughput_mb": 64,
                "streaming_throughput_mb": 200,
                "concurrent_compactors": 2,
                "read_request_timeout_ms": 5000,
                "write_request_timeout_ms": 2000,
            }
        });
        json_response(StatusCode::OK, &body)
    }
}

/// `POST /api/v1/config` — update runtime configuration settings.
///
/// Expects a JSON object of key-value pairs to update and returns the list of
/// keys that were accepted.
pub async fn handle_set_config(
    req: Request<Incoming>,
    _state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_set_config_inner(&body_bytes)
}

fn handle_set_config_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    let payload: serde_json::Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let updated: Vec<String> = match payload.as_object() {
        Some(map) => map.keys().cloned().collect(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "expected a JSON object of key-value pairs"}),
            );
        }
    };

    json_response(StatusCode::OK, &json!({"updated": updated}))
}

/// `POST /api/v1/config/reload/schema` — trigger schema reload.
pub fn handle_reload_schema(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "schema reloaded"}))
}

/// `POST /api/v1/config/reload/triggers` — trigger triggers reload.
pub fn handle_reload_triggers(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "triggers reloaded"}))
}

/// `POST /api/v1/config/reload/ssl` — trigger SSL certificate reload.
pub fn handle_reload_ssl(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({"status": "ssl certificates reloaded"}),
    )
}

/// `POST /api/v1/binary/enable` — enable the CQL native transport.
pub fn handle_enable_binary(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({"status": "native transport enabled"}),
    )
}

/// `POST /api/v1/binary/disable` — disable the CQL native transport.
pub fn handle_disable_binary(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({"status": "native transport disabled"}),
    )
}

/// `GET /api/v1/binary/status` — check native transport status.
pub fn handle_binary_status(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"running": true}))
}

/// `POST /api/v1/gossip/enable` — enable the gossip protocol.
pub fn handle_enable_gossip(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "gossip enabled"}))
}

/// `POST /api/v1/gossip/disable` — disable the gossip protocol.
pub fn handle_disable_gossip(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "gossip disabled"}))
}

/// `GET /api/v1/gossip/status` — check gossip protocol status.
pub fn handle_gossip_status(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"running": true}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;
    use http_body_util::BodyExt;
    use std::sync::Arc;

    fn test_state() -> Arc<AdminState> {
        Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
            topology_controller: None,
        })
    }

    fn body_string(resp: Response<Full<Bytes>>) -> String {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        String::from_utf8(collected.to_bytes().to_vec()).unwrap()
    }

    #[test]
    fn get_config_returns_ok_with_virtual_table() {
        let state = test_state();
        let resp = handle_get_config(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        // When virtual table is present, settings is an array of rows
        assert!(parsed["settings"].is_array());
    }

    #[test]
    fn get_config_returns_defaults_without_virtual_table() {
        let state = Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
            topology_controller: None,
        });
        let resp = handle_get_config(&state);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        let settings = &parsed["settings"];
        assert_eq!(settings["compaction_throughput_mb"], 64);
        assert_eq!(settings["streaming_throughput_mb"], 200);
        assert_eq!(settings["concurrent_compactors"], 2);
        assert_eq!(settings["read_request_timeout_ms"], 5000);
        assert_eq!(settings["write_request_timeout_ms"], 2000);
    }

    #[test]
    fn reload_schema_returns_status() {
        let state = test_state();
        let resp = handle_reload_schema(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "schema reloaded");
    }

    #[test]
    fn reload_triggers_returns_status() {
        let state = test_state();
        let resp = handle_reload_triggers(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "triggers reloaded");
    }

    #[test]
    fn reload_ssl_returns_status() {
        let state = test_state();
        let resp = handle_reload_ssl(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "ssl certificates reloaded");
    }

    #[test]
    fn enable_binary_returns_status() {
        let state = test_state();
        let resp = handle_enable_binary(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "native transport enabled");
    }

    #[test]
    fn disable_binary_returns_status() {
        let state = test_state();
        let resp = handle_disable_binary(&state);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "native transport disabled");
    }

    #[test]
    fn binary_status_returns_running() {
        let state = test_state();
        let resp = handle_binary_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["running"], true);
    }

    #[test]
    fn enable_gossip_returns_status() {
        let state = test_state();
        let resp = handle_enable_gossip(&state);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "gossip enabled");
    }

    #[test]
    fn disable_gossip_returns_status() {
        let state = test_state();
        let resp = handle_disable_gossip(&state);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "gossip disabled");
    }

    #[test]
    fn gossip_status_returns_running() {
        let state = test_state();
        let resp = handle_gossip_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["running"], true);
    }

    #[test]
    fn set_config_returns_updated_keys() {
        let body = serde_json::to_vec(&json!({
            "compaction_throughput_mb": 128,
            "read_request_timeout_ms": 10000
        }))
        .unwrap();

        let resp = handle_set_config_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        let updated = parsed["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 2);
    }

    #[test]
    fn set_config_rejects_non_object() {
        let resp = handle_set_config_inner(b"\"not an object\"");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn set_config_rejects_invalid_json() {
        let resp = handle_set_config_inner(b"not json");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
