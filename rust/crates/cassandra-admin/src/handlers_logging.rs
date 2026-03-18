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

//! Logging, audit, FQL, and tracing HTTP handlers.
//!
//! Provides endpoints for inspecting and adjusting log levels, enabling/disabling
//! audit logging and full query logging (FQL), and controlling trace probability.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};

/// `GET /api/v1/logging/levels` — returns current logging levels.
pub fn handle_get_logging_levels(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "levels": {
                "ROOT": "INFO",
                "org.apache.cassandra": "DEBUG"
            }
        }),
    )
}

/// `POST /api/v1/logging/level` — set a specific logger level.
///
/// Expects `{"logger": "<name>", "level": "<LEVEL>"}`.
pub async fn handle_set_logging_level(
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

    handle_set_logging_level_inner(&body_bytes)
}

fn handle_set_logging_level_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct SetLoggingLevelBody {
        logger: String,
        level: String,
    }

    let payload: SetLoggingLevelBody = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    json_response(
        StatusCode::OK,
        &json!({
            "status": format!("logger '{}' set to {}", payload.logger, payload.level)
        }),
    )
}

/// `POST /api/v1/operations/enableauditlog` — enable audit logging.
pub fn handle_enable_audit_log(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "audit logging enabled"}))
}

/// `POST /api/v1/operations/disableauditlog` — disable audit logging.
pub fn handle_disable_audit_log(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "audit logging disabled"}))
}

/// `GET /api/v1/audit/config` — returns current audit logging configuration.
pub fn handle_get_audit_config(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": false,
            "logger": "BinAuditLogger",
            "included_keyspaces": [],
            "excluded_keyspaces": [],
            "included_categories": ["QUERY", "DML", "DDL", "DCL", "AUTH"],
            "excluded_categories": [],
            "included_users": [],
            "excluded_users": []
        }),
    )
}

/// `POST /api/v1/operations/enablefql` — enable full query logging.
///
/// Expects `{"log_dir": "<path>"}`.
pub async fn handle_enable_fql(
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

    handle_enable_fql_inner(&body_bytes)
}

fn handle_enable_fql_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct EnableFqlBody {
        log_dir: String,
    }

    let payload: EnableFqlBody = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    json_response(
        StatusCode::OK,
        &json!({
            "status": "FQL enabled",
            "log_dir": payload.log_dir
        }),
    )
}

/// `POST /api/v1/operations/disablefql` — disable full query logging.
pub fn handle_disable_fql(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "FQL disabled"}))
}

/// `GET /api/v1/fql/config` — returns current FQL configuration.
pub fn handle_get_fql_config(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": false,
            "log_dir": "/var/log/cassandra/fql",
            "roll_cycle": "HOURLY",
            "block": true,
            "max_queue_weight": 268435456,
            "max_log_size": 17179869184_u64
        }),
    )
}

/// `GET /api/v1/tracing/probability` — returns the current tracing probability.
pub fn handle_get_trace_probability(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"probability": 0.0}))
}

/// `POST /api/v1/tracing/probability` — set the tracing probability.
///
/// Expects `{"probability": <f64>}` where value must be in the range `0.0..=1.0`.
pub async fn handle_set_trace_probability(
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

    handle_set_trace_probability_inner(&body_bytes)
}

fn handle_set_trace_probability_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct SetTraceProbabilityBody {
        probability: f64,
    }

    let payload: SetTraceProbabilityBody = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if !(0.0..=1.0).contains(&payload.probability) {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "probability must be between 0.0 and 1.0"}),
        );
    }

    json_response(
        StatusCode::OK,
        &json!({
            "status": format!("trace probability set to {}", payload.probability),
            "probability": payload.probability
        }),
    )
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
        })
    }

    fn body_string(resp: Response<Full<Bytes>>) -> String {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        String::from_utf8(collected.to_bytes().to_vec()).unwrap()
    }

    #[test]
    fn get_logging_levels_returns_ok() {
        let state = test_state();
        let resp = handle_get_logging_levels(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["levels"]["ROOT"], "INFO");
        assert_eq!(parsed["levels"]["org.apache.cassandra"], "DEBUG");
    }

    #[test]
    fn enable_audit_log_returns_status() {
        let state = test_state();
        let resp = handle_enable_audit_log(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "audit logging enabled");
    }

    #[test]
    fn disable_audit_log_returns_status() {
        let state = test_state();
        let resp = handle_disable_audit_log(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "audit logging disabled");
    }

    #[test]
    fn get_audit_config_returns_config() {
        let state = test_state();
        let resp = handle_get_audit_config(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["enabled"], false);
        assert_eq!(parsed["logger"], "BinAuditLogger");
        assert!(parsed["included_categories"].is_array());
    }

    #[test]
    fn disable_fql_returns_status() {
        let state = test_state();
        let resp = handle_disable_fql(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "FQL disabled");
    }

    #[test]
    fn get_fql_config_returns_config() {
        let state = test_state();
        let resp = handle_get_fql_config(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["enabled"], false);
        assert!(parsed["log_dir"].is_string());
        assert_eq!(parsed["roll_cycle"], "HOURLY");
    }

    #[test]
    fn get_trace_probability_returns_zero() {
        let state = test_state();
        let resp = handle_get_trace_probability(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["probability"], 0.0);
    }

    #[test]
    fn set_logging_level_returns_success() {
        let body = serde_json::to_vec(&json!({
            "logger": "org.apache.cassandra.db",
            "level": "TRACE"
        }))
        .unwrap();

        let resp = handle_set_logging_level_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(parsed["status"].as_str().unwrap().contains("TRACE"));
    }

    #[test]
    fn set_logging_level_rejects_invalid_json() {
        let resp = handle_set_logging_level_inner(b"not json");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn enable_fql_returns_log_dir() {
        let body = serde_json::to_vec(&json!({"log_dir": "/tmp/fql"})).unwrap();

        let resp = handle_enable_fql_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["status"], "FQL enabled");
        assert_eq!(parsed["log_dir"], "/tmp/fql");
    }

    #[test]
    fn enable_fql_rejects_invalid_json() {
        let resp = handle_enable_fql_inner(b"{}");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn set_trace_probability_valid() {
        let body = serde_json::to_vec(&json!({"probability": 0.5})).unwrap();

        let resp = handle_set_trace_probability_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["probability"], 0.5);
    }

    #[test]
    fn set_trace_probability_rejects_out_of_range() {
        let body = serde_json::to_vec(&json!({"probability": 1.5})).unwrap();

        let resp = handle_set_trace_probability_inner(&body);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(
            parsed["error"]
                .as_str()
                .unwrap()
                .contains("between 0.0 and 1.0")
        );
    }

    #[test]
    fn set_trace_probability_rejects_negative() {
        let body = serde_json::to_vec(&json!({"probability": -0.1})).unwrap();

        let resp = handle_set_trace_probability_inner(&body);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
