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
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::http_admin::{AdminState, json_response};

#[derive(Debug, Clone)]
struct AuditConfigState {
    enabled: bool,
    logger: String,
    included_keyspaces: Vec<String>,
    excluded_keyspaces: Vec<String>,
    included_categories: Vec<String>,
    excluded_categories: Vec<String>,
    included_users: Vec<String>,
    excluded_users: Vec<String>,
}

#[derive(Debug, Clone)]
struct FqlConfigState {
    enabled: bool,
    log_dir: String,
    roll_cycle: String,
    block: bool,
    max_queue_weight: u64,
    max_log_size: u64,
}

#[derive(Debug, Clone)]
struct LoggingRuntimeState {
    levels: HashMap<String, String>,
    audit: AuditConfigState,
    fql: FqlConfigState,
    trace_probability: f64,
}

impl Default for LoggingRuntimeState {
    fn default() -> Self {
        let mut levels = HashMap::new();
        levels.insert("ROOT".to_string(), "INFO".to_string());
        levels.insert("org.apache.cassandra".to_string(), "DEBUG".to_string());

        Self {
            levels,
            audit: AuditConfigState {
                enabled: false,
                logger: "BinAuditLogger".to_string(),
                included_keyspaces: Vec::new(),
                excluded_keyspaces: Vec::new(),
                included_categories: vec![
                    "QUERY".to_string(),
                    "DML".to_string(),
                    "DDL".to_string(),
                    "DCL".to_string(),
                    "AUTH".to_string(),
                ],
                excluded_categories: Vec::new(),
                included_users: Vec::new(),
                excluded_users: Vec::new(),
            },
            fql: FqlConfigState {
                enabled: false,
                log_dir: "/var/log/cassandra/fql".to_string(),
                roll_cycle: "HOURLY".to_string(),
                block: true,
                max_queue_weight: 268_435_456,
                max_log_size: 17_179_869_184,
            },
            trace_probability: 0.0,
        }
    }
}

static LOGGING_RUNTIME_STATE: OnceLock<parking_lot::RwLock<LoggingRuntimeState>> = OnceLock::new();

fn logging_runtime_state() -> &'static parking_lot::RwLock<LoggingRuntimeState> {
    LOGGING_RUNTIME_STATE.get_or_init(|| parking_lot::RwLock::new(LoggingRuntimeState::default()))
}

/// `GET /api/v1/logging/levels` — returns current logging levels.
pub fn handle_get_logging_levels(_state: &AdminState) -> Response<Full<Bytes>> {
    let levels = logging_runtime_state().read().levels.clone();
    json_response(
        StatusCode::OK,
        &json!({
            "levels": levels
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

    logging_runtime_state()
        .write()
        .levels
        .insert(payload.logger.clone(), payload.level.clone());

    json_response(
        StatusCode::OK,
        &json!({
            "status": format!("logger '{}' set to {}", payload.logger, payload.level)
        }),
    )
}

/// `POST /api/v1/operations/enableauditlog` — enable audit logging.
pub fn handle_enable_audit_log(_state: &AdminState) -> Response<Full<Bytes>> {
    let mut runtime = logging_runtime_state().write();
    runtime.audit.enabled = true;
    json_response(
        StatusCode::OK,
        &json!({"status": "audit logging enabled", "enabled": true}),
    )
}

/// `POST /api/v1/operations/disableauditlog` — disable audit logging.
pub fn handle_disable_audit_log(_state: &AdminState) -> Response<Full<Bytes>> {
    let mut runtime = logging_runtime_state().write();
    runtime.audit.enabled = false;
    json_response(
        StatusCode::OK,
        &json!({"status": "audit logging disabled", "enabled": false}),
    )
}

/// `GET /api/v1/audit/config` — returns current audit logging configuration.
pub fn handle_get_audit_config(_state: &AdminState) -> Response<Full<Bytes>> {
    let audit = logging_runtime_state().read().audit.clone();
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": audit.enabled,
            "logger": audit.logger,
            "included_keyspaces": audit.included_keyspaces,
            "excluded_keyspaces": audit.excluded_keyspaces,
            "included_categories": audit.included_categories,
            "excluded_categories": audit.excluded_categories,
            "included_users": audit.included_users,
            "excluded_users": audit.excluded_users
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

    let mut runtime = logging_runtime_state().write();
    runtime.fql.enabled = true;
    runtime.fql.log_dir = payload.log_dir.clone();

    json_response(
        StatusCode::OK,
        &json!({
            "status": "FQL enabled",
            "enabled": true,
            "log_dir": payload.log_dir
        }),
    )
}

/// `POST /api/v1/operations/disablefql` — disable full query logging.
pub fn handle_disable_fql(_state: &AdminState) -> Response<Full<Bytes>> {
    let mut runtime = logging_runtime_state().write();
    runtime.fql.enabled = false;
    json_response(
        StatusCode::OK,
        &json!({"status": "FQL disabled", "enabled": false}),
    )
}

/// `GET /api/v1/fql/config` — returns current FQL configuration.
pub fn handle_get_fql_config(_state: &AdminState) -> Response<Full<Bytes>> {
    let fql = logging_runtime_state().read().fql.clone();
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": fql.enabled,
            "log_dir": fql.log_dir,
            "roll_cycle": fql.roll_cycle,
            "block": fql.block,
            "max_queue_weight": fql.max_queue_weight,
            "max_log_size": fql.max_log_size
        }),
    )
}

/// `GET /api/v1/tracing/probability` — returns the current tracing probability.
pub fn handle_get_trace_probability(_state: &AdminState) -> Response<Full<Bytes>> {
    let probability = logging_runtime_state().read().trace_probability;
    json_response(StatusCode::OK, &json!({"probability": probability}))
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

    logging_runtime_state().write().trace_probability = payload.probability;

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
    use std::sync::{Mutex, OnceLock};

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

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn get_logging_levels_returns_ok() {
        let _guard = test_lock();
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
        let _guard = test_lock();
        let state = test_state();
        let resp = handle_enable_audit_log(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "audit logging enabled");
        assert_eq!(parsed["enabled"], true);
    }

    #[test]
    fn disable_audit_log_returns_status() {
        let _guard = test_lock();
        let state = test_state();
        let resp = handle_disable_audit_log(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "audit logging disabled");
        assert_eq!(parsed["enabled"], false);
    }

    #[test]
    fn get_audit_config_returns_config() {
        let _guard = test_lock();
        let state = test_state();
        let _ = handle_enable_audit_log(&state);
        let resp = handle_get_audit_config(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["enabled"], true);
        assert_eq!(parsed["logger"], "BinAuditLogger");
        assert!(parsed["included_categories"].is_array());
    }

    #[test]
    fn disable_fql_returns_status() {
        let _guard = test_lock();
        let state = test_state();
        let resp = handle_disable_fql(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "FQL disabled");
        assert_eq!(parsed["enabled"], false);
    }

    #[test]
    fn get_fql_config_returns_config() {
        let _guard = test_lock();
        let state = test_state();
        let _ = handle_enable_fql_inner(br#"{"log_dir":"/tmp/fql-config-test"}"#);
        let resp = handle_get_fql_config(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["enabled"], true);
        assert_eq!(parsed["log_dir"], "/tmp/fql-config-test");
        assert_eq!(parsed["roll_cycle"], "HOURLY");
    }

    #[test]
    fn get_trace_probability_returns_zero() {
        let _guard = test_lock();
        let state = test_state();
        let _ = handle_set_trace_probability_inner(br#"{"probability":0.0}"#);
        let resp = handle_get_trace_probability(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["probability"], 0.0);
    }

    #[test]
    fn set_logging_level_returns_success() {
        let _guard = test_lock();
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
        let _guard = test_lock();
        let resp = handle_set_logging_level_inner(b"not json");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn enable_fql_returns_log_dir() {
        let _guard = test_lock();
        let body = serde_json::to_vec(&json!({"log_dir": "/tmp/fql"})).unwrap();

        let resp = handle_enable_fql_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["status"], "FQL enabled");
        assert_eq!(parsed["enabled"], true);
        assert_eq!(parsed["log_dir"], "/tmp/fql");
    }

    #[test]
    fn enable_fql_rejects_invalid_json() {
        let _guard = test_lock();
        let resp = handle_enable_fql_inner(b"{}");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn set_trace_probability_valid() {
        let _guard = test_lock();
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
        let _guard = test_lock();
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
        let _guard = test_lock();
        let body = serde_json::to_vec(&json!({"probability": -0.1})).unwrap();

        let resp = handle_set_trace_probability_inner(&body);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
