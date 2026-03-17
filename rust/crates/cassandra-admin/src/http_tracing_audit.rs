// Licensed under Apache License, Version 2.0.

//! HTTP endpoints for tracing session inspection and audit configuration.
//!
//! Provides handler functions that serialise tracing and audit data as JSON
//! responses using the same hyper/http-body-util stack as [`crate::http_admin`].

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

/// Summary of a single tracing session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingSessionInfo {
    pub session_id: String,
    pub started_at_ms: u64,
    pub event_count: usize,
    pub duration_us: u64,
}

/// Detailed view of a tracing session including its events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingSessionDetail {
    pub session_id: String,
    pub duration_us: u64,
    pub events: Vec<TracingEventInfo>,
}

/// A single event within a tracing session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingEventInfo {
    pub activity: String,
    pub source: String,
    pub elapsed_us: u64,
}

/// Current audit-logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfigInfo {
    pub enabled: bool,
    pub logger: String,
    pub included_keyspaces: Vec<String>,
    pub excluded_keyspaces: Vec<String>,
    pub included_categories: Vec<String>,
    pub excluded_categories: Vec<String>,
}

// ---------------------------------------------------------------------------
// Provider traits
// ---------------------------------------------------------------------------

/// Provides tracing session data for the HTTP layer.
pub trait TracingInfoProvider: Send + Sync {
    /// Return up to `limit` most-recent tracing sessions.
    fn list_sessions(&self, limit: usize) -> Vec<TracingSessionInfo>;

    /// Return full detail for a single session, or `None` if not found.
    fn session_detail(&self, session_id: &str) -> Option<TracingSessionDetail>;
}

/// Provides the current audit-logging configuration.
pub trait AuditConfigProvider: Send + Sync {
    fn config_info(&self) -> AuditConfigInfo;
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /admin/tracing/sessions?limit=N`
pub fn handle_tracing_sessions(
    provider: &dyn TracingInfoProvider,
    limit: usize,
) -> Response<Full<Bytes>> {
    let sessions = provider.list_sessions(limit);
    let body = serde_json::json!({ "sessions": sessions });
    json_response(StatusCode::OK, &body)
}

/// `GET /admin/tracing/sessions/<id>`
pub fn handle_tracing_session_detail(
    provider: &dyn TracingInfoProvider,
    session_id: &str,
) -> Response<Full<Bytes>> {
    match provider.session_detail(session_id) {
        Some(detail) => {
            let body = serde_json::to_value(&detail).unwrap_or_default();
            json_response(StatusCode::OK, &body)
        }
        None => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": format!("tracing session '{}' not found", session_id)}),
        ),
    }
}

/// `GET /admin/audit/config`
pub fn handle_audit_config(provider: &dyn AuditConfigProvider) -> Response<Full<Bytes>> {
    let info = provider.config_info();
    let body = serde_json::to_value(&info).unwrap_or_default();
    json_response(StatusCode::OK, &body)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_string(body).unwrap_or_default(),
        )))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    /// Extract body bytes from a `Full<Bytes>` response (test helper).
    fn body_string(resp: Response<Full<Bytes>>) -> String {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        String::from_utf8(collected.to_bytes().to_vec()).unwrap()
    }

    struct StubTracingProvider {
        sessions: Vec<TracingSessionInfo>,
        detail: Option<TracingSessionDetail>,
    }

    impl TracingInfoProvider for StubTracingProvider {
        fn list_sessions(&self, limit: usize) -> Vec<TracingSessionInfo> {
            self.sessions.iter().take(limit).cloned().collect()
        }

        fn session_detail(&self, session_id: &str) -> Option<TracingSessionDetail> {
            self.detail
                .as_ref()
                .filter(|d| d.session_id == session_id)
                .cloned()
        }
    }

    struct StubAuditProvider {
        info: AuditConfigInfo,
    }

    impl AuditConfigProvider for StubAuditProvider {
        fn config_info(&self) -> AuditConfigInfo {
            self.info.clone()
        }
    }

    fn sample_session() -> TracingSessionInfo {
        TracingSessionInfo {
            session_id: "abc-123".into(),
            started_at_ms: 1700000000000,
            event_count: 5,
            duration_us: 4200,
        }
    }

    fn sample_detail() -> TracingSessionDetail {
        TracingSessionDetail {
            session_id: "abc-123".into(),
            duration_us: 4200,
            events: vec![TracingEventInfo {
                activity: "Parsing query".into(),
                source: "127.0.0.1".into(),
                elapsed_us: 120,
            }],
        }
    }

    fn sample_audit_config() -> AuditConfigInfo {
        AuditConfigInfo {
            enabled: true,
            logger: "BinAuditLogger".into(),
            included_keyspaces: vec!["ks1".into()],
            excluded_keyspaces: vec![],
            included_categories: vec!["QUERY".into()],
            excluded_categories: vec!["DDL".into()],
        }
    }

    #[test]
    fn tracing_sessions_returns_ok() {
        let provider = StubTracingProvider {
            sessions: vec![sample_session()],
            detail: None,
        };
        let resp = handle_tracing_sessions(&provider, 10);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn tracing_sessions_respects_limit() {
        let provider = StubTracingProvider {
            sessions: vec![sample_session(), sample_session()],
            detail: None,
        };
        let resp = handle_tracing_sessions(&provider, 1);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["sessions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn tracing_session_detail_found() {
        let provider = StubTracingProvider {
            sessions: vec![],
            detail: Some(sample_detail()),
        };
        let resp = handle_tracing_session_detail(&provider, "abc-123");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn tracing_session_detail_not_found() {
        let provider = StubTracingProvider {
            sessions: vec![],
            detail: None,
        };
        let resp = handle_tracing_session_detail(&provider, "nonexistent");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn audit_config_returns_ok() {
        let provider = StubAuditProvider {
            info: sample_audit_config(),
        };
        let resp = handle_audit_config(&provider);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn audit_config_body_contains_enabled() {
        let provider = StubAuditProvider {
            info: sample_audit_config(),
        };
        let resp = handle_audit_config(&provider);
        let body = body_string(resp);
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["enabled"], true);
        assert_eq!(parsed["logger"], "BinAuditLogger");
    }
}
