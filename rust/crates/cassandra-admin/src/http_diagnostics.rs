// Licensed under Apache License, Version 2.0.

//! HTTP diagnostics endpoints for recent diagnostic events.
//!
//! Provides a trait-based provider model so any subsystem can surface
//! diagnostic history through the admin HTTP API.

use std::collections::HashMap;

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use serde::{Deserialize, Serialize};

/// Serializable DTO representing a single diagnostic event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticEventInfo {
    pub event_type: String,
    pub timestamp_ms: u64,
    pub description: String,
    pub attributes: HashMap<String, String>,
}

/// Trait for components that can provide recent diagnostic events.
pub trait DiagnosticHistoryProvider: Send + Sync {
    /// Return up to `limit` most-recent diagnostic events.
    fn recent_events(&self, limit: usize) -> Vec<DiagnosticEventInfo>;
}

/// Handler that returns a JSON array of recent diagnostic events.
pub fn handle_diagnostics_recent(
    provider: &dyn DiagnosticHistoryProvider,
    limit: usize,
) -> Response<Full<Bytes>> {
    let events = provider.recent_events(limit);
    let body = serde_json::to_value(&events).unwrap_or(serde_json::Value::Array(vec![]));
    json_response(StatusCode::OK, &body)
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

    /// Trivial provider that returns a fixed set of events.
    struct StubProvider {
        events: Vec<DiagnosticEventInfo>,
    }

    impl DiagnosticHistoryProvider for StubProvider {
        fn recent_events(&self, limit: usize) -> Vec<DiagnosticEventInfo> {
            self.events.iter().take(limit).cloned().collect()
        }
    }

    fn sample_event(event_type: &str, ts: u64) -> DiagnosticEventInfo {
        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), "value".to_string());
        DiagnosticEventInfo {
            event_type: event_type.to_string(),
            timestamp_ms: ts,
            description: format!("{} at {}", event_type, ts),
            attributes: attrs,
        }
    }

    #[test]
    fn handle_diagnostics_recent_returns_ok() {
        let provider = StubProvider {
            events: vec![sample_event("COMPACTION", 1000)],
        };
        let resp = handle_diagnostics_recent(&provider, 10);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn handle_diagnostics_recent_respects_limit() {
        use http_body_util::BodyExt;

        let provider = StubProvider {
            events: vec![
                sample_event("COMPACTION", 1000),
                sample_event("FLUSH", 2000),
                sample_event("REPAIR", 3000),
            ],
        };
        let resp = handle_diagnostics_recent(&provider, 2);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: Vec<DiagnosticEventInfo> = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[tokio::test]
    async fn handle_diagnostics_recent_empty_provider() {
        use http_body_util::BodyExt;

        let provider = StubProvider { events: vec![] };
        let resp = handle_diagnostics_recent(&provider, 10);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: Vec<DiagnosticEventInfo> = serde_json::from_slice(&collected).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn diagnostic_event_info_roundtrip_serde() {
        let event = sample_event("GC_PAUSE", 5000);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: DiagnosticEventInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_type, "GC_PAUSE");
        assert_eq!(deserialized.timestamp_ms, 5000);
        assert_eq!(deserialized.attributes.get("key").unwrap(), "value");
    }

    #[test]
    fn json_response_sets_content_type() {
        let body = serde_json::json!({"test": true});
        let resp = json_response(StatusCode::OK, &body);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/json"
        );
    }
}
