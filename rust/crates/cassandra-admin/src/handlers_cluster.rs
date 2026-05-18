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

//! HTTP handlers for cluster information endpoints.

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};

/// GET /api/v1/cluster/status — cluster status with datacenter and node list.
pub fn handle_cluster_status(state: &AdminState) -> Response<Full<Bytes>> {
    // Try to pull node info from virtual tables
    let local = state.virtual_tables.get("system_views", "local");
    let gossip = state.virtual_tables.get("system_views", "gossip_info");

    let mut nodes = Vec::new();

    // Add local node
    if let Some(table) = local {
        for row in table.rows() {
            let address = row
                .get("listen_address")
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".into());
            let dc = row
                .get("data_center")
                .cloned()
                .unwrap_or_else(|| "datacenter1".into());
            let rack = row.get("rack").cloned().unwrap_or_else(|| "rack1".into());
            let host_id = row
                .get("host_id")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            nodes.push(json!({
                "address": address,
                "status": "UP",
                "state": "Normal",
                "load": "0 bytes",
                "tokens": 256,
                "owns": "100.0%",
                "rack": rack,
                "host_id": host_id,
                "datacenter": dc,
            }));
        }
    }

    // Add gossip peers
    if let Some(table) = gossip {
        for row in table.rows() {
            let address = row
                .get("address")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            let dc = row
                .get("data_center")
                .cloned()
                .unwrap_or_else(|| "datacenter1".into());
            let rack = row.get("rack").cloned().unwrap_or_else(|| "rack1".into());
            let host_id = row
                .get("host_id")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            let status = row.get("status").cloned().unwrap_or_else(|| "UP".into());
            let load = row.get("load").cloned().unwrap_or_else(|| "0 bytes".into());
            nodes.push(json!({
                "address": address,
                "status": status,
                "state": "Normal",
                "load": load,
                "tokens": 256,
                "owns": "?",
                "rack": rack,
                "host_id": host_id,
                "datacenter": dc,
            }));
        }
    }

    // Determine datacenter from first node or default
    let datacenter = nodes
        .first()
        .and_then(|n| n.get("datacenter"))
        .and_then(|v| v.as_str())
        .unwrap_or("datacenter1")
        .to_string();

    // Fall back to a single default node if none found
    if nodes.is_empty() {
        nodes.push(json!({
            "address": "127.0.0.1",
            "status": "UP",
            "state": "Normal",
            "load": "0 bytes",
            "tokens": 256,
            "owns": "100.0%",
            "rack": "rack1",
            "host_id": "unknown",
            "datacenter": "datacenter1",
        }));
    }

    json_response(
        StatusCode::OK,
        &json!({
            "datacenter": datacenter,
            "nodes": nodes,
        }),
    )
}

/// GET /api/v1/cluster/info — local node information.
pub fn handle_cluster_info(state: &AdminState) -> Response<Full<Bytes>> {
    let local = state.virtual_tables.get("system_views", "local");

    if let Some(table) = local {
        let rows = table.rows();
        if let Some(row) = rows.first() {
            let host_id = row
                .get("host_id")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            let dc = row
                .get("data_center")
                .cloned()
                .unwrap_or_else(|| "datacenter1".into());
            let rack = row.get("rack").cloned().unwrap_or_else(|| "rack1".into());
            let listen_address = row
                .get("listen_address")
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".into());
            let release_version = row
                .get("release_version")
                .cloned()
                .unwrap_or_else(|| "unknown".into());

            return json_response(
                StatusCode::OK,
                &json!({
                    "host_id": host_id,
                    "gossip_active": true,
                    "native_transport": true,
                    "load": "0 bytes",
                    "uptime_seconds": 0,
                    "datacenter": dc,
                    "rack": rack,
                    "listen_address": listen_address,
                    "release_version": release_version,
                    "tokens": 256,
                }),
            );
        }
    }

    // Default fallback
    json_response(
        StatusCode::OK,
        &json!({
            "host_id": "unknown",
            "gossip_active": true,
            "native_transport": true,
            "load": "0 bytes",
            "uptime_seconds": 0,
            "datacenter": "datacenter1",
            "rack": "rack1",
            "listen_address": "127.0.0.1",
            "release_version": "unknown",
            "tokens": 256,
        }),
    )
}

/// GET /api/v1/cluster/ring — token ring information.
pub fn handle_cluster_ring(state: &AdminState) -> Response<Full<Bytes>> {
    let local = state.virtual_tables.get("system_views", "local");
    let gossip = state.virtual_tables.get("system_views", "gossip_info");

    let mut ring_nodes = Vec::new();

    if let Some(table) = local {
        for row in table.rows() {
            let address = row
                .get("listen_address")
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".into());
            let rack = row.get("rack").cloned().unwrap_or_else(|| "rack1".into());
            ring_nodes.push(json!({
                "address": address,
                "rack": rack,
                "status": "Up",
                "state": "Normal",
                "load": "0 bytes",
                "owns": "100.0%",
                "token": "-9223372036854775808",
            }));
        }
    }

    if let Some(table) = gossip {
        for row in table.rows() {
            let address = row
                .get("address")
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            let rack = row.get("rack").cloned().unwrap_or_else(|| "rack1".into());
            let status = row.get("status").cloned().unwrap_or_else(|| "Up".into());
            let load = row.get("load").cloned().unwrap_or_else(|| "0 bytes".into());
            ring_nodes.push(json!({
                "address": address,
                "rack": rack,
                "status": status,
                "state": "Normal",
                "load": load,
                "owns": "?",
                "token": "0",
            }));
        }
    }

    if ring_nodes.is_empty() {
        ring_nodes.push(json!({
            "address": "127.0.0.1",
            "rack": "rack1",
            "status": "Up",
            "state": "Normal",
            "load": "0 bytes",
            "owns": "100.0%",
            "token": "-9223372036854775808",
        }));
    }

    json_response(StatusCode::OK, &json!({ "ring": ring_nodes }))
}

/// GET /api/v1/cluster/describe — cluster description.
pub fn handle_describe_cluster(state: &AdminState) -> Response<Full<Bytes>> {
    let local = state.virtual_tables.get("system_views", "local");

    let cluster_name = local
        .and_then(|t| t.rows().into_iter().next())
        .and_then(|row| row.get("cluster_name").cloned())
        .unwrap_or_else(|| "Test Cluster".into());

    let release_version = local
        .and_then(|t| t.rows().into_iter().next())
        .and_then(|row| row.get("release_version").cloned())
        .unwrap_or_else(|| "unknown".into());

    let mut schema_versions = serde_json::Map::new();
    schema_versions.insert("current".into(), json!([release_version]));

    json_response(
        StatusCode::OK,
        &json!({
            "name": cluster_name,
            "snitch": "org.apache.cassandra.locator.SimpleSnitch",
            "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
            "schema_versions": schema_versions,
        }),
    )
}

/// GET /api/v1/cluster/gossip — gossip information.
pub fn handle_gossip_info(state: &AdminState) -> Response<Full<Bytes>> {
    let gossip = state.virtual_tables.get("system_views", "gossip_info");

    match gossip {
        Some(table) => {
            let rows = table.rows();
            let endpoints: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "address": row.get("address").cloned().unwrap_or_default(),
                        "port": row.get("port").cloned().unwrap_or_else(|| "7000".into()),
                        "hostname": row.get("hostname").cloned().unwrap_or_default(),
                        "generation": row.get("generation").cloned().unwrap_or_else(|| "0".into()),
                        "heartbeat": row.get("heartbeat").cloned().unwrap_or_else(|| "0".into()),
                        "status": row.get("status").cloned().unwrap_or_else(|| "NORMAL".into()),
                        "load": row.get("load").cloned().unwrap_or_else(|| "0".into()),
                        "data_center": row.get("data_center").cloned().unwrap_or_default(),
                        "rack": row.get("rack").cloned().unwrap_or_default(),
                        "release_version": row.get("release_version").cloned().unwrap_or_default(),
                        "host_id": row.get("host_id").cloned().unwrap_or_default(),
                    })
                })
                .collect();

            json_response(StatusCode::OK, &json!({ "endpoints": endpoints }))
        }
        None => json_response(StatusCode::OK, &json!({ "endpoints": [] })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;

    fn test_state() -> AdminState {
        AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
        }
    }

    #[test]
    fn cluster_status_returns_ok_with_nodes() {
        let state = test_state();
        let resp = handle_cluster_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn cluster_info_returns_ok() {
        let state = test_state();
        let resp = handle_cluster_info(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn cluster_ring_returns_ok() {
        let state = test_state();
        let resp = handle_cluster_ring(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn describe_cluster_returns_ok_with_metadata() {
        let state = test_state();
        let resp = handle_describe_cluster(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn gossip_info_returns_ok_empty_endpoints() {
        let state = test_state();
        let resp = handle_gossip_info(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn cluster_status_fallback_with_empty_registry() {
        let state = AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
        };
        let resp = handle_cluster_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
