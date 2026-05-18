// Licensed under Apache License, Version 2.0.

//! Data structs for `system.local` and `system.peers_v2` rows.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.SystemKeyspace` (LOCAL / PEERS columns)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ─── LocalNodeInfo ───────────────────────────────────────────────────────────

/// Key columns from a `system.local` row describing the local node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalNodeInfo {
    /// Always `"local"`.
    pub key: String,
    /// UUID string identifying this host.
    pub host_id: String,
    pub cluster_name: String,
    pub cql_version: String,
    pub data_center: String,
    pub rack: String,
    pub release_version: String,
    pub native_protocol_version: String,
    pub listen_address: String,
    pub listen_port: u16,
    pub broadcast_address: String,
    pub broadcast_port: u16,
    pub rpc_address: String,
    pub rpc_port: u16,
    pub partitioner: String,
    pub tokens: Vec<String>,
    /// UUID string for the current schema version.
    pub schema_version: String,
    /// Bootstrap state, e.g. `"COMPLETED"`.
    pub bootstrapped: String,
}

impl LocalNodeInfo {
    /// Convert all fields to a flat `HashMap<String, String>`.
    ///
    /// Tokens are joined with `","`.
    pub fn to_row_map(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("key".into(), self.key.clone());
        m.insert("host_id".into(), self.host_id.clone());
        m.insert("cluster_name".into(), self.cluster_name.clone());
        m.insert("cql_version".into(), self.cql_version.clone());
        m.insert("data_center".into(), self.data_center.clone());
        m.insert("rack".into(), self.rack.clone());
        m.insert("release_version".into(), self.release_version.clone());
        m.insert(
            "native_protocol_version".into(),
            self.native_protocol_version.clone(),
        );
        m.insert("listen_address".into(), self.listen_address.clone());
        m.insert("listen_port".into(), self.listen_port.to_string());
        m.insert("broadcast_address".into(), self.broadcast_address.clone());
        m.insert("broadcast_port".into(), self.broadcast_port.to_string());
        m.insert("rpc_address".into(), self.rpc_address.clone());
        m.insert("rpc_port".into(), self.rpc_port.to_string());
        m.insert("partitioner".into(), self.partitioner.clone());
        m.insert("tokens".into(), self.tokens.join(","));
        m.insert("schema_version".into(), self.schema_version.clone());
        m.insert("bootstrapped".into(), self.bootstrapped.clone());
        m
    }
}

// ─── PeersEntry ──────────────────────────────────────────────────────────────

/// A single row from `system.peers_v2` describing a remote peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeersEntry {
    /// IP address of the peer.
    pub peer: String,
    pub peer_port: u16,
    /// UUID string identifying the peer host.
    pub host_id: String,
    pub data_center: String,
    pub rack: String,
    pub release_version: String,
    pub native_address: String,
    pub native_port: u16,
    pub preferred_ip: Option<String>,
    pub preferred_port: Option<u16>,
    pub tokens: Vec<String>,
    /// UUID string for the peer's schema version.
    pub schema_version: String,
}

impl PeersEntry {
    /// Convert all fields to a flat `HashMap<String, String>`.
    ///
    /// Tokens are joined with `","`. `None` optional fields are omitted.
    pub fn to_row_map(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("peer".into(), self.peer.clone());
        m.insert("peer_port".into(), self.peer_port.to_string());
        m.insert("host_id".into(), self.host_id.clone());
        m.insert("data_center".into(), self.data_center.clone());
        m.insert("rack".into(), self.rack.clone());
        m.insert("release_version".into(), self.release_version.clone());
        m.insert("native_address".into(), self.native_address.clone());
        m.insert("native_port".into(), self.native_port.to_string());
        if let Some(ref ip) = self.preferred_ip {
            m.insert("preferred_ip".into(), ip.clone());
        }
        if let Some(port) = self.preferred_port {
            m.insert("preferred_port".into(), port.to_string());
        }
        m.insert("tokens".into(), self.tokens.join(","));
        m.insert("schema_version".into(), self.schema_version.clone());
        m
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_local() -> LocalNodeInfo {
        LocalNodeInfo {
            key: "local".into(),
            host_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            cluster_name: "TestCluster".into(),
            cql_version: "3.4.6".into(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            release_version: "4.1.0".into(),
            native_protocol_version: "5".into(),
            listen_address: "127.0.0.1".into(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".into(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".into(),
            rpc_port: 9042,
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
            tokens: vec!["-9223372036854775808".into(), "0".into()],
            schema_version: "e84b6a60-24cf-30ca-9b58-452d92911703".into(),
            bootstrapped: "COMPLETED".into(),
        }
    }

    fn sample_peer() -> PeersEntry {
        PeersEntry {
            peer: "10.0.0.2".into(),
            peer_port: 7000,
            host_id: "660e8400-e29b-41d4-a716-446655440001".into(),
            data_center: "dc1".into(),
            rack: "rack2".into(),
            release_version: "4.1.0".into(),
            native_address: "10.0.0.2".into(),
            native_port: 9042,
            preferred_ip: None,
            preferred_port: None,
            tokens: vec!["100".into(), "200".into(), "300".into()],
            schema_version: "e84b6a60-24cf-30ca-9b58-452d92911703".into(),
        }
    }

    #[test]
    fn test_local_to_row_map_contains_all_fields() {
        let info = sample_local();
        let map = info.to_row_map();

        assert_eq!(map.get("key").unwrap(), "local");
        assert_eq!(map.get("host_id").unwrap(), &info.host_id);
        assert_eq!(map.get("cluster_name").unwrap(), "TestCluster");
        assert_eq!(map.get("listen_port").unwrap(), "7000");
        assert_eq!(map.get("rpc_port").unwrap(), "9042");
        assert_eq!(map.get("tokens").unwrap(), "-9223372036854775808,0");
        assert_eq!(map.get("bootstrapped").unwrap(), "COMPLETED");
        assert_eq!(map.len(), 18);
    }

    #[test]
    fn test_peer_to_row_map_omits_none_optionals() {
        let peer = sample_peer();
        let map = peer.to_row_map();

        assert_eq!(map.get("peer").unwrap(), "10.0.0.2");
        assert_eq!(map.get("peer_port").unwrap(), "7000");
        assert_eq!(map.get("tokens").unwrap(), "100,200,300");
        assert!(!map.contains_key("preferred_ip"));
        assert!(!map.contains_key("preferred_port"));
        // 12 total fields minus 2 None optionals = 10
        assert_eq!(map.len(), 10);
    }

    #[test]
    fn test_peer_to_row_map_includes_some_optionals() {
        let mut peer = sample_peer();
        peer.preferred_ip = Some("10.0.0.99".into());
        peer.preferred_port = Some(7099);
        let map = peer.to_row_map();

        assert_eq!(map.get("preferred_ip").unwrap(), "10.0.0.99");
        assert_eq!(map.get("preferred_port").unwrap(), "7099");
        assert_eq!(map.len(), 12);
    }

    #[test]
    fn test_local_serde_roundtrip() {
        let info = sample_local();
        let json = serde_json::to_string(&info).unwrap();
        let restored: LocalNodeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.key, info.key);
        assert_eq!(restored.tokens, info.tokens);
    }

    #[test]
    fn test_peer_serde_roundtrip() {
        let peer = sample_peer();
        let json = serde_json::to_string(&peer).unwrap();
        let restored: PeersEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.peer, peer.peer);
        assert_eq!(restored.tokens, peer.tokens);
        assert!(restored.preferred_ip.is_none());
    }
}
