// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Cluster Metadata Service (CMS) lifecycle and discovery.
//!
//! Manages the service state machine (gossip → local/remote CMS),
//! CMS node discovery, and service configuration for the TCM subsystem.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.ClusterMetadataService`
//! - `org.apache.cassandra.tcm.Discovery`
//! - `org.apache.cassandra.service.Startup`

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::node::{Endpoint, NodeId};
use crate::tcm::Epoch;

// ─────────────────────────────────────────────────────────────────────────────
// ServiceState
// ─────────────────────────────────────────────────────────────────────────────

/// The operational mode of the Cluster Metadata Service.
///
/// Tracks whether metadata is managed via gossip (legacy), a local CMS
/// instance, or a remote CMS peer set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceState {
    /// Legacy gossip-based metadata management.
    Gossip,
    /// This node is the local CMS instance.
    Local,
    /// Metadata is served by remote CMS nodes.
    Remote { cms_nodes: Vec<Endpoint> },
    /// Service is resetting (e.g., during upgrade or recovery).
    Reset,
}

impl ServiceState {
    /// Returns `true` if the service is actively managing metadata
    /// (either locally or via remote CMS nodes).
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Local | Self::Remote { .. })
    }

    /// Returns `true` if this node delegates to remote CMS nodes.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }

    /// Returns `true` if this node uses legacy gossip for metadata.
    pub fn is_gossip(&self) -> bool {
        matches!(self, Self::Gossip)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ServiceError
// ─────────────────────────────────────────────────────────────────────────────

/// Errors arising from CMS service operations.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Invalid state transition from {from} to {to}")]
    InvalidTransition { from: String, to: String },

    #[error("Service not ready: {0}")]
    NotReady(String),

    #[error("CMS discovery failed: {0}")]
    DiscoveryFailed(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// CmsDiscovery
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks known CMS nodes and manages discovery of new ones.
///
/// Used during startup and topology changes to locate the set of
/// nodes that can serve cluster metadata.
#[derive(Debug, Clone)]
pub struct CmsDiscovery {
    /// Set of known CMS node endpoints.
    known_cms_nodes: HashSet<Endpoint>,
    /// This node's own endpoint.
    local_node: Endpoint,
}

impl CmsDiscovery {
    /// Create a new discovery instance for the given local endpoint.
    pub fn new(local: Endpoint) -> Self {
        Self {
            known_cms_nodes: HashSet::new(),
            local_node: local,
        }
    }

    /// Add a known CMS node endpoint.
    pub fn add_known_node(&mut self, ep: Endpoint) {
        self.known_cms_nodes.insert(ep);
    }

    /// Remove a CMS node endpoint from the known set.
    pub fn remove_node(&mut self, ep: &Endpoint) {
        self.known_cms_nodes.remove(ep);
    }

    /// Return a sorted list of all known CMS node endpoints.
    pub fn known_nodes(&self) -> Vec<Endpoint> {
        self.known_cms_nodes.iter().copied().collect()
    }

    /// Returns `true` if the local node is in the known CMS set.
    pub fn is_local_cms(&self) -> bool {
        self.known_cms_nodes.contains(&self.local_node)
    }

    /// Add all peer endpoints to the known CMS set.
    pub fn discover_from_peers(&mut self, peers: &[Endpoint]) {
        for ep in peers {
            self.known_cms_nodes.insert(*ep);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ClusterMetadataServiceConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the Cluster Metadata Service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadataServiceConfig {
    /// Delay between retry attempts in milliseconds.
    pub retry_delay_ms: u64,
    /// Maximum number of retries for CMS operations.
    pub max_retries: u32,
    /// Epoch interval between automatic snapshots.
    pub snapshot_interval: u64,
    /// Number of log entries before compaction is triggered.
    pub log_compaction_threshold: usize,
}

impl Default for ClusterMetadataServiceConfig {
    fn default() -> Self {
        Self {
            retry_delay_ms: 1000,
            max_retries: 10,
            snapshot_interval: 100,
            log_compaction_threshold: 1000,
        }
    }
}

impl ClusterMetadataServiceConfig {
    /// Validate the configuration, returning an error if any values
    /// are out of acceptable range.
    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.retry_delay_ms == 0 {
            return Err(ServiceError::ConfigError(
                "retry_delay_ms must be greater than 0".into(),
            ));
        }
        if self.max_retries == 0 {
            return Err(ServiceError::ConfigError(
                "max_retries must be greater than 0".into(),
            ));
        }
        if self.snapshot_interval == 0 {
            return Err(ServiceError::ConfigError(
                "snapshot_interval must be greater than 0".into(),
            ));
        }
        if self.log_compaction_threshold == 0 {
            return Err(ServiceError::ConfigError(
                "log_compaction_threshold must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ClusterMetadataService
// ─────────────────────────────────────────────────────────────────────────────

/// The main CMS service, managing state transitions, discovery,
/// and epoch tracking for this node.
#[derive(Debug)]
pub struct ClusterMetadataService {
    /// Current operational state of the service.
    state: ServiceState,
    /// Service configuration.
    config: ClusterMetadataServiceConfig,
    /// Identity of the local node.
    local_node: NodeId,
    /// Network endpoint of the local node.
    local_endpoint: Endpoint,
    /// CMS node discovery tracker.
    discovery: CmsDiscovery,
    /// Current metadata epoch known to this service.
    current_epoch: Epoch,
}

impl ClusterMetadataService {
    /// Create a new CMS service instance, starting in `Gossip` state.
    ///
    /// The config is validated before construction.
    pub fn new(
        local_node: NodeId,
        local_endpoint: Endpoint,
        config: ClusterMetadataServiceConfig,
    ) -> Result<Self, ServiceError> {
        config.validate()?;
        Ok(Self {
            state: ServiceState::Gossip,
            config,
            local_node,
            local_endpoint,
            discovery: CmsDiscovery::new(local_endpoint),
            current_epoch: Epoch::EMPTY,
        })
    }

    /// Returns a reference to the current service state.
    pub fn state(&self) -> &ServiceState {
        &self.state
    }

    /// Returns the current metadata epoch.
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Transition the service to a new state.
    ///
    /// Valid transitions:
    /// - `Gossip`  → `Local` | `Remote`
    /// - `Local`   → `Remote` | `Reset`
    /// - `Remote`  → `Local` | `Reset`
    /// - `Reset`   → `Gossip`
    pub fn transition_to(&mut self, new_state: ServiceState) -> Result<(), ServiceError> {
        let valid = match (&self.state, &new_state) {
            (ServiceState::Gossip, ServiceState::Local) => true,
            (ServiceState::Gossip, ServiceState::Remote { .. }) => true,
            (ServiceState::Local, ServiceState::Remote { .. }) => true,
            (ServiceState::Local, ServiceState::Reset) => true,
            (ServiceState::Remote { .. }, ServiceState::Local) => true,
            (ServiceState::Remote { .. }, ServiceState::Reset) => true,
            (ServiceState::Reset, ServiceState::Gossip) => true,
            _ => false,
        };

        if !valid {
            return Err(ServiceError::InvalidTransition {
                from: format!("{:?}", self.state),
                to: format!("{:?}", new_state),
            });
        }

        self.state = new_state;
        Ok(())
    }

    /// Update the current metadata epoch.
    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.current_epoch = epoch;
    }

    /// Returns a reference to the CMS discovery tracker.
    pub fn discovery(&self) -> &CmsDiscovery {
        &self.discovery
    }

    /// Returns a mutable reference to the CMS discovery tracker.
    pub fn discovery_mut(&mut self) -> &mut CmsDiscovery {
        &mut self.discovery
    }

    /// Returns `true` if the service is in an active state (Local or Remote).
    pub fn is_ready(&self) -> bool {
        self.state.is_active()
    }

    /// Returns the service configuration.
    pub fn config(&self) -> &ClusterMetadataServiceConfig {
        &self.config
    }

    /// Returns the local node identity.
    pub fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Returns the local endpoint.
    pub fn local_endpoint(&self) -> Endpoint {
        self.local_endpoint
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    // ── State transition tests ──────────────────────────────────────────

    #[test]
    fn gossip_to_local_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        assert!(svc.state().is_gossip());
        assert!(!svc.is_ready());

        svc.transition_to(ServiceState::Local).unwrap();
        assert!(svc.is_ready());
    }

    #[test]
    fn gossip_to_remote_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Remote {
            cms_nodes: vec![ep(7001), ep(7002)],
        })
        .unwrap();

        assert!(svc.state().is_remote());
        assert!(svc.is_ready());
    }

    #[test]
    fn local_to_remote_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Local).unwrap();
        svc.transition_to(ServiceState::Remote {
            cms_nodes: vec![ep(7001)],
        })
        .unwrap();

        assert!(svc.state().is_remote());
    }

    #[test]
    fn remote_to_local_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Remote {
            cms_nodes: vec![ep(7001)],
        })
        .unwrap();
        svc.transition_to(ServiceState::Local).unwrap();

        assert!(!svc.state().is_remote());
    }

    #[test]
    fn local_to_reset_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Local).unwrap();
        svc.transition_to(ServiceState::Reset).unwrap();
        assert!(!svc.is_ready());
    }

    #[test]
    fn remote_to_reset_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Remote {
            cms_nodes: vec![ep(7001)],
        })
        .unwrap();
        svc.transition_to(ServiceState::Reset).unwrap();
        assert!(!svc.is_ready());
    }

    #[test]
    fn reset_to_gossip_is_valid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Local).unwrap();
        svc.transition_to(ServiceState::Reset).unwrap();
        svc.transition_to(ServiceState::Gossip).unwrap();

        assert!(svc.state().is_gossip());
    }

    #[test]
    fn gossip_to_reset_is_invalid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        let result = svc.transition_to(ServiceState::Reset);
        assert!(result.is_err());
    }

    #[test]
    fn gossip_to_gossip_is_invalid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        let result = svc.transition_to(ServiceState::Gossip);
        assert!(result.is_err());
    }

    #[test]
    fn reset_to_local_is_invalid() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.transition_to(ServiceState::Local).unwrap();
        svc.transition_to(ServiceState::Reset).unwrap();

        let result = svc.transition_to(ServiceState::Local);
        assert!(result.is_err());
    }

    // ── Discovery tests ─────────────────────────────────────────────────

    #[test]
    fn discovery_add_and_remove_nodes() {
        let mut disc = CmsDiscovery::new(ep(7000));

        disc.add_known_node(ep(7001));
        disc.add_known_node(ep(7002));
        assert_eq!(disc.known_nodes().len(), 2);

        disc.remove_node(&ep(7001));
        assert_eq!(disc.known_nodes().len(), 1);
    }

    #[test]
    fn discovery_is_local_cms() {
        let mut disc = CmsDiscovery::new(ep(7000));
        assert!(!disc.is_local_cms());

        disc.add_known_node(ep(7000));
        assert!(disc.is_local_cms());
    }

    #[test]
    fn discovery_from_peers() {
        let mut disc = CmsDiscovery::new(ep(7000));
        let peers = vec![ep(7001), ep(7002), ep(7003)];

        disc.discover_from_peers(&peers);
        assert_eq!(disc.known_nodes().len(), 3);
    }

    #[test]
    fn discovery_dedup() {
        let mut disc = CmsDiscovery::new(ep(7000));

        disc.add_known_node(ep(7001));
        disc.add_known_node(ep(7001));
        assert_eq!(disc.known_nodes().len(), 1);
    }

    // ── Config tests ────────────────────────────────────────────────────

    #[test]
    fn default_config_is_valid() {
        let config = ClusterMetadataServiceConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.retry_delay_ms, 1000);
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.snapshot_interval, 100);
        assert_eq!(config.log_compaction_threshold, 1000);
    }

    #[test]
    fn config_zero_retry_delay_is_invalid() {
        let config = ClusterMetadataServiceConfig {
            retry_delay_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_zero_max_retries_is_invalid() {
        let config = ClusterMetadataServiceConfig {
            max_retries: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_zero_snapshot_interval_is_invalid() {
        let config = ClusterMetadataServiceConfig {
            snapshot_interval: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_zero_compaction_threshold_is_invalid() {
        let config = ClusterMetadataServiceConfig {
            log_compaction_threshold: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // ── Service creation tests ──────────────────────────────────────────

    #[test]
    fn service_creation_with_default_config() {
        let svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        assert!(svc.state().is_gossip());
        assert_eq!(svc.current_epoch(), Epoch::EMPTY);
        assert!(!svc.is_ready());
    }

    #[test]
    fn service_creation_with_invalid_config_fails() {
        let config = ClusterMetadataServiceConfig {
            retry_delay_ms: 0,
            ..Default::default()
        };

        let result = ClusterMetadataService::new(node_id(1), ep(7000), config);
        assert!(result.is_err());
    }

    #[test]
    fn service_set_epoch() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.set_epoch(Epoch(42));
        assert_eq!(svc.current_epoch(), Epoch(42));
    }

    #[test]
    fn service_discovery_access() {
        let mut svc = ClusterMetadataService::new(
            node_id(1),
            ep(7000),
            ClusterMetadataServiceConfig::default(),
        )
        .unwrap();

        svc.discovery_mut().add_known_node(ep(7001));
        assert_eq!(svc.discovery().known_nodes().len(), 1);
    }
}
