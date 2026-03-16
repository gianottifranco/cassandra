use std::fmt;

use serde::{Deserialize, Serialize};

use crate::gossip::{ApplicationState, Gossiper};
use crate::node::Endpoint;
use crate::tcm::{Epoch, TcmMetadata};

/// Control plane mode for the cluster.
///
/// Determines whether cluster metadata is managed via gossip,
/// TCM (Transactional Cluster Metadata), or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlPlaneMode {
    /// Classic gossip-based membership and schema propagation.
    GossipOnly,
    /// TCM is authoritative for topology and schema; gossip carries
    /// health/status information and TCM epoch for catch-up.
    TcmWithGossipBridge,
    /// Full TCM mode: gossip is disabled for metadata.
    TcmOnly,
}

impl ControlPlaneMode {
    /// Whether gossip is used for any purpose in this mode.
    pub fn uses_gossip(&self) -> bool {
        matches!(self, Self::GossipOnly | Self::TcmWithGossipBridge)
    }

    /// Whether TCM is used as the metadata authority.
    pub fn uses_tcm(&self) -> bool {
        matches!(self, Self::TcmWithGossipBridge | Self::TcmOnly)
    }
}

impl fmt::Display for ControlPlaneMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GossipOnly => write!(f, "GossipOnly"),
            Self::TcmWithGossipBridge => write!(f, "TcmWithGossipBridge"),
            Self::TcmOnly => write!(f, "TcmOnly"),
        }
    }
}

impl Default for ControlPlaneMode {
    fn default() -> Self {
        Self::GossipOnly
    }
}

/// The control plane bridge: dispatches metadata operations to the
/// appropriate subsystem based on the configured mode.
///
/// In `TcmWithGossipBridge` mode:
/// - Schema changes go through TCM commit.
/// - Node status goes through gossip.
/// - Topology changes go through TCM.
/// - Gossip carries the current TCM epoch for peer catch-up.
pub struct ControlPlaneBridge {
    mode: ControlPlaneMode,
}

impl ControlPlaneBridge {
    pub fn new(mode: ControlPlaneMode) -> Self {
        Self { mode }
    }

    pub fn mode(&self) -> ControlPlaneMode {
        self.mode
    }

    /// Whether a schema change should go through TCM commit.
    pub fn schema_via_tcm(&self) -> bool {
        self.mode.uses_tcm()
    }

    /// Whether topology changes should go through TCM commit.
    pub fn topology_via_tcm(&self) -> bool {
        self.mode.uses_tcm()
    }

    /// Whether health/status should be disseminated via gossip.
    pub fn health_via_gossip(&self) -> bool {
        self.mode.uses_gossip()
    }

    // ── Epoch Dissemination ─────────────────────────────────────────────

    /// Publish the current TCM epoch as a gossip application state.
    ///
    /// In TcmWithGossipBridge mode, after each successful TCM commit,
    /// the local node publishes its epoch so peers can detect staleness.
    ///
    /// ## Java Oracle
    ///
    /// `Gossiper.addLocalApplicationState(METADATA_EPOCH, ...)`
    pub fn disseminate_epoch(&self, gossiper: &Gossiper, epoch: Epoch) {
        if !matches!(self.mode, ControlPlaneMode::TcmWithGossipBridge) {
            return;
        }

        gossiper.set_local_state(ApplicationState::TcmEpoch, epoch.value().to_string());
    }

    /// Detect peers that are behind the current epoch.
    ///
    /// Scans gossip state for endpoints whose `TcmEpoch` is older than ours.
    /// Returns (endpoint, their_epoch) tuples for stale peers.
    pub fn detect_stale_peers(
        &self,
        gossiper: &Gossiper,
        our_epoch: Epoch,
    ) -> Vec<(Endpoint, Epoch)> {
        if !matches!(self.mode, ControlPlaneMode::TcmWithGossipBridge) {
            return Vec::new();
        }

        let mut stale = Vec::new();
        for ep in gossiper.live_endpoints() {
            if let Some(state) = gossiper.get_endpoint_state(&ep) {
                if let Some(epoch_val) = state.get_state(&ApplicationState::TcmEpoch) {
                    if let Ok(peer_epoch) = epoch_val.value.parse::<u64>() {
                        let peer = Epoch(peer_epoch);
                        if peer < our_epoch {
                            stale.push((ep, peer));
                        }
                    }
                } else {
                    // No epoch → pre-TCM node, treat as epoch 0
                    stale.push((ep, Epoch::EMPTY));
                }
            }
        }
        stale
    }

    /// Get log entries a stale peer needs for catch-up.
    pub fn entries_for_catch_up(
        &self,
        tcm: &TcmMetadata,
        from_epoch: Epoch,
    ) -> Vec<crate::tcm::MetadataLogEntry> {
        tcm.log.entries_since(from_epoch).to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{SeedProvider, VersionedValue};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn gossip_only_mode() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::GossipOnly);
        assert!(!bridge.schema_via_tcm());
        assert!(!bridge.topology_via_tcm());
        assert!(bridge.health_via_gossip());
    }

    #[test]
    fn tcm_with_gossip_bridge() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::TcmWithGossipBridge);
        assert!(bridge.schema_via_tcm());
        assert!(bridge.topology_via_tcm());
        assert!(bridge.health_via_gossip());
    }

    #[test]
    fn tcm_only_mode() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::TcmOnly);
        assert!(bridge.schema_via_tcm());
        assert!(bridge.topology_via_tcm());
        assert!(!bridge.health_via_gossip());
    }

    #[test]
    fn default_mode() {
        assert_eq!(ControlPlaneMode::default(), ControlPlaneMode::GossipOnly);
    }

    #[test]
    fn epoch_dissemination() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::TcmWithGossipBridge);
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![ep(7002)]), 1);

        bridge.disseminate_epoch(&gossiper, Epoch(5));

        let state = gossiper.get_endpoint_state(&ep(7001)).unwrap();
        let epoch_val = state.get_state(&ApplicationState::TcmEpoch).unwrap();
        assert_eq!(epoch_val.value, "5");
    }

    #[test]
    fn detect_stale_peers() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::TcmWithGossipBridge);
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1);

        // Set our epoch to 5
        bridge.disseminate_epoch(&gossiper, Epoch(5));

        // Add a "peer" with epoch 3
        gossiper.update_remote_state(
            ep(7002),
            ApplicationState::TcmEpoch,
            VersionedValue::new(3, "3"),
        );

        let stale = bridge.detect_stale_peers(&gossiper, Epoch(5));
        assert!(!stale.is_empty());
        assert_eq!(stale[0].0, ep(7002));
        assert_eq!(stale[0].1, Epoch(3));
    }

    #[test]
    fn gossip_only_skips_epoch() {
        let bridge = ControlPlaneBridge::new(ControlPlaneMode::GossipOnly);
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1);

        bridge.disseminate_epoch(&gossiper, Epoch(5));

        // Should not have set any state
        let state = gossiper.get_endpoint_state(&ep(7001)).unwrap();
        assert!(state.get_state(&ApplicationState::TcmEpoch).is_none());
    }
}
