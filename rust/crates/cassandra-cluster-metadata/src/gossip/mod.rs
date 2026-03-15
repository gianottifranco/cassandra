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

//! Gossip protocol: cluster membership, state propagation, failure detection.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.Gossiper`
//! - `org.apache.cassandra.gms.EndpointState`
//! - `org.apache.cassandra.gms.ApplicationState`
//! - `org.apache.cassandra.gms.VersionedValue`
//! - `org.apache.cassandra.gms.GossipDigest`
//! - `org.apache.cassandra.gms.FailureDetector`

pub mod failure_detector;
pub mod messages;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use tracing::{debug, info, warn};

use crate::node::{Endpoint, NodeState};
use self::failure_detector::FailureDetector;
use self::messages::{GossipDigest, GossipDigestAck, GossipDigestAck2, GossipDigestSyn};

/// Application states propagated through gossip.
///
/// Each node advertises these states; peers learn them through the gossip
/// protocol's SYN→ACK→ACK2 exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum ApplicationState {
    /// Node lifecycle status (JOINING, NORMAL, LEAVING, etc.).
    Status,
    /// Datacenter name.
    Datacenter,
    /// Rack name.
    Rack,
    /// Reported load in bytes.
    Load,
    /// Token list (serialized).
    Tokens,
    /// Schema version UUID.
    SchemaVersion,
    /// Host ID UUID.
    HostId,
    /// RPC (native protocol) address.
    RpcAddress,
    /// Release version string.
    ReleaseVersion,
    /// Internal IP (for internode).
    InternalIp,
    /// Severity (for dynamic snitch).
    Severity,
}

/// A versioned value for gossip state propagation.
///
/// Each update increments the version; during gossip, nodes send only
/// states newer than what the peer already has.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionedValue {
    /// Monotonically increasing version number.
    pub version: i64,
    /// String representation of the value.
    pub value: String,
}

impl VersionedValue {
    pub fn new(version: i64, value: impl Into<String>) -> Self {
        Self {
            version,
            value: value.into(),
        }
    }
}

/// Heartbeat state: generation counter + version counter.
///
/// Generation changes on node restart; version increments on every heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatState {
    /// Incremented on node restart.
    pub generation: i64,
    /// Incremented on every gossip round by the node itself.
    pub version: i64,
}

impl HeartbeatState {
    pub fn new(generation: i64) -> Self {
        Self {
            generation,
            version: 0,
        }
    }

    pub fn increment(&mut self) {
        self.version += 1;
    }
}

/// The complete state of an endpoint as known through gossip.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndpointState {
    pub heartbeat: HeartbeatState,
    pub application_states: BTreeMap<ApplicationState, VersionedValue>,
    /// When this state was last updated locally.
    #[serde(skip)]
    pub update_timestamp: Option<Instant>,
}

impl EndpointState {
    pub fn new(generation: i64) -> Self {
        Self {
            heartbeat: HeartbeatState::new(generation),
            application_states: BTreeMap::new(),
            update_timestamp: Some(Instant::now()),
        }
    }

    /// Add or update an application state.
    pub fn set_state(&mut self, key: ApplicationState, value: VersionedValue) {
        self.application_states.insert(key, value);
        self.update_timestamp = Some(Instant::now());
    }

    /// Get the current value for an application state.
    pub fn get_state(&self, key: &ApplicationState) -> Option<&VersionedValue> {
        self.application_states.get(key)
    }

    /// The maximum version across heartbeat and all application states.
    pub fn max_version(&self) -> i64 {
        let app_max = self
            .application_states
            .values()
            .map(|v| v.version)
            .max()
            .unwrap_or(0);
        self.heartbeat.version.max(app_max)
    }

    /// Returns the node status string if set.
    pub fn status(&self) -> Option<&str> {
        self.get_state(&ApplicationState::Status)
            .map(|v| v.value.as_str())
    }
}

/// Seed provider: supplies the initial contact points for gossip.
#[derive(Debug, Clone)]
pub struct SeedProvider {
    seeds: Vec<Endpoint>,
}

impl SeedProvider {
    pub fn new(seeds: Vec<Endpoint>) -> Self {
        Self { seeds }
    }

    pub fn seeds(&self) -> &[Endpoint] {
        &self.seeds
    }
}

/// The Gossiper: manages cluster membership via gossip protocol.
///
/// Runs periodic gossip rounds (default: every 1 second) to exchange
/// endpoint state with peers. Uses phi-accrual failure detection.
pub struct Gossiper {
    /// This node's endpoint.
    local_endpoint: Endpoint,
    /// Known endpoint states for all nodes (including self).
    endpoint_states: Arc<RwLock<HashMap<Endpoint, EndpointState>>>,
    /// Seed provider for initial contact.
    seeds: SeedProvider,
    /// Failure detector.
    failure_detector: Arc<RwLock<FailureDetector>>,
    /// Nodes currently marked as live.
    live_endpoints: Arc<RwLock<Vec<Endpoint>>>,
    /// Nodes currently marked as dead/unreachable.
    dead_endpoints: Arc<RwLock<Vec<Endpoint>>>,
    /// Version counter for local state updates.
    version_counter: Arc<std::sync::atomic::AtomicI64>,
}

impl Gossiper {
    /// Create a new Gossiper.
    pub fn new(
        local_endpoint: Endpoint,
        seeds: SeedProvider,
        generation: i64,
    ) -> Self {
        let mut endpoint_states = HashMap::new();
        endpoint_states.insert(local_endpoint, EndpointState::new(generation));

        Self {
            local_endpoint,
            endpoint_states: Arc::new(RwLock::new(endpoint_states)),
            seeds,
            failure_detector: Arc::new(RwLock::new(FailureDetector::new(8.0))),
            live_endpoints: Arc::new(RwLock::new(Vec::new())),
            dead_endpoints: Arc::new(RwLock::new(Vec::new())),
            version_counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        }
    }

    /// Set a local application state at the next version.
    pub fn set_local_state(&self, key: ApplicationState, value: String) {
        let version = self
            .version_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let mut states = self.endpoint_states.write();
        if let Some(local) = states.get_mut(&self.local_endpoint) {
            local.set_state(key, VersionedValue::new(version, value));
            local.heartbeat.increment();
        }
    }

    /// Build a GossipDigestSyn message to send to a peer.
    pub fn make_gossip_digest_syn(&self) -> GossipDigestSyn {
        let states = self.endpoint_states.read();
        let digests: Vec<GossipDigest> = states
            .iter()
            .map(|(ep, state)| GossipDigest {
                endpoint: *ep,
                generation: state.heartbeat.generation,
                max_version: state.max_version(),
            })
            .collect();

        GossipDigestSyn {
            cluster_id: "cassandra-rust".to_string(),
            digests,
        }
    }

    /// Handle an incoming GossipDigestSyn — produce an ACK.
    ///
    /// Returns a GossipDigestAck containing:
    /// - Digests for states the sender is behind on
    /// - Full state diffs for states the sender doesn't have
    pub fn handle_syn(&self, syn: &GossipDigestSyn) -> GossipDigestAck {
        let states = self.endpoint_states.read();

        let mut stale_digests = Vec::new();
        let mut updated_states = HashMap::new();

        for digest in &syn.digests {
            match states.get(&digest.endpoint) {
                Some(local_state) => {
                    let local_max = local_state.max_version();
                    if digest.max_version < local_max {
                        // We have newer data — send it back
                        let delta = self.extract_delta(local_state, digest.max_version);
                        updated_states.insert(digest.endpoint, delta);
                    } else if digest.max_version > local_max {
                        // They have newer data — request it
                        stale_digests.push(GossipDigest {
                            endpoint: digest.endpoint,
                            generation: digest.generation,
                            max_version: local_max,
                        });
                    }
                }
                None => {
                    // We don't know about this endpoint — request full state
                    stale_digests.push(GossipDigest {
                        endpoint: digest.endpoint,
                        generation: digest.generation,
                        max_version: 0,
                    });
                }
            }
        }

        // Also include endpoints we know about but they didn't mention
        for (ep, state) in states.iter() {
            if !syn.digests.iter().any(|d| d.endpoint == *ep) {
                let delta = self.extract_delta(state, 0);
                updated_states.insert(*ep, delta);
            }
        }

        GossipDigestAck {
            stale_digests,
            updated_states,
        }
    }

    /// Handle an incoming GossipDigestAck — apply remote state and produce ACK2.
    pub fn handle_ack(&self, ack: &GossipDigestAck) -> GossipDigestAck2 {
        // Apply the state updates from the ACK
        self.apply_remote_states(&ack.updated_states);

        // For each stale digest (states they need from us), send them
        let states = self.endpoint_states.read();
        let mut updated_states = HashMap::new();

        for digest in &ack.stale_digests {
            if let Some(local_state) = states.get(&digest.endpoint) {
                let delta = self.extract_delta(local_state, digest.max_version);
                updated_states.insert(digest.endpoint, delta);
            }
        }

        GossipDigestAck2 { updated_states }
    }

    /// Handle an incoming GossipDigestAck2 — apply final state deltas.
    pub fn handle_ack2(&self, ack2: &GossipDigestAck2) {
        self.apply_remote_states(&ack2.updated_states);
    }

    /// Apply state updates received from a remote peer.
    fn apply_remote_states(&self, remote_states: &HashMap<Endpoint, EndpointState>) {
        let mut states = self.endpoint_states.write();

        for (ep, remote_state) in remote_states {
            if *ep == self.local_endpoint {
                continue; // Never overwrite our own state
            }

            match states.get_mut(ep) {
                Some(local) => {
                    // Update if remote has higher generation or newer states
                    if remote_state.heartbeat.generation > local.heartbeat.generation {
                        *local = remote_state.clone();
                        local.update_timestamp = Some(Instant::now());
                    } else if remote_state.heartbeat.generation == local.heartbeat.generation {
                        // Same generation: merge application states
                        for (key, remote_val) in &remote_state.application_states {
                            match local.application_states.get(key) {
                                Some(local_val) if local_val.version >= remote_val.version => {
                                    // Our version is newer or equal, keep ours
                                }
                                _ => {
                                    local.set_state(*key, remote_val.clone());
                                }
                            }
                        }
                        if remote_state.heartbeat.version > local.heartbeat.version {
                            local.heartbeat.version = remote_state.heartbeat.version;
                        }
                    }
                }
                None => {
                    // New endpoint — add it
                    let mut new_state = remote_state.clone();
                    new_state.update_timestamp = Some(Instant::now());
                    states.insert(*ep, new_state);
                    info!(endpoint = %ep, "Discovered new node via gossip");
                }
            }

            // Report the heartbeat arrival to the failure detector
            self.failure_detector.write().report(*ep);
        }
    }

    /// Extract state delta: all application states with version > `since`.
    fn extract_delta(&self, state: &EndpointState, since: i64) -> EndpointState {
        let mut delta = EndpointState::new(state.heartbeat.generation);
        delta.heartbeat = state.heartbeat.clone();

        for (key, val) in &state.application_states {
            if val.version > since {
                delta.application_states.insert(*key, val.clone());
            }
        }

        delta
    }

    /// Select a random peer for the next gossip round.
    pub fn pick_gossip_target(&self) -> Option<Endpoint> {
        let mut candidates: Vec<Endpoint> = Vec::new();

        // Add live endpoints
        candidates.extend(self.live_endpoints.read().iter());

        // Occasionally gossip with a dead endpoint to detect recovery
        // Seed endpoints always get a chance
        for seed in self.seeds.seeds() {
            if *seed != self.local_endpoint && !candidates.contains(seed) {
                candidates.push(*seed);
            }
        }

        if candidates.is_empty() {
            // Bootstrap: pick a seed
            return self
                .seeds
                .seeds()
                .iter()
                .find(|s| **s != self.local_endpoint)
                .copied();
        }

        let mut rng = rand::thread_rng();
        candidates.choose(&mut rng).copied()
    }

    /// Run the failure detector against all known endpoints.
    ///
    /// Marks endpoints as live or dead based on phi values.
    pub fn assess_endpoint_health(&self) {
        let states = self.endpoint_states.read();
        let fd = self.failure_detector.read();

        let mut live = Vec::new();
        let mut dead = Vec::new();

        for ep in states.keys() {
            if *ep == self.local_endpoint {
                continue;
            }

            if fd.is_alive(ep) {
                live.push(*ep);
            } else {
                dead.push(*ep);
                debug!(endpoint = %ep, phi = fd.phi(ep), "Endpoint suspected dead");
            }
        }

        drop(states);
        drop(fd);

        *self.live_endpoints.write() = live;
        *self.dead_endpoints.write() = dead;
    }

    /// Get the current endpoint state for a given endpoint.
    pub fn get_endpoint_state(&self, endpoint: &Endpoint) -> Option<EndpointState> {
        self.endpoint_states.read().get(endpoint).cloned()
    }

    /// Get all known endpoint states (snapshot).
    pub fn all_endpoint_states(&self) -> HashMap<Endpoint, EndpointState> {
        self.endpoint_states.read().clone()
    }

    /// Get the list of currently live endpoints.
    pub fn live_endpoints(&self) -> Vec<Endpoint> {
        self.live_endpoints.read().clone()
    }

    /// Get the list of currently dead endpoints.
    pub fn dead_endpoints(&self) -> Vec<Endpoint> {
        self.dead_endpoints.read().clone()
    }

    /// Get the local endpoint.
    pub fn local_endpoint(&self) -> Endpoint {
        self.local_endpoint
    }

    /// Get the failure detector (for testing/metrics).
    pub fn failure_detector(&self) -> Arc<RwLock<FailureDetector>> {
        Arc::clone(&self.failure_detector)
    }

    /// Total number of known endpoints (including self).
    pub fn known_endpoint_count(&self) -> usize {
        self.endpoint_states.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn make_gossiper(port: u16, seeds: Vec<Endpoint>) -> Gossiper {
        Gossiper::new(ep(port), SeedProvider::new(seeds), 1)
    }

    #[test]
    fn initial_state() {
        let g = make_gossiper(7001, vec![ep(7002)]);
        assert_eq!(g.known_endpoint_count(), 1); // just self
        assert!(g.get_endpoint_state(&ep(7001)).is_some());
    }

    #[test]
    fn set_local_state() {
        let g = make_gossiper(7001, vec![]);
        g.set_local_state(ApplicationState::Status, "NORMAL".to_string());
        g.set_local_state(ApplicationState::Datacenter, "dc1".to_string());

        let state = g.get_endpoint_state(&ep(7001)).unwrap();
        assert_eq!(state.status(), Some("NORMAL"));
        assert_eq!(
            state.get_state(&ApplicationState::Datacenter).unwrap().value,
            "dc1"
        );
    }

    #[test]
    fn gossip_digest_syn() {
        let g = make_gossiper(7001, vec![]);
        g.set_local_state(ApplicationState::Status, "NORMAL".to_string());

        let syn = g.make_gossip_digest_syn();
        assert_eq!(syn.cluster_id, "cassandra-rust");
        assert_eq!(syn.digests.len(), 1);
        assert_eq!(syn.digests[0].endpoint, ep(7001));
    }

    #[test]
    fn syn_ack_ack2_exchange() {
        let g1 = make_gossiper(7001, vec![ep(7002)]);
        let g2 = make_gossiper(7002, vec![ep(7001)]);

        // G1 sets its state
        g1.set_local_state(ApplicationState::Status, "NORMAL".to_string());
        g1.set_local_state(ApplicationState::Datacenter, "dc1".to_string());

        // G2 sets its state
        g2.set_local_state(ApplicationState::Status, "NORMAL".to_string());
        g2.set_local_state(ApplicationState::Datacenter, "dc2".to_string());

        // G1 → G2: SYN
        let syn = g1.make_gossip_digest_syn();

        // G2 handles SYN, produces ACK
        let ack = g2.handle_syn(&syn);

        // G1 handles ACK, produces ACK2
        let ack2 = g1.handle_ack(&ack);

        // G2 handles ACK2
        g2.handle_ack2(&ack2);

        // Now both should know about each other
        assert_eq!(g1.known_endpoint_count(), 2);
        assert_eq!(g2.known_endpoint_count(), 2);

        // G1 should know G2's datacenter
        let g2_state_from_g1 = g1.get_endpoint_state(&ep(7002)).unwrap();
        assert_eq!(
            g2_state_from_g1
                .get_state(&ApplicationState::Datacenter)
                .unwrap()
                .value,
            "dc2"
        );

        // G2 should know G1's datacenter
        let g1_state_from_g2 = g2.get_endpoint_state(&ep(7001)).unwrap();
        assert_eq!(
            g1_state_from_g2
                .get_state(&ApplicationState::Datacenter)
                .unwrap()
                .value,
            "dc1"
        );
    }

    #[test]
    fn pick_gossip_target_returns_seed() {
        let g = make_gossiper(7001, vec![ep(7002), ep(7003)]);
        let target = g.pick_gossip_target();
        assert!(target.is_some());
        let t = target.unwrap();
        assert!(t == ep(7002) || t == ep(7003));
        assert_ne!(t, ep(7001)); // never self
    }

    #[test]
    fn convergence_three_nodes() {
        let g1 = make_gossiper(7001, vec![ep(7002), ep(7003)]);
        let g2 = make_gossiper(7002, vec![ep(7001), ep(7003)]);
        let g3 = make_gossiper(7003, vec![ep(7001), ep(7002)]);

        g1.set_local_state(ApplicationState::Datacenter, "dc1".to_string());
        g2.set_local_state(ApplicationState::Datacenter, "dc1".to_string());
        g3.set_local_state(ApplicationState::Datacenter, "dc2".to_string());

        // Round 1: G1 ↔ G2
        let syn = g1.make_gossip_digest_syn();
        let ack = g2.handle_syn(&syn);
        let ack2 = g1.handle_ack(&ack);
        g2.handle_ack2(&ack2);

        // Round 2: G2 ↔ G3
        let syn = g2.make_gossip_digest_syn();
        let ack = g3.handle_syn(&syn);
        let ack2 = g2.handle_ack(&ack);
        g3.handle_ack2(&ack2);

        // Round 3: G3 ↔ G1
        let syn = g3.make_gossip_digest_syn();
        let ack = g1.handle_syn(&syn);
        let ack2 = g3.handle_ack(&ack);
        g1.handle_ack2(&ack2);

        // All three should know about all three
        assert_eq!(g1.known_endpoint_count(), 3);
        assert_eq!(g2.known_endpoint_count(), 3);
        assert_eq!(g3.known_endpoint_count(), 3);

        // G1 should know G3's DC
        let g3_from_g1 = g1.get_endpoint_state(&ep(7003)).unwrap();
        assert_eq!(
            g3_from_g1
                .get_state(&ApplicationState::Datacenter)
                .unwrap()
                .value,
            "dc2"
        );
    }
}
