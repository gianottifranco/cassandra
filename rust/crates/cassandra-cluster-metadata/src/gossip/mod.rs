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

use std::collections::{BTreeMap, HashMap, HashSet};

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use tracing::{debug, info, warn};
use crate::node::Endpoint;
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

    // ── New in Cassandra 4.x+ ──────────────────────────────────────────
    /// Status with port (STATUS_WITH_PORT). Carries the same info as
    /// Status but includes port numbers for mixed-version clusters.
    StatusWithPort,
    /// Native transport address with port.
    NativeAddressAndPort,
    /// Internode messaging version for version negotiation.
    NetVersion,
    /// Index build status (for SAI/2i).
    IndexStatus,
    /// SSTable version capability.
    SstableVersions,
    /// Disk usage percentage.
    DiskUsage,
    /// Current TCM (Transactional Cluster Metadata) epoch.
    /// Used for epoch dissemination and peer catch-up detection.
    TcmEpoch,
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

    /// Returns the schema version if set.
    pub fn schema_version(&self) -> Option<&str> {
        self.get_state(&ApplicationState::SchemaVersion)
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

/// Quarantine delay: how long to ignore a node after it's been marked down/removed.
const QUARANTINE_DELAY: Duration = Duration::from_secs(60);

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
    /// Quarantined endpoints (endpoint → quarantine expiry).
    quarantined: Arc<RwLock<HashMap<Endpoint, Instant>>>,
    /// Whether the gossiper is running.
    is_running: Arc<std::sync::atomic::AtomicBool>,
    /// Cluster name for SYN validation.
    cluster_name: String,
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
            quarantined: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cluster_name: "cassandra-rust".to_string(),
        }
    }

    /// Create a new Gossiper with a custom cluster name.
    pub fn with_cluster_name(
        local_endpoint: Endpoint,
        seeds: SeedProvider,
        generation: i64,
        cluster_name: impl Into<String>,
    ) -> Self {
        let mut g = Self::new(local_endpoint, seeds, generation);
        g.cluster_name = cluster_name.into();
        g
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

    /// Update a remote endpoint's application state.
    ///
    /// Used when receiving state from the TCM bridge or when setting
    /// state on behalf of a known remote peer (e.g., during testing).
    pub fn update_remote_state(
        &self,
        endpoint: Endpoint,
        key: ApplicationState,
        value: VersionedValue,
    ) {
        let mut states = self.endpoint_states.write();
        let state = states
            .entry(endpoint)
            .or_insert_with(|| EndpointState::new(1));
        state.set_state(key, value);

        // Ensure it appears in live_endpoints
        drop(states);
        let mut live = self.live_endpoints.write();
        if !live.contains(&endpoint) {
            live.push(endpoint);
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
            cluster_id: self.cluster_name.clone(),
            digests,
        }
    }

    /// Handle an incoming GossipDigestSyn — produce an ACK.
    pub fn handle_syn(&self, syn: &GossipDigestSyn) -> GossipDigestAck {
        let states = self.endpoint_states.read();

        let mut stale_digests = Vec::new();
        let mut updated_states = HashMap::new();

        for digest in &syn.digests {
            // Skip quarantined endpoints
            if self.is_quarantined(&digest.endpoint) {
                continue;
            }

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
                if !self.is_quarantined(ep) {
                    let delta = self.extract_delta(state, 0);
                    updated_states.insert(*ep, delta);
                }
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

            if self.is_quarantined(ep) {
                debug!(endpoint = %ep, "Skipping quarantined endpoint");
                continue;
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

            if self.is_quarantined(ep) {
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

    // ── Shutdown / Quarantine / Convict ─────────────────────────────────

    /// Graceful shutdown: mark this node as leaving and stop gossiping.
    pub fn shutdown(&self) {
        self.set_local_state(ApplicationState::Status, "LEFT".to_string());
        self.is_running.store(false, std::sync::atomic::Ordering::SeqCst);
        info!(endpoint = %self.local_endpoint, "Gossiper shutdown");
    }

    /// Returns `true` if the gossiper is actively running.
    pub fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Quarantine an endpoint: ignore its gossip for the specified duration.
    pub fn quarantine(&self, endpoint: Endpoint) {
        let expiry = Instant::now() + QUARANTINE_DELAY;
        self.quarantined.write().insert(endpoint, expiry);
        info!(endpoint = %endpoint, "Quarantined endpoint");
    }

    /// Check if an endpoint is currently quarantined.
    pub fn is_quarantined(&self, endpoint: &Endpoint) -> bool {
        let quarantined = self.quarantined.read();
        if let Some(&expiry) = quarantined.get(endpoint) {
            if Instant::now() < expiry {
                return true;
            }
        }
        false
    }

    /// Evict expired quarantine entries.
    pub fn cleanup_quarantine(&self) {
        let now = Instant::now();
        self.quarantined.write().retain(|_, expiry| *expiry > now);
    }

    /// Convict an endpoint: forcefully mark as dead regardless of phi.
    pub fn convict(&self, endpoint: &Endpoint) {
        // Remove from live, add to dead
        self.live_endpoints.write().retain(|e| e != endpoint);
        let mut dead = self.dead_endpoints.write();
        if !dead.contains(endpoint) {
            dead.push(*endpoint);
        }
        warn!(endpoint = %endpoint, "Endpoint convicted (forcefully marked dead)");
    }

    /// Reintegrate a previously-dead endpoint (e.g. after restart with new generation).
    pub fn reintegrate(&self, endpoint: &Endpoint) {
        // Remove from dead, add to live
        self.dead_endpoints.write().retain(|e| e != endpoint);
        let mut live = self.live_endpoints.write();
        if !live.contains(endpoint) {
            live.push(*endpoint);
        }
        // Clear quarantine
        self.quarantined.write().remove(endpoint);
        info!(endpoint = %endpoint, "Endpoint reintegrated (marked alive)");
    }

    // ── Schema Agreement ───────────────────────────────────────────────

    /// Check if all live endpoints agree on the schema version.
    ///
    /// Returns `true` if every live endpoint has the same SchemaVersion
    /// application state, or there are no other live endpoints.
    pub fn assess_schema_agreement(&self) -> bool {
        let states = self.endpoint_states.read();
        let live = self.live_endpoints.read();

        let mut schema_versions: HashSet<String> = HashSet::new();

        // Include our own schema version
        if let Some(local) = states.get(&self.local_endpoint) {
            if let Some(sv) = local.get_state(&ApplicationState::SchemaVersion) {
                schema_versions.insert(sv.value.clone());
            }
        }

        // Include all live endpoints' schema versions
        for ep in live.iter() {
            if let Some(state) = states.get(ep) {
                if let Some(sv) = state.get_state(&ApplicationState::SchemaVersion) {
                    schema_versions.insert(sv.value.clone());
                }
            }
        }

        schema_versions.len() <= 1
    }

    /// Get map of schema_version → endpoints for schema disagreement debugging.
    pub fn schema_version_map(&self) -> HashMap<String, Vec<Endpoint>> {
        let states = self.endpoint_states.read();
        let mut map: HashMap<String, Vec<Endpoint>> = HashMap::new();

        for (ep, state) in states.iter() {
            if let Some(sv) = state.get_state(&ApplicationState::SchemaVersion) {
                map.entry(sv.value.clone()).or_default().push(*ep);
            }
        }

        map
    }

    // ── Accessors ──────────────────────────────────────────────────────

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

    // ── Gossip Hardening ───────────────────────────────────────────────

    /// Probabilistically probe dead endpoints to detect recovery.
    ///
    /// Each gossip round, with probability `1 / (dead_count + 1)`, pick a
    /// dead node and gossip to it. This ensures dead nodes that have restarted
    /// are eventually rediscovered, while avoiding flooding all dead nodes.
    ///
    /// ## Java Oracle
    ///
    /// `Gossiper.maybeGossipToUnreachableMember()`
    pub fn maybe_probe_dead_endpoints(&self) -> Option<Endpoint> {
        let dead = self.dead_endpoints.read().clone();
        if dead.is_empty() {
            return None;
        }

        // Probabilistic: probe with probability 1/(dead_count + 1) per round
        let mut rng = rand::thread_rng();
        use rand::Rng;
        let probe: f64 = rng.gen_range(0.0..1.0);
        if probe < 1.0 / (dead.len() as f64 + 1.0) {
            dead.choose(&mut rng).copied()
        } else {
            None
        }
    }

    /// Shadow round: discover the existing cluster before bootstrapping.
    ///
    /// Sends a SYN to all seeds and collects responses WITHOUT applying
    /// them to the live endpoint list. This lets a new node learn what
    /// the cluster looks like before announcing itself.
    ///
    /// Returns the set of endpoints discovered during the shadow round.
    ///
    /// ## Java Oracle
    ///
    /// `Gossiper.doShadowRound()`
    pub fn do_shadow_round(&self) -> Vec<Endpoint> {
        let syn = self.make_gossip_digest_syn();
        let mut discovered = Vec::new();

        // In a real implementation, we'd send SYNs over the network.
        // Here we build the digest for callers to send and collect responses.
        // The shadow round does NOT update live/dead lists — it only collects state.
        for digest in &syn.digests {
            if digest.endpoint != self.local_endpoint {
                discovered.push(digest.endpoint);
            }
        }

        // Also include all seeds we know about
        for seed in self.seeds.seeds() {
            if *seed != self.local_endpoint && !discovered.contains(seed) {
                discovered.push(*seed);
            }
        }

        discovered
    }

    /// Handle a generation change: when a remote node restarts with a new
    /// generation, reset all of its application state and report to the
    /// failure detector.
    ///
    /// This is critical for correctness: stale state from a previous
    /// incarnation must be discarded.
    ///
    /// ## Java Oracle
    ///
    /// `Gossiper.handleMajorStateChange()`
    pub fn handle_generation_change(
        &self,
        endpoint: Endpoint,
        new_generation: i64,
        initial_states: BTreeMap<ApplicationState, VersionedValue>,
    ) {
        if endpoint == self.local_endpoint {
            return; // Never reset our own state this way
        }

        let mut states = self.endpoint_states.write();
        let mut new_state = EndpointState::new(new_generation);
        for (key, val) in initial_states {
            new_state.set_state(key, val);
        }
        states.insert(endpoint, new_state);

        drop(states);

        // Clear quarantine so the restarted node can gossip
        self.quarantined.write().remove(&endpoint);

        // Report heartbeat to failure detector
        self.failure_detector.write().report(endpoint);

        // Move from dead to live
        self.reintegrate(&endpoint);

        info!(
            endpoint = %endpoint,
            generation = new_generation,
            "Handled generation change (node restarted)"
        );
    }

    /// Gossip to an unreachable member: attempt to send state to a node
    /// we haven't heard from, to help it catch up or to detect its recovery.
    ///
    /// Returns the target endpoint if one was selected.
    ///
    /// ## Java Oracle
    ///
    /// `Gossiper.gossipToUnreachableMember()`
    pub fn gossip_to_unreachable_member(&self) -> Option<Endpoint> {
        let dead = self.dead_endpoints.read().clone();
        if dead.is_empty() {
            return None;
        }
        let mut rng = rand::thread_rng();
        dead.choose(&mut rng).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

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

    #[test]
    fn shutdown_marks_left() {
        let g = make_gossiper(7001, vec![]);
        assert!(g.is_running());
        g.shutdown();
        assert!(!g.is_running());
        let state = g.get_endpoint_state(&ep(7001)).unwrap();
        assert_eq!(state.status(), Some("LEFT"));
    }

    #[test]
    fn quarantine_blocks_gossip() {
        let g1 = make_gossiper(7001, vec![ep(7002)]);
        let g2 = make_gossiper(7002, vec![ep(7001)]);

        g2.set_local_state(ApplicationState::Status, "NORMAL".to_string());

        // Quarantine ep(7002) on g1
        g1.quarantine(ep(7002));
        assert!(g1.is_quarantined(&ep(7002)));

        // G2 gossips to G1 — but G1 should skip the quarantined endpoint
        let syn = g2.make_gossip_digest_syn();
        let _ack = g1.handle_syn(&syn);

        // G1 should not have learned about G2 (quarantined)
        // The SYN would have G2's state in digests, but G1 skips it
        assert_eq!(g1.known_endpoint_count(), 1);
    }

    #[test]
    fn convict_and_reintegrate() {
        let g = make_gossiper(7001, vec![ep(7002)]);

        // Simulate ep(7002) being live
        g.live_endpoints.write().push(ep(7002));
        assert_eq!(g.live_endpoints().len(), 1);

        // Convict
        g.convict(&ep(7002));
        assert!(g.live_endpoints().is_empty());
        assert_eq!(g.dead_endpoints().len(), 1);

        // Reintegrate
        g.reintegrate(&ep(7002));
        assert_eq!(g.live_endpoints().len(), 1);
        assert!(g.dead_endpoints().is_empty());
    }

    #[test]
    fn schema_agreement_all_same() {
        let g = make_gossiper(7001, vec![ep(7002)]);
        g.set_local_state(ApplicationState::SchemaVersion, "uuid-1".to_string());

        // Add ep(7002) with same schema version
        {
            let mut states = g.endpoint_states.write();
            let mut state = EndpointState::new(1);
            state.set_state(
                ApplicationState::SchemaVersion,
                VersionedValue::new(1, "uuid-1"),
            );
            states.insert(ep(7002), state);
        }
        g.live_endpoints.write().push(ep(7002));

        assert!(g.assess_schema_agreement());
    }

    #[test]
    fn schema_disagreement() {
        let g = make_gossiper(7001, vec![ep(7002)]);
        g.set_local_state(ApplicationState::SchemaVersion, "uuid-1".to_string());

        // Add ep(7002) with DIFFERENT schema version
        {
            let mut states = g.endpoint_states.write();
            let mut state = EndpointState::new(1);
            state.set_state(
                ApplicationState::SchemaVersion,
                VersionedValue::new(1, "uuid-2"),
            );
            states.insert(ep(7002), state);
        }
        g.live_endpoints.write().push(ep(7002));

        assert!(!g.assess_schema_agreement());

        let map = g.schema_version_map();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn new_application_states() {
        let g = make_gossiper(7001, vec![]);
        g.set_local_state(ApplicationState::StatusWithPort, "NORMAL,7001".to_string());
        g.set_local_state(ApplicationState::NativeAddressAndPort, "127.0.0.1:9042".to_string());
        g.set_local_state(ApplicationState::NetVersion, "12".to_string());
        g.set_local_state(ApplicationState::DiskUsage, "45".to_string());

        let state = g.get_endpoint_state(&ep(7001)).unwrap();
        assert_eq!(
            state.get_state(&ApplicationState::StatusWithPort).unwrap().value,
            "NORMAL,7001"
        );
        assert_eq!(
            state.get_state(&ApplicationState::NetVersion).unwrap().value,
            "12"
        );
    }

    #[test]
    fn convergence_six_nodes_multi_dc() {
        // 3 nodes in dc1, 3 nodes in dc2
        let nodes: Vec<Gossiper> = (0..6)
            .map(|i| {
                let port = 7001 + i as u16;
                let seeds: Vec<Endpoint> = (0..6)
                    .filter(|j| *j != i)
                    .map(|j| ep(7001 + j as u16))
                    .collect();
                let g = make_gossiper(port, seeds);
                let dc = if i < 3 { "dc1" } else { "dc2" };
                g.set_local_state(ApplicationState::Datacenter, dc.to_string());
                g.set_local_state(ApplicationState::Status, "NORMAL".to_string());
                g
            })
            .collect();

        // Run 3 full rounds of gossip between consecutive pairs
        for _round in 0..3 {
            for i in 0..6 {
                let j = (i + 1) % 6;
                let syn = nodes[i].make_gossip_digest_syn();
                let ack = nodes[j].handle_syn(&syn);
                let ack2 = nodes[i].handle_ack(&ack);
                nodes[j].handle_ack2(&ack2);
            }
        }

        // All six should know about all six
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                n.known_endpoint_count(),
                6,
                "Node {} should know about all 6 nodes, knows {}",
                i,
                n.known_endpoint_count()
            );
        }

        // Node 0 (dc1) should know node 5 is in dc2
        let state = nodes[0].get_endpoint_state(&ep(7006)).unwrap();
        assert_eq!(
            state.get_state(&ApplicationState::Datacenter).unwrap().value,
            "dc2"
        );
    }

    // ── Gossip Hardening Tests ──────────────────────────────────────────

    #[test]
    fn probe_dead_returns_none_when_all_alive() {
        let g = make_gossiper(7001, vec![ep(7002)]);
        // No dead endpoints → no probe target
        assert!(g.maybe_probe_dead_endpoints().is_none());
    }

    #[test]
    fn probe_dead_may_return_dead_endpoint() {
        let g = make_gossiper(7001, vec![ep(7002)]);
        g.dead_endpoints.write().push(ep(7002));

        // Run multiple times — eventually should probe
        let mut probed = false;
        for _ in 0..100 {
            if g.maybe_probe_dead_endpoints().is_some() {
                probed = true;
                break;
            }
        }
        assert!(probed, "Should eventually probe a dead endpoint");
    }

    #[test]
    fn shadow_round_discovers_seeds() {
        let g = make_gossiper(7001, vec![ep(7002), ep(7003)]);
        let discovered = g.do_shadow_round();
        assert!(discovered.contains(&ep(7002)));
        assert!(discovered.contains(&ep(7003)));
        assert!(!discovered.contains(&ep(7001))); // not self
    }

    #[test]
    fn generation_change_resets_state() {
        let g = make_gossiper(7001, vec![ep(7002)]);

        // Simulate ep(7002) having old state at generation 1
        {
            let mut states = g.endpoint_states.write();
            let mut old_state = EndpointState::new(1);
            old_state.set_state(
                ApplicationState::Datacenter,
                VersionedValue::new(1, "old-dc"),
            );
            states.insert(ep(7002), old_state);
        }
        g.dead_endpoints.write().push(ep(7002));

        // Handle generation change (restart at generation 2)
        let mut new_states = BTreeMap::new();
        new_states.insert(
            ApplicationState::Datacenter,
            VersionedValue::new(1, "new-dc"),
        );
        g.handle_generation_change(ep(7002), 2, new_states);

        // State should be reset to new generation
        let state = g.get_endpoint_state(&ep(7002)).unwrap();
        assert_eq!(state.heartbeat.generation, 2);
        assert_eq!(
            state.get_state(&ApplicationState::Datacenter).unwrap().value,
            "new-dc"
        );

        // Should be reintegrated (live, not dead)
        assert!(g.live_endpoints().contains(&ep(7002)));
        assert!(!g.dead_endpoints().contains(&ep(7002)));
    }

    #[test]
    fn gossip_to_unreachable_returns_none_when_all_alive() {
        let g = make_gossiper(7001, vec![]);
        assert!(g.gossip_to_unreachable_member().is_none());
    }

    #[test]
    fn gossip_to_unreachable_returns_dead_node() {
        let g = make_gossiper(7001, vec![]);
        g.dead_endpoints.write().push(ep(7002));
        let target = g.gossip_to_unreachable_member();
        assert_eq!(target, Some(ep(7002)));
    }
}
