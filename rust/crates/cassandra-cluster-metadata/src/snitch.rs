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

//! Snitch: determines the datacenter and rack of endpoints.
//!
//! Snitches are used by `NetworkTopologyStrategy` to place replicas across
//! racks and datacenters.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.IEndpointSnitch`
//! - `org.apache.cassandra.locator.SimpleSnitch`
//! - `org.apache.cassandra.locator.PropertyFileSnitch`
//! - `org.apache.cassandra.locator.GossipingPropertyFileSnitch`
//! - `org.apache.cassandra.locator.DynamicEndpointSnitch`
//! - `org.apache.cassandra.locator.RackInferringSnitch`
//! - `org.apache.cassandra.locator.Ec2Snitch`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::node::Endpoint;

/// Determines the datacenter and rack for endpoints.
pub trait Snitch: Send + Sync {
    /// Returns the datacenter name for the given endpoint.
    fn datacenter(&self, endpoint: &Endpoint) -> String;

    /// Returns the rack name for the given endpoint.
    fn rack(&self, endpoint: &Endpoint) -> String;

    /// Sort endpoints by proximity to the source endpoint.
    /// Default implementation returns them in the original order.
    fn sort_by_proximity(&self, source: &Endpoint, endpoints: &mut [Endpoint]) {
        // Default: prefer same DC, then same rack
        let source_dc = self.datacenter(source);
        let source_rack = self.rack(source);

        endpoints.sort_by(|a, b| {
            let a_dc = self.datacenter(a);
            let b_dc = self.datacenter(b);
            let a_rack = self.rack(a);
            let b_rack = self.rack(b);

            let a_score = proximity_score(&source_dc, &source_rack, &a_dc, &a_rack);
            let b_score = proximity_score(&source_dc, &source_rack, &b_dc, &b_rack);

            a_score.cmp(&b_score)
        });
    }

    /// Returns the human-readable name for this snitch.
    fn snitch_name(&self) -> &'static str {
        "UnknownSnitch"
    }
}

fn proximity_score(src_dc: &str, src_rack: &str, dc: &str, rack: &str) -> u8 {
    if dc == src_dc && rack == src_rack {
        0 // same rack
    } else if dc == src_dc {
        1 // same DC, different rack
    } else {
        2 // different DC
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SimpleSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// Simple snitch: all nodes are in the same datacenter and rack.
///
/// Suitable for single-DC deployments and testing.
#[derive(Debug, Clone)]
pub struct SimpleSnitch;

impl Snitch for SimpleSnitch {
    fn datacenter(&self, _endpoint: &Endpoint) -> String {
        "datacenter1".to_string()
    }

    fn rack(&self, _endpoint: &Endpoint) -> String {
        "rack1".to_string()
    }

    fn snitch_name(&self) -> &'static str {
        "SimpleSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropertyFileSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// Property-file snitch: reads DC/rack assignments from a configuration map.
///
/// In production, this would parse cassandra-rackdc.properties.
/// Here we accept a pre-built map for flexibility.
#[derive(Debug, Clone)]
pub struct PropertyFileSnitch {
    /// endpoint → (datacenter, rack)
    topology: HashMap<Endpoint, (String, String)>,
    /// Default DC and rack for unknown endpoints.
    default_dc: String,
    default_rack: String,
}

impl PropertyFileSnitch {
    pub fn new(
        topology: HashMap<Endpoint, (String, String)>,
        default_dc: impl Into<String>,
        default_rack: impl Into<String>,
    ) -> Self {
        Self {
            topology,
            default_dc: default_dc.into(),
            default_rack: default_rack.into(),
        }
    }
}

impl Snitch for PropertyFileSnitch {
    fn datacenter(&self, endpoint: &Endpoint) -> String {
        self.topology
            .get(endpoint)
            .map(|(dc, _)| dc.clone())
            .unwrap_or_else(|| self.default_dc.clone())
    }

    fn rack(&self, endpoint: &Endpoint) -> String {
        self.topology
            .get(endpoint)
            .map(|(_, rack)| rack.clone())
            .unwrap_or_else(|| self.default_rack.clone())
    }

    fn snitch_name(&self) -> &'static str {
        "PropertyFileSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GossipingPropertyFileSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// GossipingPropertyFileSnitch: uses local config for the local node and
/// gossip-disseminated application states for remote nodes.
///
/// This is the **recommended** snitch for production multi-DC clusters.
/// The local node's DC/rack are read from config; remote nodes' DC/rack
/// arrive via gossip ApplicationState::Datacenter / ApplicationState::Rack.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.GossipingPropertyFileSnitch`
#[derive(Debug, Clone)]
pub struct GossipingPropertyFileSnitch {
    /// Local node's datacenter.
    local_dc: String,
    /// Local node's rack.
    local_rack: String,
    /// Local endpoint.
    local_endpoint: Endpoint,
    /// Remote nodes' DC/rack learned via gossip.
    /// Updated by the gossip subsystem when ApplicationState changes arrive.
    remote_topology: Arc<RwLock<HashMap<Endpoint, (String, String)>>>,
}

impl GossipingPropertyFileSnitch {
    /// Create from local config.
    pub fn new(
        local_endpoint: Endpoint,
        local_dc: impl Into<String>,
        local_rack: impl Into<String>,
    ) -> Self {
        Self {
            local_dc: local_dc.into(),
            local_rack: local_rack.into(),
            local_endpoint,
            remote_topology: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update a remote endpoint's topology (called from gossip when we
    /// receive ApplicationState::Datacenter / ApplicationState::Rack).
    pub fn update_remote(&self, endpoint: Endpoint, dc: String, rack: String) {
        self.remote_topology.write().insert(endpoint, (dc, rack));
    }

    /// Remove a remote endpoint (on decommission/removal).
    pub fn remove_remote(&self, endpoint: &Endpoint) {
        self.remote_topology.write().remove(endpoint);
    }
}

impl Snitch for GossipingPropertyFileSnitch {
    fn datacenter(&self, endpoint: &Endpoint) -> String {
        if *endpoint == self.local_endpoint {
            return self.local_dc.clone();
        }
        self.remote_topology
            .read()
            .get(endpoint)
            .map(|(dc, _)| dc.clone())
            .unwrap_or_else(|| self.local_dc.clone())
    }

    fn rack(&self, endpoint: &Endpoint) -> String {
        if *endpoint == self.local_endpoint {
            return self.local_rack.clone();
        }
        self.remote_topology
            .read()
            .get(endpoint)
            .map(|(_, rack)| rack.clone())
            .unwrap_or_else(|| self.local_rack.clone())
    }

    fn snitch_name(&self) -> &'static str {
        "GossipingPropertyFileSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DynamicEndpointSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// DynamicEndpointSnitch: wraps another snitch and reorders endpoints based
/// on measured latency (severity from gossip or direct measurements).
///
/// Periodically resets scores to allow slow nodes to recover.
/// Java uses a badness threshold (default 0.10 = 10%): a node with latency
/// only 10% worse won't be reordered.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.DynamicEndpointSnitch`
pub struct DynamicEndpointSnitch {
    /// Underlying static snitch.
    inner: Box<dyn Snitch>,
    /// Latency scores per endpoint (lower = better).
    scores: Arc<RwLock<HashMap<Endpoint, f64>>>,
    /// Severity scores from gossip (0.0 = healthy, > 0 = under pressure).
    severities: Arc<RwLock<HashMap<Endpoint, f64>>>,
    /// Badness threshold: if the score difference is less than this fraction,
    /// the original snitch order is preserved. Default 0.10 (10%).
    badness_threshold: f64,
    /// When scores were last reset.
    last_reset: Arc<RwLock<Instant>>,
    /// Reset interval for score decay.
    reset_interval: Duration,
}

impl DynamicEndpointSnitch {
    /// Create a new DynamicEndpointSnitch wrapping the given inner snitch.
    pub fn new(inner: Box<dyn Snitch>) -> Self {
        Self {
            inner,
            scores: Arc::new(RwLock::new(HashMap::new())),
            severities: Arc::new(RwLock::new(HashMap::new())),
            badness_threshold: 0.10,
            last_reset: Arc::new(RwLock::new(Instant::now())),
            reset_interval: Duration::from_secs(600), // 10 minutes
        }
    }

    /// Create with custom badness threshold and reset interval.
    pub fn with_config(
        inner: Box<dyn Snitch>,
        badness_threshold: f64,
        reset_interval: Duration,
    ) -> Self {
        Self {
            inner,
            scores: Arc::new(RwLock::new(HashMap::new())),
            severities: Arc::new(RwLock::new(HashMap::new())),
            badness_threshold,
            last_reset: Arc::new(RwLock::new(Instant::now())),
            reset_interval,
        }
    }

    /// Record a latency sample for an endpoint (in microseconds).
    pub fn record_latency(&self, endpoint: Endpoint, latency_us: f64) {
        let mut scores = self.scores.write();
        let current = scores.entry(endpoint).or_insert(0.0);
        // EWMA: weight new sample at 10%
        *current = *current * 0.9 + latency_us * 0.1;
    }

    /// Update severity for an endpoint (from gossip ApplicationState::Severity).
    pub fn update_severity(&self, endpoint: Endpoint, severity: f64) {
        self.severities.write().insert(endpoint, severity);
    }

    /// Effective score: latency + severity.
    fn effective_score(&self, endpoint: &Endpoint) -> f64 {
        let latency = self.scores.read().get(endpoint).copied().unwrap_or(0.0);
        let severity = self.severities.read().get(endpoint).copied().unwrap_or(0.0);
        latency + severity * 1000.0 // severity is 0-1, scale to match latency range
    }

    /// Reset all scores (called periodically to allow recovery).
    pub fn reset_scores(&self) {
        self.scores.write().clear();
        *self.last_reset.write() = Instant::now();
    }

    /// Check if scores need periodic reset.
    pub fn maybe_reset(&self) {
        if self.last_reset.read().elapsed() >= self.reset_interval {
            self.reset_scores();
        }
    }
}

impl Snitch for DynamicEndpointSnitch {
    fn datacenter(&self, endpoint: &Endpoint) -> String {
        self.inner.datacenter(endpoint)
    }

    fn rack(&self, endpoint: &Endpoint) -> String {
        self.inner.rack(endpoint)
    }

    fn sort_by_proximity(&self, source: &Endpoint, endpoints: &mut [Endpoint]) {
        self.maybe_reset();

        // First sort by inner snitch (static topology)
        self.inner.sort_by_proximity(source, endpoints);

        // Then re-sort using dynamic scores, respecting badness threshold
        endpoints.sort_by(|a, b| {
            let a_score = self.effective_score(a);
            let b_score = self.effective_score(b);

            // Only reorder if the difference exceeds the badness threshold
            if a_score == 0.0 && b_score == 0.0 {
                return std::cmp::Ordering::Equal;
            }

            let max_score = a_score.max(b_score);
            if max_score > 0.0 {
                let diff = (a_score - b_score).abs() / max_score;
                if diff < self.badness_threshold {
                    return std::cmp::Ordering::Equal;
                }
            }

            a_score
                .partial_cmp(&b_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    fn snitch_name(&self) -> &'static str {
        "DynamicEndpointSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RackInferringSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// RackInferringSnitch: infers DC and rack from the IP address octets.
///
/// - `datacenter` = 2nd octet
/// - `rack` = 3rd octet
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.RackInferringSnitch`
#[derive(Debug, Clone, Copy, Default)]
pub struct RackInferringSnitch;

impl Snitch for RackInferringSnitch {
    fn datacenter(&self, endpoint: &Endpoint) -> String {
        match endpoint.addr() {
            std::net::SocketAddr::V4(v4) => {
                let octets = v4.ip().octets();
                format!("dc{}", octets[1])
            }
            std::net::SocketAddr::V6(_) => "dc0".to_string(),
        }
    }

    fn rack(&self, endpoint: &Endpoint) -> String {
        match endpoint.addr() {
            std::net::SocketAddr::V4(v4) => {
                let octets = v4.ip().octets();
                format!("rack{}", octets[2])
            }
            std::net::SocketAddr::V6(_) => "rack0".to_string(),
        }
    }

    fn snitch_name(&self) -> &'static str {
        "RackInferringSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ec2Snitch / Ec2MultiRegionSnitch stubs
// ─────────────────────────────────────────────────────────────────────────────

/// Ec2Snitch: reads DC/rack from EC2 instance metadata API.
///
/// **Stub implementation** — returns defaults until the EC2 metadata
/// fetcher is wired. Gated behind feature flag for non-AWS builds.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.Ec2Snitch`
#[derive(Debug, Clone)]
pub struct Ec2Snitch {
    /// Cached region (datacenter).
    region: String,
    /// Cached availability zone (rack).
    availability_zone: String,
}

impl Ec2Snitch {
    /// Create with pre-fetched values (for testing/injection).
    pub fn new(region: impl Into<String>, az: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            availability_zone: az.into(),
        }
    }
}

impl Snitch for Ec2Snitch {
    fn datacenter(&self, _endpoint: &Endpoint) -> String {
        self.region.clone()
    }

    fn rack(&self, _endpoint: &Endpoint) -> String {
        self.availability_zone.clone()
    }

    fn snitch_name(&self) -> &'static str {
        "Ec2Snitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ec2MultiRegionSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// Ec2MultiRegionSnitch: like Ec2Snitch but uses public IPs for inter-DC
/// communication and private IPs within the same DC.
///
/// In a multi-region AWS deployment, nodes in the same region communicate
/// via private IPs (lower latency, no transfer costs), while nodes in
/// different regions use public IPs.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.Ec2MultiRegionSnitch`
#[derive(Debug, Clone)]
pub struct Ec2MultiRegionSnitch {
    /// Local node's region (datacenter).
    region: String,
    /// Local node's availability zone (rack).
    availability_zone: String,
    /// Private (local) endpoint for intra-DC.
    local_address: Endpoint,
    /// Public (broadcast) endpoint for inter-DC.
    broadcast_address: Endpoint,
    /// Remote endpoint → (DC, rack) learned via gossip/config.
    remote_topology: Arc<RwLock<HashMap<Endpoint, (String, String)>>>,
}

impl Ec2MultiRegionSnitch {
    /// Create with explicit region/AZ and local/broadcast addresses.
    pub fn new(
        region: impl Into<String>,
        az: impl Into<String>,
        local_address: Endpoint,
        broadcast_address: Endpoint,
    ) -> Self {
        Self {
            region: region.into(),
            availability_zone: az.into(),
            local_address,
            broadcast_address,
            remote_topology: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update remote DC/rack info (from gossip or EC2 metadata).
    pub fn update_remote(&self, endpoint: Endpoint, dc: String, rack: String) {
        self.remote_topology.write().insert(endpoint, (dc, rack));
    }

    /// The local (private) address.
    pub fn local_address(&self) -> Endpoint {
        self.local_address
    }

    /// The broadcast (public) address.
    pub fn broadcast_address(&self) -> Endpoint {
        self.broadcast_address
    }
}

impl Snitch for Ec2MultiRegionSnitch {
    fn datacenter(&self, endpoint: &Endpoint) -> String {
        if *endpoint == self.local_address || *endpoint == self.broadcast_address {
            return self.region.clone();
        }
        self.remote_topology
            .read()
            .get(endpoint)
            .map(|(dc, _)| dc.clone())
            .unwrap_or_else(|| self.region.clone())
    }

    fn rack(&self, endpoint: &Endpoint) -> String {
        if *endpoint == self.local_address || *endpoint == self.broadcast_address {
            return self.availability_zone.clone();
        }
        self.remote_topology
            .read()
            .get(endpoint)
            .map(|(_, rack)| rack.clone())
            .unwrap_or_else(|| self.availability_zone.clone())
    }

    fn snitch_name(&self) -> &'static str {
        "Ec2MultiRegionSnitch"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GoogleCloudSnitch
// ─────────────────────────────────────────────────────────────────────────────

/// GoogleCloudSnitch: reads DC/rack from GCP metadata API.
///
/// **Stub implementation** — returns pre-configured values until the GCE
/// metadata service is wired. Gated behind feature flag for non-GCP builds.
///
/// Convention: project-region = datacenter, zone = rack.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.GoogleCloudSnitch`
#[derive(Debug, Clone)]
pub struct GoogleCloudSnitch {
    /// Cached region (datacenter), e.g., "us-central1".
    region: String,
    /// Cached zone (rack), e.g., "us-central1-a".
    zone: String,
}

impl GoogleCloudSnitch {
    /// Create with pre-fetched values (for testing/injection).
    pub fn new(region: impl Into<String>, zone: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            zone: zone.into(),
        }
    }
}

impl Snitch for GoogleCloudSnitch {
    fn datacenter(&self, _endpoint: &Endpoint) -> String {
        self.region.clone()
    }

    fn rack(&self, _endpoint: &Endpoint) -> String {
        self.zone.clone()
    }

    fn snitch_name(&self) -> &'static str {
        "GoogleCloudSnitch"
    }
}

/// Factory: create a snitch by name.
pub fn create_snitch(name: &str) -> Box<dyn Snitch> {
    if name.contains("SimpleSnitch") || name == "simple" {
        Box::new(SimpleSnitch)
    } else if name.contains("RackInferringSnitch") || name == "rackinferring" {
        Box::new(RackInferringSnitch)
    } else if name.contains("Ec2MultiRegionSnitch") || name == "ec2multiregion" {
        // Defaults for stub; real deployment would fetch from metadata API
        Box::new(Ec2Snitch::new("us-east-1", "us-east-1a"))
    } else if name.contains("Ec2Snitch") || name == "ec2" {
        Box::new(Ec2Snitch::new("us-east-1", "us-east-1a"))
    } else if name.contains("GoogleCloudSnitch") || name == "googlecloud" {
        Box::new(GoogleCloudSnitch::new("us-central1", "us-central1-a"))
    } else {
        // Default to SimpleSnitch
        Box::new(SimpleSnitch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    fn ep_ip(a: u8, b: u8, c: u8, d: u8) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(a, b, c, d),
            7000,
        )))
    }

    #[test]
    fn simple_snitch_single_dc() {
        let snitch = SimpleSnitch;
        assert_eq!(snitch.datacenter(&ep(7001)), "datacenter1");
        assert_eq!(snitch.rack(&ep(7001)), "rack1");
        // All endpoints same DC/rack
        assert_eq!(snitch.datacenter(&ep(7002)), snitch.datacenter(&ep(7003)));
    }

    #[test]
    fn property_file_snitch() {
        let mut topology = HashMap::new();
        topology.insert(ep(7001), ("us-east".to_string(), "rack-a".to_string()));
        topology.insert(ep(7002), ("us-east".to_string(), "rack-b".to_string()));
        topology.insert(ep(7003), ("eu-west".to_string(), "rack-a".to_string()));

        let snitch = PropertyFileSnitch::new(topology, "unknown-dc", "unknown-rack");

        assert_eq!(snitch.datacenter(&ep(7001)), "us-east");
        assert_eq!(snitch.rack(&ep(7001)), "rack-a");
        assert_eq!(snitch.datacenter(&ep(7003)), "eu-west");

        // Unknown endpoint gets default
        assert_eq!(snitch.datacenter(&ep(9999)), "unknown-dc");
        assert_eq!(snitch.rack(&ep(9999)), "unknown-rack");
    }

    #[test]
    fn sort_by_proximity() {
        let mut topology = HashMap::new();
        topology.insert(ep(7001), ("dc1".to_string(), "rack1".to_string()));
        topology.insert(ep(7002), ("dc1".to_string(), "rack2".to_string()));
        topology.insert(ep(7003), ("dc2".to_string(), "rack1".to_string()));

        let snitch = PropertyFileSnitch::new(topology, "dc1", "rack1");

        let source = ep(7001); // dc1/rack1
        let mut endpoints = vec![ep(7003), ep(7002), ep(7001)];
        snitch.sort_by_proximity(&source, &mut endpoints);

        // ep(7001) = same rack (0), ep(7002) = same DC (1), ep(7003) = different DC (2)
        assert_eq!(endpoints[0], ep(7001));
        assert_eq!(endpoints[1], ep(7002));
        assert_eq!(endpoints[2], ep(7003));
    }

    #[test]
    fn gossiping_property_file_snitch_local() {
        let snitch = GossipingPropertyFileSnitch::new(ep(7001), "dc1", "rack1");
        assert_eq!(snitch.datacenter(&ep(7001)), "dc1");
        assert_eq!(snitch.rack(&ep(7001)), "rack1");
    }

    #[test]
    fn gossiping_property_file_snitch_remote() {
        let snitch = GossipingPropertyFileSnitch::new(ep(7001), "dc1", "rack1");

        // Initially unknown remote → falls back to local DC/rack
        assert_eq!(snitch.datacenter(&ep(7002)), "dc1");

        // Update remote via gossip
        snitch.update_remote(ep(7002), "dc2".to_string(), "rack-b".to_string());
        assert_eq!(snitch.datacenter(&ep(7002)), "dc2");
        assert_eq!(snitch.rack(&ep(7002)), "rack-b");

        // Remove remote
        snitch.remove_remote(&ep(7002));
        assert_eq!(snitch.datacenter(&ep(7002)), "dc1");
    }

    #[test]
    fn dynamic_snitch_latency_ordering() {
        let inner = PropertyFileSnitch::new(HashMap::new(), "dc1", "rack1");
        let snitch = DynamicEndpointSnitch::new(Box::new(inner));

        // Record latencies: ep(7001) fast, ep(7002) slow
        for _ in 0..20 {
            snitch.record_latency(ep(7001), 100.0);
            snitch.record_latency(ep(7002), 5000.0);
        }

        let mut endpoints = vec![ep(7002), ep(7001)];
        snitch.sort_by_proximity(&ep(9999), &mut endpoints);

        // ep(7001) should come first (lower latency)
        assert_eq!(endpoints[0], ep(7001));
        assert_eq!(endpoints[1], ep(7002));
    }

    #[test]
    fn dynamic_snitch_badness_threshold() {
        let inner = PropertyFileSnitch::new(HashMap::new(), "dc1", "rack1");
        let snitch =
            DynamicEndpointSnitch::with_config(Box::new(inner), 0.10, Duration::from_secs(600));

        // Record very similar latencies (< 10% difference)
        for _ in 0..20 {
            snitch.record_latency(ep(7001), 100.0);
            snitch.record_latency(ep(7002), 105.0);
        }

        // Should preserve original order (diff < badness threshold)
        let mut endpoints = vec![ep(7002), ep(7001)];
        snitch.sort_by_proximity(&ep(9999), &mut endpoints);
        // Order should be preserved since diff is < 10%
        assert_eq!(endpoints[0], ep(7002));
        assert_eq!(endpoints[1], ep(7001));
    }

    #[test]
    fn dynamic_snitch_reset() {
        let inner = PropertyFileSnitch::new(HashMap::new(), "dc1", "rack1");
        let snitch = DynamicEndpointSnitch::new(Box::new(inner));

        snitch.record_latency(ep(7001), 5000.0);
        assert!(snitch.effective_score(&ep(7001)) > 0.0);

        snitch.reset_scores();
        assert_eq!(snitch.effective_score(&ep(7001)), 0.0);
    }

    #[test]
    fn dynamic_snitch_severity() {
        let inner = PropertyFileSnitch::new(HashMap::new(), "dc1", "rack1");
        let snitch = DynamicEndpointSnitch::new(Box::new(inner));

        snitch.update_severity(ep(7001), 0.5);
        // Severity 0.5 * 1000 = 500
        assert!(snitch.effective_score(&ep(7001)) > 400.0);
    }

    #[test]
    fn rack_inferring_snitch() {
        let snitch = RackInferringSnitch;
        // 10.1.2.3 → dc1, rack2
        assert_eq!(snitch.datacenter(&ep_ip(10, 1, 2, 3)), "dc1");
        assert_eq!(snitch.rack(&ep_ip(10, 1, 2, 3)), "rack2");

        // 10.5.10.3 → dc5, rack10
        assert_eq!(snitch.datacenter(&ep_ip(10, 5, 10, 3)), "dc5");
        assert_eq!(snitch.rack(&ep_ip(10, 5, 10, 3)), "rack10");
    }

    #[test]
    fn ec2_snitch() {
        let snitch = Ec2Snitch::new("us-west-2", "us-west-2a");
        assert_eq!(snitch.datacenter(&ep(7001)), "us-west-2");
        assert_eq!(snitch.rack(&ep(7001)), "us-west-2a");
    }

    #[test]
    fn ec2_multi_region_snitch_local() {
        let local = ep(7001);
        let broadcast = ep(7002);
        let snitch = Ec2MultiRegionSnitch::new("us-east-1", "us-east-1a", local, broadcast);

        // Local and broadcast should return our region
        assert_eq!(snitch.datacenter(&local), "us-east-1");
        assert_eq!(snitch.rack(&local), "us-east-1a");
        assert_eq!(snitch.datacenter(&broadcast), "us-east-1");
    }

    #[test]
    fn ec2_multi_region_snitch_remote() {
        let local = ep(7001);
        let broadcast = ep(7002);
        let snitch = Ec2MultiRegionSnitch::new("us-east-1", "us-east-1a", local, broadcast);

        // Update remote topology
        snitch.update_remote(ep(7003), "eu-west-1".into(), "eu-west-1b".into());

        assert_eq!(snitch.datacenter(&ep(7003)), "eu-west-1");
        assert_eq!(snitch.rack(&ep(7003)), "eu-west-1b");

        // Unknown remote falls back to local region
        assert_eq!(snitch.datacenter(&ep(7099)), "us-east-1");
    }

    #[test]
    fn google_cloud_snitch() {
        let snitch = GoogleCloudSnitch::new("us-central1", "us-central1-a");
        assert_eq!(snitch.datacenter(&ep(7001)), "us-central1");
        assert_eq!(snitch.rack(&ep(7001)), "us-central1-a");
        assert_eq!(snitch.snitch_name(), "GoogleCloudSnitch");
    }

    #[test]
    fn snitch_factory() {
        let s = create_snitch("SimpleSnitch");
        assert_eq!(s.snitch_name(), "SimpleSnitch");

        let s = create_snitch("rackinferring");
        assert_eq!(s.snitch_name(), "RackInferringSnitch");

        let s = create_snitch("googlecloud");
        assert_eq!(s.snitch_name(), "GoogleCloudSnitch");

        let s = create_snitch("ec2");
        assert_eq!(s.snitch_name(), "Ec2Snitch");
    }
}
