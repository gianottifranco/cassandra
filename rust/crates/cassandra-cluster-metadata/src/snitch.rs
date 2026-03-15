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

use std::collections::HashMap;

use crate::node::Endpoint;

/// Determines the datacenter and rack for endpoints.
pub trait Snitch: Send + Sync {
    /// Returns the datacenter name for the given endpoint.
    fn datacenter(&self, endpoint: &Endpoint) -> String;

    /// Returns the rack name for the given endpoint.
    fn rack(&self, endpoint: &Endpoint) -> String;

    /// Sort endpoints by proximity to the source endpoint.
    /// Default implementation returns them in the original order.
    fn sort_by_proximity(
        &self,
        source: &Endpoint,
        endpoints: &mut [Endpoint],
    ) {
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
}

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
}
