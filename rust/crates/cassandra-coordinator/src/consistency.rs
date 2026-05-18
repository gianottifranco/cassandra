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

//! Consistency level definitions and enforcement helpers.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.ConsistencyLevel`
//! - `org.apache.cassandra.service.AbstractWriteResponseHandler`

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// CQL consistency levels.
///
/// Defines how many replica responses are required before a coordinator
/// returns success to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Wait for one replica.
    One,
    /// Wait for two replicas.
    Two,
    /// Wait for three replicas.
    Three,
    /// Wait for `floor(RF/2) + 1` replicas.
    Quorum,
    /// Wait for all replicas.
    All,
    /// Wait for `floor(local_RF/2) + 1` replicas in the local datacenter.
    LocalQuorum,
    /// Wait for quorum in each datacenter.
    EachQuorum,
    /// Serial consistency (for LWT).
    Serial,
    /// Local serial consistency (for LWT).
    LocalSerial,
    /// Wait for one replica in the local datacenter.
    LocalOne,
    /// Write to at least one node (including hints).
    Any,
}

impl ConsistencyLevel {
    /// Calculate the number of responses needed to satisfy this CL.
    ///
    /// `rf` is the total replication factor for the keyspace.
    /// For `LocalQuorum`, `LocalOne`, and `LocalSerial`, `rf` should be the local-DC RF.
    ///
    /// `EachQuorum` only has a single scalar result when `rf` represents one
    /// datacenter. Use [`Self::block_for_replicated_dcs`] when per-DC RFs are
    /// available.
    pub fn block_for(&self, rf: usize) -> usize {
        match self {
            Self::One | Self::LocalOne => 1,
            Self::Two => 2.min(rf),
            Self::Three => 3.min(rf),
            Self::Quorum | Self::LocalQuorum | Self::Serial | Self::LocalSerial => rf / 2 + 1,
            Self::All => rf,
            Self::Any => 1, // hints count
            Self::EachQuorum => rf / 2 + 1,
        }
    }

    /// Returns `true` if the given number of responses meets this CL.
    pub fn is_satisfied(&self, received: usize, rf: usize) -> bool {
        received >= self.block_for(rf)
    }

    /// Returns `true` if this is a serial consistency level (LWT).
    pub fn is_serial(&self) -> bool {
        matches!(self, Self::Serial | Self::LocalSerial)
    }

    /// Returns `true` if this CL is datacenter-local.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::LocalOne | Self::LocalQuorum | Self::LocalSerial)
    }

    /// Returns `true` if this CL is usable for writes (not serial-only).
    ///
    /// Serial CLs are only valid as the serial component of a CAS operation,
    /// not as standalone write CLs.
    pub fn requires_write(&self) -> bool {
        !self.is_serial()
    }

    /// Returns `true` if this CL scopes to a single datacenter.
    pub fn is_datacenter_local(&self) -> bool {
        matches!(self, Self::LocalOne | Self::LocalQuorum | Self::LocalSerial)
    }

    /// Compute `block_for` for EACH_QUORUM across multiple DCs.
    ///
    /// Takes a map of datacenter name -> RF for that DC.
    /// Returns the total number of acks required (quorum per DC, summed).
    ///
    /// ## Java Oracle
    ///
    /// `ConsistencyLevel.blockForEachQuorum()`
    pub fn block_for_each_quorum(dc_rf_map: &HashMap<String, usize>) -> usize {
        dc_rf_map.values().map(|rf| rf / 2 + 1).sum()
    }

    /// Compute `block_for` using per-datacenter replication factors.
    ///
    /// This is the topology-aware counterpart to [`Self::block_for`]. For
    /// aggregate CLs it sums the RFs; for local CLs it uses the RF from
    /// `local_dc`; for `EACH_QUORUM` it sums each datacenter quorum.
    pub fn block_for_replicated_dcs(
        &self,
        dc_rf_map: &HashMap<String, usize>,
        local_dc: Option<&str>,
    ) -> usize {
        match self {
            Self::EachQuorum => Self::block_for_each_quorum(dc_rf_map),
            Self::LocalOne | Self::LocalQuorum | Self::LocalSerial => {
                let rf = local_dc
                    .and_then(|dc| dc_rf_map.get(dc))
                    .copied()
                    .unwrap_or(0);
                self.block_for(rf)
            }
            _ => self.block_for(dc_rf_map.values().sum()),
        }
    }

    /// Returns `true` if per-datacenter responses satisfy this CL.
    ///
    /// `received_by_dc` contains successful responses by datacenter and
    /// `dc_rf_map` contains the natural replica count by datacenter.
    pub fn is_satisfied_by_datacenter(
        &self,
        received_by_dc: &HashMap<String, usize>,
        dc_rf_map: &HashMap<String, usize>,
        local_dc: Option<&str>,
    ) -> bool {
        match self {
            Self::EachQuorum => {
                !dc_rf_map.is_empty()
                    && dc_rf_map.iter().all(|(dc, rf)| {
                        let required = rf / 2 + 1;
                        received_by_dc.get(dc).copied().unwrap_or(0) >= required
                    })
            }
            Self::LocalOne | Self::LocalQuorum | Self::LocalSerial => {
                let Some(local_dc) = local_dc else {
                    return false;
                };
                let rf = dc_rf_map.get(local_dc).copied().unwrap_or(0);
                let received = received_by_dc.get(local_dc).copied().unwrap_or(0);
                received >= self.block_for(rf)
            }
            _ => {
                let rf = dc_rf_map.values().sum();
                let received = received_by_dc.values().sum();
                self.is_satisfied(received, rf)
            }
        }
    }

    /// Returns the CQL protocol encoding for this consistency level.
    pub fn protocol_code(&self) -> u16 {
        match self {
            Self::Any => 0x0000,
            Self::One => 0x0001,
            Self::Two => 0x0002,
            Self::Three => 0x0003,
            Self::Quorum => 0x0004,
            Self::All => 0x0005,
            Self::LocalQuorum => 0x0006,
            Self::EachQuorum => 0x0007,
            Self::Serial => 0x0008,
            Self::LocalSerial => 0x0009,
            Self::LocalOne => 0x000A,
        }
    }

    /// Parse from CQL protocol code.
    pub fn from_protocol_code(code: u16) -> Option<Self> {
        match code {
            0x0000 => Some(Self::Any),
            0x0001 => Some(Self::One),
            0x0002 => Some(Self::Two),
            0x0003 => Some(Self::Three),
            0x0004 => Some(Self::Quorum),
            0x0005 => Some(Self::All),
            0x0006 => Some(Self::LocalQuorum),
            0x0007 => Some(Self::EachQuorum),
            0x0008 => Some(Self::Serial),
            0x0009 => Some(Self::LocalSerial),
            0x000A => Some(Self::LocalOne),
            _ => None,
        }
    }
}

impl fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One => write!(f, "ONE"),
            Self::Two => write!(f, "TWO"),
            Self::Three => write!(f, "THREE"),
            Self::Quorum => write!(f, "QUORUM"),
            Self::All => write!(f, "ALL"),
            Self::LocalQuorum => write!(f, "LOCAL_QUORUM"),
            Self::EachQuorum => write!(f, "EACH_QUORUM"),
            Self::Serial => write!(f, "SERIAL"),
            Self::LocalSerial => write!(f, "LOCAL_SERIAL"),
            Self::LocalOne => write!(f, "LOCAL_ONE"),
            Self::Any => write!(f, "ANY"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_for_one() {
        assert_eq!(ConsistencyLevel::One.block_for(3), 1);
    }

    #[test]
    fn block_for_quorum() {
        assert_eq!(ConsistencyLevel::Quorum.block_for(3), 2);
        assert_eq!(ConsistencyLevel::Quorum.block_for(5), 3);
        assert_eq!(ConsistencyLevel::Quorum.block_for(1), 1);
    }

    #[test]
    fn block_for_all() {
        assert_eq!(ConsistencyLevel::All.block_for(3), 3);
        assert_eq!(ConsistencyLevel::All.block_for(1), 1);
    }

    #[test]
    fn block_for_local_quorum() {
        // With local RF=3
        assert_eq!(ConsistencyLevel::LocalQuorum.block_for(3), 2);
    }

    #[test]
    fn block_for_any() {
        assert_eq!(ConsistencyLevel::Any.block_for(3), 1);
    }

    #[test]
    fn is_satisfied() {
        assert!(ConsistencyLevel::Quorum.is_satisfied(2, 3));
        assert!(!ConsistencyLevel::Quorum.is_satisfied(1, 3));
        assert!(ConsistencyLevel::All.is_satisfied(3, 3));
        assert!(!ConsistencyLevel::All.is_satisfied(2, 3));
    }

    #[test]
    fn protocol_code_round_trip() {
        let all = [
            ConsistencyLevel::Any,
            ConsistencyLevel::One,
            ConsistencyLevel::Two,
            ConsistencyLevel::Three,
            ConsistencyLevel::Quorum,
            ConsistencyLevel::All,
            ConsistencyLevel::LocalQuorum,
            ConsistencyLevel::EachQuorum,
            ConsistencyLevel::Serial,
            ConsistencyLevel::LocalSerial,
            ConsistencyLevel::LocalOne,
        ];

        for cl in all {
            let code = cl.protocol_code();
            assert_eq!(ConsistencyLevel::from_protocol_code(code), Some(cl));
        }
    }

    #[test]
    fn is_serial() {
        assert!(ConsistencyLevel::Serial.is_serial());
        assert!(ConsistencyLevel::LocalSerial.is_serial());
        assert!(!ConsistencyLevel::Quorum.is_serial());
    }

    #[test]
    fn is_local() {
        assert!(ConsistencyLevel::LocalOne.is_local());
        assert!(ConsistencyLevel::LocalQuorum.is_local());
        assert!(!ConsistencyLevel::Quorum.is_local());
    }

    #[test]
    fn requires_write() {
        assert!(ConsistencyLevel::One.requires_write());
        assert!(ConsistencyLevel::Quorum.requires_write());
        assert!(ConsistencyLevel::Any.requires_write());
        assert!(!ConsistencyLevel::Serial.requires_write());
        assert!(!ConsistencyLevel::LocalSerial.requires_write());
    }

    #[test]
    fn block_for_each_quorum_multi_dc() {
        let mut dc_map = HashMap::new();
        dc_map.insert("dc1".to_string(), 3);
        dc_map.insert("dc2".to_string(), 3);
        // quorum(3) = 2 per DC, total = 4
        assert_eq!(ConsistencyLevel::block_for_each_quorum(&dc_map), 4);

        let mut single = HashMap::new();
        single.insert("dc1".to_string(), 5);
        // quorum(5) = 3
        assert_eq!(ConsistencyLevel::block_for_each_quorum(&single), 3);
    }

    #[test]
    fn topology_aware_block_for_each_quorum_does_not_use_global_quorum() {
        let mut dc_map = HashMap::new();
        dc_map.insert("dc1".to_string(), 3);
        dc_map.insert("dc2".to_string(), 2);

        assert_eq!(
            ConsistencyLevel::EachQuorum.block_for_replicated_dcs(&dc_map, Some("dc1")),
            4
        );
        assert_eq!(
            ConsistencyLevel::Quorum.block_for_replicated_dcs(&dc_map, None),
            3
        );
        assert_eq!(
            ConsistencyLevel::LocalQuorum.block_for_replicated_dcs(&dc_map, Some("dc2")),
            2
        );
    }

    #[test]
    fn each_quorum_requires_quorum_in_every_datacenter() {
        let mut dc_rf = HashMap::new();
        dc_rf.insert("dc1".to_string(), 3);
        dc_rf.insert("dc2".to_string(), 2);

        let mut all_acks_one_dc = HashMap::new();
        all_acks_one_dc.insert("dc1".to_string(), 3);
        assert!(!ConsistencyLevel::EachQuorum.is_satisfied_by_datacenter(
            &all_acks_one_dc,
            &dc_rf,
            Some("dc1")
        ));

        let mut per_dc_quorums = HashMap::new();
        per_dc_quorums.insert("dc1".to_string(), 2);
        per_dc_quorums.insert("dc2".to_string(), 2);
        assert!(ConsistencyLevel::EachQuorum.is_satisfied_by_datacenter(
            &per_dc_quorums,
            &dc_rf,
            Some("dc1")
        ));
    }

    #[test]
    fn local_quorum_uses_only_local_datacenter_responses() {
        let mut dc_rf = HashMap::new();
        dc_rf.insert("dc1".to_string(), 3);
        dc_rf.insert("dc2".to_string(), 3);

        let mut remote_only = HashMap::new();
        remote_only.insert("dc2".to_string(), 3);
        assert!(!ConsistencyLevel::LocalQuorum.is_satisfied_by_datacenter(
            &remote_only,
            &dc_rf,
            Some("dc1")
        ));

        let mut local_quorum = HashMap::new();
        local_quorum.insert("dc1".to_string(), 2);
        assert!(ConsistencyLevel::LocalQuorum.is_satisfied_by_datacenter(
            &local_quorum,
            &dc_rf,
            Some("dc1")
        ));
    }
}
