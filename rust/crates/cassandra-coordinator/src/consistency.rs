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
    /// For `LocalQuorum` and `LocalOne`, `rf` should be the local-DC RF.
    pub fn block_for(&self, rf: usize) -> usize {
        match self {
            Self::One | Self::LocalOne => 1,
            Self::Two => 2.min(rf),
            Self::Three => 3.min(rf),
            Self::Quorum | Self::LocalQuorum => rf / 2 + 1,
            Self::All => rf,
            Self::Any => 1, // hints count
            Self::EachQuorum => rf / 2 + 1, // per-DC
            Self::Serial | Self::LocalSerial => {
                // Serial CLs are used for LWT; they don't map to block_for
                // in the same way. For now, treat like QUORUM.
                rf / 2 + 1
            }
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
}
