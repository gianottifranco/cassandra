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

//! Read repair logic triggered after digest mismatches.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.reads.repair.BlockingReadRepair`
//! - `org.apache.cassandra.service.reads.repair.ReadOnlyReadRepair`
//! - `org.apache.cassandra.service.reads.repair.ReadRepair`
//! - `org.apache.cassandra.service.reads.repair.ReadRepairStrategy`

use cassandra_cluster_metadata::Endpoint;
use cassandra_messaging::{MessagingService, frame::Message, verb::Verb};
use cassandra_storage::memtable::partition::PartitionData;
use std::sync::Arc;

use tracing::{debug, info};

/// Strategy for read repair.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.reads.repair.ReadRepairStrategy`
/// controlled by `read_repair` table property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRepairStrategy {
    /// Blocking: repair before returning to client (default in Java 4.x+).
    Blocking,
    /// None: no read repair (configurable per table).
    None,
}

impl ReadRepairStrategy {
    /// Parse from CQL table property value.
    pub fn from_str_cql(s: &str) -> Self {
        match s.trim().to_uppercase().as_str() {
            "NONE" => Self::None,
            _ => Self::Blocking,
        }
    }
}

impl Default for ReadRepairStrategy {
    fn default() -> Self {
        Self::Blocking
    }
}

/// A pending read repair mutation to send to a stale replica.
#[derive(Debug, Clone)]
pub struct ReadRepairMutation {
    /// Target replica.
    pub target: Endpoint,
    /// Keyspace.
    pub keyspace: String,
    /// Table.
    pub table: String,
    /// Partition key.
    pub partition_key: Vec<u8>,
    /// The reconciled data to write.
    pub data: PartitionData,
}

/// Read repair handler that collects mutations and dispatches them.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.reads.repair.BlockingReadRepair`
#[derive(Debug)]
pub struct ReadRepairHandler {
    /// Strategy in use.
    pub strategy: ReadRepairStrategy,
    /// Pending mutations.
    pub pending: Vec<ReadRepairMutation>,
    /// dclocal_read_repair_chance (deprecated in C* 4.0 but still in schema).
    pub dc_local_read_repair_chance: f64,
    /// read_repair_chance (deprecated in C* 4.0 but still in schema).
    pub read_repair_chance: f64,
}

impl ReadRepairHandler {
    pub fn new(strategy: ReadRepairStrategy) -> Self {
        Self {
            strategy,
            pending: Vec::new(),
            dc_local_read_repair_chance: 0.0,
            read_repair_chance: 0.0,
        }
    }

    /// Whether read repair is enabled.
    pub fn is_enabled(&self) -> bool {
        self.strategy != ReadRepairStrategy::None
    }

    /// Stage a repair mutation for a stale replica.
    pub fn stage_repair(
        &mut self,
        target: Endpoint,
        keyspace: String,
        table: String,
        partition_key: Vec<u8>,
        data: PartitionData,
    ) {
        if !self.is_enabled() {
            return;
        }
        debug!(
            target = %target,
            keyspace = %keyspace,
            table = %table,
            "Staging read repair mutation"
        );
        self.pending.push(ReadRepairMutation {
            target,
            keyspace,
            table,
            partition_key,
            data,
        });
    }

    /// Execute all pending repairs.
    ///
    /// In a real implementation, this would send mutations via MessagingService.
    /// For now, we log and track the mutations.
    ///
    /// Returns the number of repair mutations dispatched.
    pub async fn execute_repairs(&mut self, messaging: Option<Arc<MessagingService>>) -> usize {
        let count = self.pending.len();
        if count > 0 {
            info!(
                count,
                strategy = ?self.strategy,
                "Executing read repair mutations"
            );
            if let Some(msg_svc) = messaging {
                // TODO: For blocking strategy, we should use send_and_wait and collect responses
                // For now, fire-and-forget or just log
                for repair in self.pending.drain(..) {
                    let payload = b"simulated_repair_payload".to_vec(); // Simplified for parity test stub
                    let msg = Message::request(Verb::ReadRepair, msg_svc.next_id(), payload);
                    let _ = msg_svc.send(repair.target.addr(), msg).await;
                }
            } else {
                self.pending.clear();
            }
        }
        count
    }

    /// Number of pending repairs.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn read_repair_strategy_parse() {
        assert_eq!(
            ReadRepairStrategy::from_str_cql("BLOCKING"),
            ReadRepairStrategy::Blocking
        );
        assert_eq!(
            ReadRepairStrategy::from_str_cql("NONE"),
            ReadRepairStrategy::None
        );
        assert_eq!(
            ReadRepairStrategy::from_str_cql("blocking"),
            ReadRepairStrategy::Blocking
        );
    }

    #[test]
    fn blocking_read_repair_stages() {
        let mut handler = ReadRepairHandler::new(ReadRepairStrategy::Blocking);
        let pd = PartitionData::new();

        handler.stage_repair(ep(7002), "ks".into(), "t1".into(), b"pk".to_vec(), pd);
        assert_eq!(handler.pending_count(), 1);
    }

    #[test]
    fn none_strategy_skips_repair() {
        let mut handler = ReadRepairHandler::new(ReadRepairStrategy::None);
        let pd = PartitionData::new();

        handler.stage_repair(ep(7002), "ks".into(), "t1".into(), b"pk".to_vec(), pd);
        assert_eq!(handler.pending_count(), 0);
    }

    #[tokio::test]
    async fn execute_repairs_clears_pending() {
        let mut handler = ReadRepairHandler::new(ReadRepairStrategy::Blocking);
        let pd = PartitionData::new();

        handler.stage_repair(
            ep(7002),
            "ks".into(),
            "t1".into(),
            b"pk".to_vec(),
            pd.clone(),
        );
        handler.stage_repair(ep(7003), "ks".into(), "t1".into(), b"pk".to_vec(), pd);

        let count = handler.execute_repairs(None).await;
        assert_eq!(count, 2);
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn is_enabled() {
        assert!(ReadRepairHandler::new(ReadRepairStrategy::Blocking).is_enabled());
        assert!(!ReadRepairHandler::new(ReadRepairStrategy::None).is_enabled());
    }
}
