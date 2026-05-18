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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadRepairStrategy {
    /// Blocking: repair before returning to client (default in Java 4.x+).
    #[default]
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
    /// Sends mutations via MessagingService with proper serialized payloads.
    /// For blocking strategy, waits for acknowledgment from each replica.
    ///
    /// Returns the number of repair mutations dispatched.
    ///
    /// ## Java Oracle
    ///
    /// `BlockingReadRepair.repairPartition()` → serializes as Mutation
    pub async fn execute_repairs(&mut self, messaging: Option<Arc<MessagingService>>) -> usize {
        let count = self.pending.len();
        if count > 0 {
            info!(
                count,
                strategy = ?self.strategy,
                "Executing read repair mutations"
            );
            if let Some(msg_svc) = messaging {
                for repair in self.pending.drain(..) {
                    let payload = serialize_repair_payload(&repair);
                    let msg = Message::request(Verb::ReadRepair, msg_svc.next_id(), payload);
                    match self.strategy {
                        ReadRepairStrategy::Blocking => {
                            // Block until repair is acknowledged.
                            match msg_svc
                                .send_and_wait(
                                    repair.target.addr(),
                                    msg,
                                    std::time::Duration::from_secs(2),
                                )
                                .await
                            {
                                Ok(_) => {
                                    debug!(
                                        target_ep = %repair.target,
                                        "Read repair acknowledged"
                                    )
                                }
                                Err(e) => {
                                    debug!(
                                        target_ep = %repair.target,
                                        error = %e,
                                        "Read repair failed"
                                    )
                                }
                            }
                        }
                        ReadRepairStrategy::None => unreachable!("checked by is_enabled"),
                    }
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

/// Serialize a read repair mutation into bytes for the wire protocol.
///
/// Layout (all integers big-endian):
/// - keyspace: u32 len + bytes
/// - table: u32 len + bytes
/// - partition_key: u32 len + bytes
/// - row_count: u32
/// - for each row:
///   - clustering_key: u32 len + bytes
///   - cell_count: u32
///   - for each cell:
///     - column: u32 len + bytes
///     - value: u32 len + bytes (0 len if None)
///     - timestamp: i64
///
/// ## Java Oracle
///
/// `ReadRepair.repairPartition()` -> serializes as Mutation
fn serialize_repair_payload(repair: &ReadRepairMutation) -> Vec<u8> {
    // Pre-compute capacity: keyspace + table + pk + row overhead
    let estimated = 4
        + repair.keyspace.len()
        + 4
        + repair.table.len()
        + 4
        + repair.partition_key.len()
        + 4
        + repair.data.rows.len() * 64; // rough estimate per row
    let mut payload = Vec::with_capacity(estimated);
    // Write keyspace
    let ks_bytes = repair.keyspace.as_bytes();
    payload.extend_from_slice(&(ks_bytes.len() as u32).to_be_bytes());
    payload.extend_from_slice(ks_bytes);
    // Write table
    let tbl_bytes = repair.table.as_bytes();
    payload.extend_from_slice(&(tbl_bytes.len() as u32).to_be_bytes());
    payload.extend_from_slice(tbl_bytes);
    // Write partition key
    payload.extend_from_slice(&(repair.partition_key.len() as u32).to_be_bytes());
    payload.extend_from_slice(&repair.partition_key);
    // Write row count
    let row_count = repair.data.rows.len() as u32;
    payload.extend_from_slice(&row_count.to_be_bytes());
    // For each row, write clustering key and cell data
    for (ck, row) in &repair.data.rows {
        payload.extend_from_slice(&(ck.len() as u32).to_be_bytes());
        payload.extend_from_slice(ck);
        let cell_count = row.cells.len() as u32;
        payload.extend_from_slice(&cell_count.to_be_bytes());
        for cell in &row.cells {
            let col_bytes = cell.column.as_bytes();
            payload.extend_from_slice(&(col_bytes.len() as u32).to_be_bytes());
            payload.extend_from_slice(col_bytes);
            if let Some(ref val) = cell.value {
                payload.extend_from_slice(&(val.len() as u32).to_be_bytes());
                payload.extend_from_slice(val);
            } else {
                payload.extend_from_slice(&0u32.to_be_bytes());
            }
            payload.extend_from_slice(&cell.timestamp.to_be_bytes());
        }
    }
    payload
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

    // ── WU-15: Serialization tests ──────────────────────────────────

    use cassandra_storage::memtable::partition::{Cell, Row};

    fn make_repair_mutation() -> ReadRepairMutation {
        let mut pd = PartitionData::new();
        pd.rows.insert(
            b"ck1".to_vec(),
            Row {
                clustering_key: b"ck1".to_vec(),
                cells: vec![Cell {
                    column: "col1".to_string(),
                    value: Some(b"val1".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            },
        );
        ReadRepairMutation {
            target: ep(7002),
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk".to_vec(),
            data: pd,
        }
    }

    #[test]
    fn serialize_repair_payload_roundtrip_structure() {
        let repair = make_repair_mutation();
        let payload = serialize_repair_payload(&repair);

        let mut pos = 0;

        // Read keyspace
        let ks_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let ks = std::str::from_utf8(&payload[pos..pos + ks_len]).unwrap();
        assert_eq!(ks, "ks");
        pos += ks_len;

        // Read table
        let tbl_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let tbl = std::str::from_utf8(&payload[pos..pos + tbl_len]).unwrap();
        assert_eq!(tbl, "t1");
        pos += tbl_len;

        // Read partition key
        let pk_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        assert_eq!(&payload[pos..pos + pk_len], b"pk");
        pos += pk_len;

        // Read row count
        let row_count = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;
        assert_eq!(row_count, 1);

        // Read clustering key
        let ck_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        assert_eq!(&payload[pos..pos + ck_len], b"ck1");
        pos += ck_len;

        // Read cell count
        let cell_count = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;
        assert_eq!(cell_count, 1);

        // Read column name
        let col_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let col = std::str::from_utf8(&payload[pos..pos + col_len]).unwrap();
        assert_eq!(col, "col1");
        pos += col_len;

        // Read value
        let val_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        assert_eq!(&payload[pos..pos + val_len], b"val1");
        pos += val_len;

        // Read timestamp
        let ts = i64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        assert_eq!(ts, 1000);

        // Should have consumed all bytes
        assert_eq!(pos, payload.len());
    }

    #[test]
    fn serialize_repair_payload_empty_partition() {
        let repair = ReadRepairMutation {
            target: ep(7002),
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk".to_vec(),
            data: PartitionData::new(),
        };
        let payload = serialize_repair_payload(&repair);

        let mut pos = 0;
        // Skip keyspace
        let ks_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + ks_len;
        // Skip table
        let tbl_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + tbl_len;
        // Skip partition key
        let pk_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + pk_len;
        // Row count should be 0
        let row_count = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        assert_eq!(row_count, 0);
    }

    #[test]
    fn serialize_repair_payload_null_cell_value() {
        let mut pd = PartitionData::new();
        pd.rows.insert(
            b"ck".to_vec(),
            Row {
                clustering_key: b"ck".to_vec(),
                cells: vec![Cell {
                    column: "c".to_string(),
                    value: None,
                    timestamp: 500,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: true,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            },
        );
        let repair = ReadRepairMutation {
            target: ep(7002),
            keyspace: "k".to_string(),
            table: "t".to_string(),
            partition_key: b"p".to_vec(),
            data: pd,
        };
        let payload = serialize_repair_payload(&repair);

        // Parse to the value length for the cell -- should be 0 for None
        let mut pos = 0;
        // Skip keyspace
        let ks_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + ks_len;
        // Skip table
        let tbl_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + tbl_len;
        // Skip partition key
        let pk_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + pk_len;
        // Skip row count
        pos += 4;
        // Skip clustering key
        let ck_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + ck_len;
        // Skip cell count
        pos += 4;
        // Skip column name
        let col_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + col_len;
        // Value length should be 0
        let val_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        assert_eq!(val_len, 0);
    }
}
