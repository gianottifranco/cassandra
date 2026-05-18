// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side mutation verb handler.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.MutationVerbHandler`
//!
//! Receives `Verb::Mutation`, deserializes `CoordinatedMutation` from
//! the payload, applies locally, and returns `Verb::MutationResponse`.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use cassandra_storage::commitlog::{
    CellMutation as StorageCellMutation, Mutation as StorageMutation,
    MutationRow as StorageMutationRow, RangeTombstoneMarker, TombstoneMarker,
};
use cassandra_storage::engine::StorageEngine;
use tracing::{debug, warn};

use crate::write::{CoordinatedMutation, RangeTombstone};

// ─── Mutation Payload ───────────────────────────────────────────

/// Serialized mutation request sent between coordinator and replicas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationRequest {
    /// The mutation to apply.
    pub mutation: CoordinatedMutation,
}

/// Response from a successful mutation apply.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationResponse {
    /// Whether the mutation was applied successfully.
    pub success: bool,
}

// ─── Handler ────────────────────────────────────────────────────

/// Replica-side handler for `Verb::Mutation` messages.
///
/// Deserializes the mutation, applies it locally, and returns a `Verb::MutationResponse`.
pub struct MutationVerbHandler;

impl MutationVerbHandler {
    /// Handle an incoming mutation message when no local storage engine is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local storage engine not configured for Mutation".to_vec(),
        ))
    }

    /// Handle an incoming mutation message using the local storage engine.
    pub fn handle_with_storage(msg: Message, storage: &StorageEngine) -> Option<Message> {
        let request: MutationRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize mutation request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            keyspace = %request.mutation.keyspace,
            table = %request.mutation.table,
            "Applying mutation locally"
        );

        let storage_mutation = to_storage_mutation(request.mutation);
        if let Err(e) = storage.apply_mutation(&storage_mutation) {
            return Some(Message::failure(
                msg.header.message_id,
                format!("Storage apply error: {e}").into_bytes(),
            ));
        }

        let response = MutationResponse { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::MutationResponse,
            payload,
        ))
    }
}

pub(crate) fn to_storage_mutation(mutation: CoordinatedMutation) -> StorageMutation {
    let range_tombstones = mutation
        .rows
        .iter()
        .filter_map(|row| row.range_tombstone.clone().map(to_storage_range_tombstone))
        .collect();

    StorageMutation {
        keyspace: mutation.keyspace,
        table: mutation.table,
        partition_key: mutation.partition_key,
        rows: mutation
            .rows
            .into_iter()
            .map(|row| {
                let range_tombstone = row.range_tombstone;
                StorageMutationRow {
                    clustering_key: row.clustering_key,
                    cells: row.cells.into_iter().map(to_storage_cell).collect(),
                    is_tombstone: row.is_tombstone || range_tombstone.is_some(),
                    local_deletion_time: range_tombstone
                        .as_ref()
                        .map(|marker| marker.local_deletion_time),
                }
            })
            .collect(),
        timestamp: mutation.timestamp,
        cdc_enabled: false,
        static_cells: mutation
            .static_cells
            .into_iter()
            .map(to_storage_cell)
            .collect(),
        partition_tombstone: mutation.partition_tombstone.map(|marker| TombstoneMarker {
            timestamp: marker.deletion_time,
            local_deletion_time: marker.local_deletion_time,
        }),
        range_tombstones,
    }
}

fn to_storage_cell(cell: crate::write::CellMutation) -> StorageCellMutation {
    StorageCellMutation {
        column: cell.column,
        value: cell.value,
        timestamp: cell.timestamp,
        ttl: cell.ttl,
        local_deletion_time: None,
        is_tombstone: cell.is_tombstone,
    }
}

fn to_storage_range_tombstone(marker: RangeTombstone) -> RangeTombstoneMarker {
    RangeTombstoneMarker {
        start: marker.start,
        end: marker.end,
        timestamp: marker.deletion_time,
        local_deletion_time: marker.local_deletion_time,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{CellMutation, CoordinatedMutation, MutationRow};
    use cassandra_storage::commitlog::CommitLogConfig;
    use cassandra_storage::engine::EngineConfig;
    use tempfile::TempDir;

    fn test_storage() -> (StorageEngine, TempDir) {
        let temp = TempDir::new().unwrap();
        let storage = StorageEngine::open(EngineConfig {
            data_directories: vec![temp.path().join("data")],
            commitlog: CommitLogConfig {
                directory: temp.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        (storage, temp)
    }

    fn make_mutation() -> CoordinatedMutation {
        CoordinatedMutation::simple(
            "test_ks".to_string(),
            "test_table".to_string(),
            vec![1, 2, 3],
            vec![MutationRow {
                clustering_key: vec![10],
                cells: vec![CellMutation {
                    column: "col1".to_string(),
                    value: Some(b"value1".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                }],
                is_tombstone: false,
                range_tombstone: None,
            }],
            1000,
        )
    }

    #[test]
    fn mutation_request_roundtrip() {
        let request = MutationRequest {
            mutation: make_mutation(),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: MutationRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.mutation.keyspace, "test_ks");
        assert_eq!(decoded.mutation.table, "test_table");
    }

    #[test]
    fn handle_valid_mutation_without_storage_fails() {
        let request = MutationRequest {
            mutation: make_mutation(),
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::Mutation, 42, payload);

        let response = MutationVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_valid_mutation_applies_to_storage() {
        let (storage, _temp) = test_storage();
        let request = MutationRequest {
            mutation: make_mutation(),
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::Mutation, 42, payload);

        let response = MutationVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::MutationResponse);
        assert!(resp.is_response());

        let resp_body: MutationResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(resp_body.success);

        let partition = storage
            .read_partition("test_ks", "test_table", &[1, 2, 3])
            .unwrap();
        let row = partition.rows.get(&vec![10]).unwrap();
        assert_eq!(row.cells[0].column, "col1");
        assert_eq!(row.cells[0].value.as_deref(), Some(b"value1".as_slice()));
    }

    #[test]
    fn handle_invalid_payload() {
        let msg = Message::request(Verb::Mutation, 42, b"not json".to_vec());
        let response = MutationVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }
}
