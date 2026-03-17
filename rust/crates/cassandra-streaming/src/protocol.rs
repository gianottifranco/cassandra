// Licensed under Apache License, Version 2.0.

//! # Stream Message Protocol
//!
//! Wire-format types for the 6 streaming verb payloads (verbs 100–105).
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.messages.StreamInitMessage`
//! - `org.apache.cassandra.streaming.messages.IncomingStreamMessage`
//! - `org.apache.cassandra.streaming.messages.OutgoingStreamMessage`
//! - `org.apache.cassandra.streaming.messages.CompleteMessage`

use cassandra_messaging::{Message, Verb};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_storage::memtable::partition::{PartitionData, Row};

use crate::plan::StreamOperation;

// ─────────────────────────────────────────────────────────────────────────────
// Wire-format partition types (avoids BTreeMap<Vec<u8>> JSON key issue)
// ─────────────────────────────────────────────────────────────────────────────

/// Wire-friendly representation of partitions for streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WirePartitions {
    pub entries: Vec<WirePartition>,
}

/// A single partition in wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WirePartition {
    pub key: Vec<u8>,
    pub rows: Vec<(Vec<u8>, Row)>,
    pub tombstone_timestamp: Option<i64>,
    pub tombstone_local_deletion_time: Option<i32>,
}

impl WirePartitions {
    /// Convert from SSTable partition pairs.
    pub fn from_partitions(partitions: &[(Vec<u8>, PartitionData)]) -> Self {
        Self {
            entries: partitions
                .iter()
                .map(|(key, pd)| WirePartition {
                    key: key.clone(),
                    rows: pd.rows.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    tombstone_timestamp: pd.tombstone_timestamp,
                    tombstone_local_deletion_time: pd.tombstone_local_deletion_time,
                })
                .collect(),
        }
    }

    /// Convert back to SSTable partition pairs.
    pub fn to_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        self.entries
            .iter()
            .map(|wp| {
                let mut pd = PartitionData::new();
                for (ck, row) in &wp.rows {
                    pd.rows.insert(ck.clone(), row.clone());
                }
                pd.tombstone_timestamp = wp.tombstone_timestamp;
                pd.tombstone_local_deletion_time = wp.tombstone_local_deletion_time;
                (wp.key.clone(), pd)
            })
            .collect()
    }
}

/// Errors from stream protocol encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum StreamProtocolError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("unknown stream verb id: {0}")]
    UnknownVerb(i32),

    #[error("unexpected verb for stream message: {0:?}")]
    UnexpectedVerb(Verb),
}

// ─────────────────────────────────────────────────────────────────────────────
// Message structs
// ─────────────────────────────────────────────────────────────────────────────

/// Sent by the initiator to start a streaming session (verb 100).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamInitMessage {
    pub session_id: Uuid,
    pub operation: StreamOperation,
    pub description: String,
    pub keyspaces: Vec<String>,
    pub ranges: Vec<(i64, i64)>,
}

/// Response to StreamInit (verb 101).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamInitResponseMessage {
    pub session_id: Uuid,
    pub accepted: bool,
    pub reason: Option<String>,
}

/// A single data chunk sent over the wire (verb 102).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamDataMessage {
    pub session_id: Uuid,
    pub transfer_id: Uuid,
    pub sequence: u64,
    pub data: Vec<u8>,
    pub checksum: [u8; 16],
    pub checksum_algorithm: String,
    pub compressed: bool,
    pub is_last: bool,
}

/// Acknowledgement for a data chunk (verb 103).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamDataResponseMessage {
    pub session_id: Uuid,
    pub transfer_id: Uuid,
    pub sequence: u64,
    pub accepted: bool,
    pub error: Option<String>,
}

/// Session completion notification (verb 104).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamCompleteMessage {
    pub session_id: Uuid,
    pub success: bool,
    pub error: Option<String>,
}

/// Response to session completion (verb 105).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamCompleteResponseMessage {
    pub session_id: Uuid,
    pub acknowledged: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Envelope enum
// ─────────────────────────────────────────────────────────────────────────────

/// All streaming message types in a single enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StreamMessage {
    Init(StreamInitMessage),
    InitResponse(StreamInitResponseMessage),
    Data(StreamDataMessage),
    DataResponse(StreamDataResponseMessage),
    Complete(StreamCompleteMessage),
    CompleteResponse(StreamCompleteResponseMessage),
}

impl StreamMessage {
    /// The verb corresponding to this message variant.
    pub fn verb(&self) -> Verb {
        match self {
            Self::Init(_) => Verb::StreamInit,
            Self::InitResponse(_) => Verb::StreamInitResponse,
            Self::Data(_) => Verb::StreamData,
            Self::DataResponse(_) => Verb::StreamDataResponse,
            Self::Complete(_) => Verb::StreamComplete,
            Self::CompleteResponse(_) => Verb::StreamCompleteResponse,
        }
    }

    /// Serialize into a [`Message`] frame ready for the wire.
    pub fn to_message(&self, msg_id: u64) -> Result<Message, StreamProtocolError> {
        let payload = match self {
            Self::Init(m) => serde_json::to_vec(m)?,
            Self::InitResponse(m) => serde_json::to_vec(m)?,
            Self::Data(m) => serde_json::to_vec(m)?,
            Self::DataResponse(m) => serde_json::to_vec(m)?,
            Self::Complete(m) => serde_json::to_vec(m)?,
            Self::CompleteResponse(m) => serde_json::to_vec(m)?,
        };
        let verb = self.verb();
        if !verb.is_request() {
            Ok(Message::response(msg_id, verb, payload))
        } else {
            Ok(Message::request(verb, msg_id, payload))
        }
    }

    /// Deserialize from a [`Message`] frame.
    pub fn from_message(msg: &Message) -> Result<Self, StreamProtocolError> {
        let verb = msg.header.verb;
        match verb {
            Verb::StreamInit => Ok(Self::Init(serde_json::from_slice(&msg.payload)?)),
            Verb::StreamInitResponse => {
                Ok(Self::InitResponse(serde_json::from_slice(&msg.payload)?))
            }
            Verb::StreamData => Ok(Self::Data(serde_json::from_slice(&msg.payload)?)),
            Verb::StreamDataResponse => {
                Ok(Self::DataResponse(serde_json::from_slice(&msg.payload)?))
            }
            Verb::StreamComplete => {
                Ok(Self::Complete(serde_json::from_slice(&msg.payload)?))
            }
            Verb::StreamCompleteResponse => {
                Ok(Self::CompleteResponse(serde_json::from_slice(&msg.payload)?))
            }
            _ => Err(StreamProtocolError::UnexpectedVerb(verb)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_init() {
        let init = StreamMessage::Init(StreamInitMessage {
            session_id: Uuid::new_v4(),
            operation: StreamOperation::Bootstrap,
            description: "bootstrap stream".into(),
            keyspaces: vec!["ks1".into()],
            ranges: vec![(0, 100)],
        });
        let msg = init.to_message(1).unwrap();
        assert_eq!(msg.header.verb, Verb::StreamInit);
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(init, decoded);
    }

    #[test]
    fn round_trip_init_response() {
        let resp = StreamMessage::InitResponse(StreamInitResponseMessage {
            session_id: Uuid::new_v4(),
            accepted: true,
            reason: None,
        });
        let msg = resp.to_message(2).unwrap();
        assert_eq!(msg.header.verb, Verb::StreamInitResponse);
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn round_trip_data() {
        let data = StreamMessage::Data(StreamDataMessage {
            session_id: Uuid::new_v4(),
            transfer_id: Uuid::new_v4(),
            sequence: 42,
            data: vec![1, 2, 3, 4],
            checksum: [0u8; 16],
            checksum_algorithm: "Crc32".into(),
            compressed: false,
            is_last: true,
        });
        let msg = data.to_message(3).unwrap();
        assert_eq!(msg.header.verb, Verb::StreamData);
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn round_trip_data_response() {
        let resp = StreamMessage::DataResponse(StreamDataResponseMessage {
            session_id: Uuid::new_v4(),
            transfer_id: Uuid::new_v4(),
            sequence: 0,
            accepted: true,
            error: None,
        });
        let msg = resp.to_message(4).unwrap();
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn round_trip_complete() {
        let complete = StreamMessage::Complete(StreamCompleteMessage {
            session_id: Uuid::new_v4(),
            success: true,
            error: None,
        });
        let msg = complete.to_message(5).unwrap();
        assert_eq!(msg.header.verb, Verb::StreamComplete);
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(complete, decoded);
    }

    #[test]
    fn round_trip_complete_response() {
        let resp = StreamMessage::CompleteResponse(StreamCompleteResponseMessage {
            session_id: Uuid::new_v4(),
            acknowledged: true,
        });
        let msg = resp.to_message(6).unwrap();
        let decoded = StreamMessage::from_message(&msg).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn unknown_verb_error() {
        let msg = Message::request(Verb::Ping, 99, b"{}".to_vec());
        let err = StreamMessage::from_message(&msg).unwrap_err();
        assert!(matches!(err, StreamProtocolError::UnexpectedVerb(Verb::Ping)));
    }

    #[test]
    fn verb_mapping() {
        let init = StreamMessage::Init(StreamInitMessage {
            session_id: Uuid::nil(),
            operation: StreamOperation::Repair,
            description: String::new(),
            keyspaces: vec![],
            ranges: vec![],
        });
        assert_eq!(init.verb(), Verb::StreamInit);

        let complete = StreamMessage::Complete(StreamCompleteMessage {
            session_id: Uuid::nil(),
            success: false,
            error: Some("fail".into()),
        });
        assert_eq!(complete.verb(), Verb::StreamComplete);
    }
}
