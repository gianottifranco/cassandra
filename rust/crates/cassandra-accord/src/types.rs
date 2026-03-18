// Licensed under Apache License, Version 2.0.

//! Core types for the Accord distributed transaction protocol.
//!
//! ## Java Oracle
//! - `accord.primitives.TxnId`
//! - `accord.primitives.Timestamp`
//! - `accord.primitives.Keys`
//! - `accord.txn.Txn`

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid;

/// A globally unique transaction identifier.
///
/// Combines a logical timestamp with a node ID for total ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId {
    /// Logical timestamp (micros since epoch).
    pub timestamp: i64,
    /// Originating node.
    pub node_id: Uuid,
    /// Sequence number for tie-breaking within the same timestamp+node.
    pub sequence: u32,
}

impl TxnId {
    pub fn new(node_id: Uuid) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros() as i64;
        Self {
            timestamp,
            node_id,
            sequence: 0,
        }
    }

    pub fn with_timestamp(timestamp: i64, node_id: Uuid, sequence: u32) -> Self {
        Self {
            timestamp,
            node_id,
            sequence,
        }
    }
}

impl Ord for TxnId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.node_id.cmp(&other.node_id))
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for TxnId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TxnId(ts={}, node={}, seq={})",
            self.timestamp, self.node_id, self.sequence
        )
    }
}

/// Accord logical timestamp for ordering operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros() as i64;
        Self(micros)
    }

    pub fn none() -> Self {
        Self(0)
    }

    pub fn is_none(&self) -> bool {
        self.0 == 0
    }
}

/// A set of partition keys involved in a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Keys(pub Vec<Vec<u8>>);

impl Keys {
    pub fn single(key: Vec<u8>) -> Self {
        Self(vec![key])
    }

    pub fn multiple(keys: Vec<Vec<u8>>) -> Self {
        Self(keys)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// An Accord transaction — the read/write set and mutation to apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Txn {
    /// The keys this transaction reads/writes.
    pub keys: Keys,
    /// Serialized mutation data.
    pub mutation: Vec<u8>,
    /// The keyspace this transaction targets.
    pub keyspace: String,
}

/// Status of a command through the Accord protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandStatus {
    /// Initial state after PreAccept.
    PreAccepted,
    /// Accepted after conflict resolution.
    Accepted,
    /// Committed — durably decided.
    Committed,
    /// Applied — mutation executed against storage.
    Applied,
    /// Invalidated — transaction was superseded.
    Invalidated,
}

impl CommandStatus {
    pub fn is_decided(&self) -> bool {
        matches!(self, CommandStatus::Committed | CommandStatus::Applied)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, CommandStatus::Applied | CommandStatus::Invalidated)
    }
}

impl fmt::Display for CommandStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandStatus::PreAccepted => write!(f, "PreAccepted"),
            CommandStatus::Accepted => write!(f, "Accepted"),
            CommandStatus::Committed => write!(f, "Committed"),
            CommandStatus::Applied => write!(f, "Applied"),
            CommandStatus::Invalidated => write!(f, "Invalidated"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_ordering() {
        let n = Uuid::nil();
        let t1 = TxnId::with_timestamp(100, n, 0);
        let t2 = TxnId::with_timestamp(200, n, 0);
        assert!(t1 < t2);
    }

    #[test]
    fn txn_id_sequence_tiebreak() {
        let n = Uuid::nil();
        let t1 = TxnId::with_timestamp(100, n, 0);
        let t2 = TxnId::with_timestamp(100, n, 1);
        assert!(t1 < t2);
    }

    #[test]
    fn command_status_transitions() {
        assert!(!CommandStatus::PreAccepted.is_decided());
        assert!(!CommandStatus::Accepted.is_decided());
        assert!(CommandStatus::Committed.is_decided());
        assert!(CommandStatus::Applied.is_decided());
        assert!(CommandStatus::Applied.is_terminal());
        assert!(CommandStatus::Invalidated.is_terminal());
    }

    #[test]
    fn keys_single_and_multiple() {
        let k = Keys::single(b"key1".to_vec());
        assert_eq!(k.len(), 1);
        let k2 = Keys::multiple(vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(k2.len(), 2);
    }

    #[test]
    fn timestamp_ordering() {
        let t1 = Timestamp(100);
        let t2 = Timestamp(200);
        assert!(t1 < t2);
        assert!(Timestamp::none().is_none());
    }
}
