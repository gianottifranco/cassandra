// Licensed under Apache License, Version 2.0.

//! Stream plan: describes what data to move between nodes.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamPlan`
//! - `org.apache.cassandra.streaming.StreamOperation`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::session::StreamSession;
use crate::transfer::StreamTransfer;

/// Describes the operation that triggered streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamOperation {
    Bootstrap,
    Decommission,
    Rebuild,
    Repair,
    Replace,
    Relocate,
}

impl std::fmt::Display for StreamOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap => write!(f, "BOOTSTRAP"),
            Self::Decommission => write!(f, "DECOMMISSION"),
            Self::Rebuild => write!(f, "REBUILD"),
            Self::Repair => write!(f, "REPAIR"),
            Self::Replace => write!(f, "REPLACE"),
            Self::Relocate => write!(f, "RELOCATE"),
        }
    }
}

/// A request to stream specific ranges from one endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRequest {
    /// Keyspace to stream.
    pub keyspace: String,
    /// Tables to stream (empty = all tables in keyspace).
    pub tables: Vec<String>,
    /// Token ranges to stream.
    pub ranges: Vec<(Token, Token)>,
    /// Source endpoint to fetch data from.
    pub source: Endpoint,
}

/// A stream plan: builder for constructing multi-session streaming operations.
///
/// Usage:
/// ```
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
///
/// use cassandra_cluster_metadata::Endpoint;
/// use cassandra_common::Token;
/// use cassandra_streaming::{StreamOperation, StreamPlan};
///
/// let source = Endpoint::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000));
/// let target = Endpoint::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001));
/// let ranges = vec![(Token::from_raw(-100), Token::from_raw(100))];
///
/// let sessions = StreamPlan::new(StreamOperation::Bootstrap)
///     .request_ranges(source, "ks", vec!["t1".to_string()], ranges.clone())
///     .transfer_ranges(target, "ks", vec!["t1".to_string()], ranges)
///     .build();
///
/// assert_eq!(sessions.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct StreamPlan {
    /// What operation this plan is for.
    pub operation: StreamOperation,
    /// Ranges to request (fetch) from other nodes, keyed by source endpoint.
    pub requests: HashMap<Endpoint, Vec<StreamRequest>>,
    /// Ranges to transfer (send) to other nodes, keyed by target endpoint.
    pub transfers: HashMap<Endpoint, Vec<StreamRequest>>,
}

impl StreamPlan {
    /// Create a new stream plan for the given operation.
    pub fn new(operation: StreamOperation) -> Self {
        Self {
            operation,
            requests: HashMap::new(),
            transfers: HashMap::new(),
        }
    }

    /// Add a request to fetch data from a source node.
    pub fn request_ranges(
        mut self,
        source: Endpoint,
        keyspace: impl Into<String>,
        tables: Vec<String>,
        ranges: Vec<(Token, Token)>,
    ) -> Self {
        self.requests
            .entry(source)
            .or_default()
            .push(StreamRequest {
                keyspace: keyspace.into(),
                tables,
                ranges,
                source,
            });
        self
    }

    /// Add a transfer to send data to a target node.
    pub fn transfer_ranges(
        mut self,
        target: Endpoint,
        keyspace: impl Into<String>,
        tables: Vec<String>,
        ranges: Vec<(Token, Token)>,
    ) -> Self {
        self.transfers
            .entry(target)
            .or_default()
            .push(StreamRequest {
                keyspace: keyspace.into(),
                tables,
                ranges,
                source: target, // source is self in this case
            });
        self
    }

    /// Build the plan into concrete stream sessions.
    ///
    /// Creates one session per peer endpoint.
    pub fn build(self) -> Vec<StreamSession> {
        let mut sessions = Vec::new();
        let mut peer_sessions: HashMap<Endpoint, StreamSession> = HashMap::new();

        // Create sessions for incoming data (requests)
        for (source, requests) in &self.requests {
            let session = peer_sessions.entry(*source).or_insert_with(|| {
                StreamSession::new(*source, format!("{} from {}", self.operation, source))
            });

            for req in requests {
                let transfer = StreamTransfer::new(
                    req.keyspace.clone(),
                    if req.tables.is_empty() {
                        "*".to_string()
                    } else {
                        req.tables.join(",")
                    },
                    req.ranges.clone(),
                );
                session.add_incoming(transfer);
            }
        }

        // Create sessions for outgoing data (transfers)
        for (target, transfers) in &self.transfers {
            let session = peer_sessions.entry(*target).or_insert_with(|| {
                StreamSession::new(*target, format!("{} to {}", self.operation, target))
            });

            for req in transfers {
                let transfer = StreamTransfer::new(
                    req.keyspace.clone(),
                    if req.tables.is_empty() {
                        "*".to_string()
                    } else {
                        req.tables.join(",")
                    },
                    req.ranges.clone(),
                );
                session.add_outgoing(transfer);
            }
        }

        sessions.extend(peer_sessions.into_values());
        sessions
    }

    /// Number of distinct peers involved in this plan.
    pub fn peer_count(&self) -> usize {
        let mut peers = std::collections::HashSet::<Endpoint>::new();
        peers.extend(self.requests.keys());
        peers.extend(self.transfers.keys());
        peers.len()
    }

    /// Whether the plan has any work to do.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.transfers.is_empty()
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
    fn empty_plan() {
        let plan = StreamPlan::new(StreamOperation::Bootstrap);
        assert!(plan.is_empty());
        assert_eq!(plan.peer_count(), 0);
        assert!(plan.build().is_empty());
    }

    #[test]
    fn single_request() {
        let ranges = vec![(Token::from_raw(-100), Token::from_raw(100))];
        let plan = StreamPlan::new(StreamOperation::Bootstrap).request_ranges(
            ep(7002),
            "ks",
            vec!["t1".into()],
            ranges,
        );

        assert!(!plan.is_empty());
        assert_eq!(plan.peer_count(), 1);

        let sessions = plan.build();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].peer, ep(7002));
        assert_eq!(sessions[0].incoming.len(), 1);
    }

    #[test]
    fn multiple_peers() {
        let r1 = vec![(Token::from_raw(-100), Token::from_raw(0))];
        let r2 = vec![(Token::from_raw(0), Token::from_raw(100))];

        let plan = StreamPlan::new(StreamOperation::Decommission)
            .transfer_ranges(ep(7002), "ks", vec![], r1)
            .transfer_ranges(ep(7003), "ks", vec![], r2);

        assert_eq!(plan.peer_count(), 2);

        let sessions = plan.build();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn bidirectional_session() {
        let ranges = vec![(Token::from_raw(0), Token::from_raw(100))];

        let plan = StreamPlan::new(StreamOperation::Repair)
            .request_ranges(ep(7002), "ks", vec!["t1".into()], ranges.clone())
            .transfer_ranges(ep(7002), "ks", vec!["t1".into()], ranges);

        assert_eq!(plan.peer_count(), 1);

        let sessions = plan.build();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].incoming.len(), 1);
        assert_eq!(sessions[0].outgoing.len(), 1);
    }

    #[test]
    fn operation_display() {
        assert_eq!(StreamOperation::Bootstrap.to_string(), "BOOTSTRAP");
        assert_eq!(StreamOperation::Decommission.to_string(), "DECOMMISSION");
        assert_eq!(StreamOperation::Repair.to_string(), "REPAIR");
    }
}
