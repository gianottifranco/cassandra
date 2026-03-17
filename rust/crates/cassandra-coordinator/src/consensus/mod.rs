//! Consensus routing module.
//!
//! Directs operations to either Paxos or Accord based on table metadata.
//! Supports per-key migration for Mixed mode (Paxos→Accord transition).

pub mod router;

pub use router::{ConsensusMetrics, ConsensusRouter};
