//! Consensus routing module.
//!
//! Directs operations to either Paxos or Accord based on table metadata.

pub mod router;

pub use router::ConsensusRouter;
