// Licensed under Apache License, Version 2.0.

//! StorageService: node lifecycle state machine.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageService`
//!
//! ## Architecture
//!
//! Manages the node's lifecycle through well-defined states:
//! Starting → Joining → Normal → Leaving → Left.
//!
//! Each transition validates that the move is legal, and the state
//! can be queried at any time to gate operations (e.g., reject writes
//! while the node is in `Leaving` state).

use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, warn};

// ─── Node State ─────────────────────────────────────────────────

/// Lifecycle state of the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Initial state: components are being initialized.
    Starting,
    /// Node is joining the ring (streaming data, building ranges).
    Joining,
    /// Normal operation: serving reads and writes.
    Normal,
    /// Node is leaving the ring (streaming data away).
    Leaving,
    /// Node has left the ring and is shutting down.
    Left,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => write!(f, "STARTING"),
            Self::Joining => write!(f, "JOINING"),
            Self::Normal => write!(f, "NORMAL"),
            Self::Leaving => write!(f, "LEAVING"),
            Self::Left => write!(f, "LEFT"),
        }
    }
}

// ─── Errors ─────────────────────────────────────────────────────

/// Errors from StorageService state transitions.
#[derive(Debug, thiserror::Error)]
pub enum StorageServiceError {
    #[error("Invalid state transition: {from} → {to}")]
    InvalidTransition { from: String, to: String },

    #[error("Operation not allowed in state: {state}")]
    OperationNotAllowed { state: String },

    #[error("Initialization error: {0}")]
    InitializationError(String),
}

// ─── StorageService ─────────────────────────────────────────────

/// Node lifecycle state machine.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.StorageService`
pub struct StorageService {
    /// Current node state.
    state: Arc<RwLock<NodeState>>,
    /// Whether the native transport (CQL) is running.
    native_transport_running: Arc<RwLock<bool>>,
}

impl StorageService {
    /// Create a new StorageService in the Starting state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(NodeState::Starting)),
            native_transport_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Get the current node state.
    pub fn state(&self) -> NodeState {
        *self.state.read()
    }

    /// Whether the native transport is running.
    pub fn is_native_transport_running(&self) -> bool {
        *self.native_transport_running.read()
    }

    /// Initialize the storage service (Starting → Joining).
    ///
    /// Performs initialization tasks: loads schema, opens storage engine,
    /// prepares for ring joining.
    pub fn initialize(&self) -> Result<(), StorageServiceError> {
        let mut state = self.state.write();
        if *state != NodeState::Starting {
            return Err(StorageServiceError::InvalidTransition {
                from: state.to_string(),
                to: NodeState::Joining.to_string(),
            });
        }

        info!("StorageService initializing");
        *state = NodeState::Joining;
        info!(state = %NodeState::Joining, "StorageService state transition");
        Ok(())
    }

    /// Start the native transport (CQL listener).
    ///
    /// Only allowed when the node is in Normal state.
    pub fn start_native_transport(&self) -> Result<(), StorageServiceError> {
        let state = self.state.read();
        if *state != NodeState::Normal {
            return Err(StorageServiceError::OperationNotAllowed {
                state: state.to_string(),
            });
        }
        drop(state);

        let mut running = self.native_transport_running.write();
        *running = true;
        info!("Native transport started");
        Ok(())
    }

    /// Transition to Normal state (Joining → Normal).
    ///
    /// Called after the node has finished joining the ring.
    pub fn set_normal(&self) -> Result<(), StorageServiceError> {
        let mut state = self.state.write();
        if *state != NodeState::Joining {
            return Err(StorageServiceError::InvalidTransition {
                from: state.to_string(),
                to: NodeState::Normal.to_string(),
            });
        }

        *state = NodeState::Normal;
        info!(state = %NodeState::Normal, "StorageService state transition");
        Ok(())
    }

    /// Begin leaving the ring (Normal → Leaving).
    ///
    /// Starts streaming data to other nodes.
    pub fn start_leaving(&self) -> Result<(), StorageServiceError> {
        let mut state = self.state.write();
        if *state != NodeState::Normal {
            return Err(StorageServiceError::InvalidTransition {
                from: state.to_string(),
                to: NodeState::Leaving.to_string(),
            });
        }

        // Stop native transport first.
        {
            let mut running = self.native_transport_running.write();
            if *running {
                *running = false;
                info!("Native transport stopped for decommission");
            }
        }

        *state = NodeState::Leaving;
        info!(state = %NodeState::Leaving, "StorageService state transition");
        Ok(())
    }

    /// Finish leaving the ring (Leaving → Left).
    ///
    /// Called after all data has been streamed away.
    pub fn finish_leaving(&self) -> Result<(), StorageServiceError> {
        let mut state = self.state.write();
        if *state != NodeState::Leaving {
            return Err(StorageServiceError::InvalidTransition {
                from: state.to_string(),
                to: NodeState::Left.to_string(),
            });
        }

        *state = NodeState::Left;
        info!(state = %NodeState::Left, "StorageService state transition");
        Ok(())
    }

    /// Whether the node is in a state that accepts client requests.
    pub fn is_serving(&self) -> bool {
        *self.state.read() == NodeState::Normal
    }

    /// Whether the node is joining the ring.
    pub fn is_joining(&self) -> bool {
        *self.state.read() == NodeState::Joining
    }

    /// Whether the node is leaving the ring.
    pub fn is_leaving(&self) -> bool {
        *self.state.read() == NodeState::Leaving
    }
}

impl Default for StorageService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state() {
        let svc = StorageService::new();
        assert_eq!(svc.state(), NodeState::Starting);
        assert!(!svc.is_serving());
        assert!(!svc.is_native_transport_running());
    }

    #[test]
    fn happy_path_lifecycle() {
        let svc = StorageService::new();

        // Starting → Joining
        svc.initialize().unwrap();
        assert_eq!(svc.state(), NodeState::Joining);
        assert!(svc.is_joining());

        // Joining → Normal
        svc.set_normal().unwrap();
        assert_eq!(svc.state(), NodeState::Normal);
        assert!(svc.is_serving());

        // Start native transport
        svc.start_native_transport().unwrap();
        assert!(svc.is_native_transport_running());

        // Normal → Leaving
        svc.start_leaving().unwrap();
        assert_eq!(svc.state(), NodeState::Leaving);
        assert!(svc.is_leaving());
        assert!(!svc.is_native_transport_running());

        // Leaving → Left
        svc.finish_leaving().unwrap();
        assert_eq!(svc.state(), NodeState::Left);
        assert!(!svc.is_serving());
    }

    #[test]
    fn invalid_initialize_from_normal() {
        let svc = StorageService::new();
        svc.initialize().unwrap();
        svc.set_normal().unwrap();

        // Cannot initialize again from Normal.
        let err = svc.initialize().unwrap_err();
        assert!(matches!(err, StorageServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn invalid_set_normal_from_starting() {
        let svc = StorageService::new();
        // Cannot go directly to Normal from Starting.
        let err = svc.set_normal().unwrap_err();
        assert!(matches!(err, StorageServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn invalid_start_leaving_from_joining() {
        let svc = StorageService::new();
        svc.initialize().unwrap();
        // Cannot leave while still Joining.
        let err = svc.start_leaving().unwrap_err();
        assert!(matches!(err, StorageServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn invalid_finish_leaving_from_normal() {
        let svc = StorageService::new();
        svc.initialize().unwrap();
        svc.set_normal().unwrap();
        // Cannot finish leaving without starting to leave.
        let err = svc.finish_leaving().unwrap_err();
        assert!(matches!(err, StorageServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn native_transport_requires_normal() {
        let svc = StorageService::new();
        svc.initialize().unwrap();
        // Cannot start native transport while Joining.
        let err = svc.start_native_transport().unwrap_err();
        assert!(matches!(
            err,
            StorageServiceError::OperationNotAllowed { .. }
        ));
    }

    #[test]
    fn node_state_display() {
        assert_eq!(NodeState::Starting.to_string(), "STARTING");
        assert_eq!(NodeState::Joining.to_string(), "JOINING");
        assert_eq!(NodeState::Normal.to_string(), "NORMAL");
        assert_eq!(NodeState::Leaving.to_string(), "LEAVING");
        assert_eq!(NodeState::Left.to_string(), "LEFT");
    }

    #[test]
    fn default_creates_starting() {
        let svc = StorageService::default();
        assert_eq!(svc.state(), NodeState::Starting);
    }
}
