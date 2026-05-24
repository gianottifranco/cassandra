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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use cassandra_common::Token;
use parking_lot::RwLock;
use tracing::info;

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
    /// Tokens currently owned by this node.
    owned_tokens: Arc<RwLock<BTreeSet<Token>>>,
    /// Tokens assigned for an in-progress bootstrap.
    pending_bootstrap_tokens: Arc<RwLock<BTreeSet<Token>>>,
    /// Target tokens for an in-place token migration.
    pending_token_migration: Arc<RwLock<BTreeSet<Token>>>,
    /// Snapshot of owned tokens when starting leave/decommission.
    leaving_tokens: Arc<RwLock<BTreeSet<Token>>>,
}

impl StorageService {
    /// Create a new StorageService in the Starting state.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(NodeState::Starting)),
            native_transport_running: Arc::new(RwLock::new(false)),
            owned_tokens: Arc::new(RwLock::new(BTreeSet::new())),
            pending_bootstrap_tokens: Arc::new(RwLock::new(BTreeSet::new())),
            pending_token_migration: Arc::new(RwLock::new(BTreeSet::new())),
            leaving_tokens: Arc::new(RwLock::new(BTreeSet::new())),
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

    /// Tokens currently owned by this node.
    pub fn owned_tokens(&self) -> Vec<Token> {
        self.owned_tokens.read().iter().copied().collect()
    }

    /// Tokens staged for bootstrap while the node is Joining.
    pub fn pending_bootstrap_tokens(&self) -> Vec<Token> {
        self.pending_bootstrap_tokens
            .read()
            .iter()
            .copied()
            .collect()
    }

    /// Tokens staged for a token migration while the node is Normal.
    pub fn pending_token_migration(&self) -> Vec<Token> {
        self.pending_token_migration
            .read()
            .iter()
            .copied()
            .collect()
    }

    /// Tokens captured at leave start.
    pub fn leaving_tokens(&self) -> Vec<Token> {
        self.leaving_tokens.read().iter().copied().collect()
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

    /// Initialize with explicit bootstrap tokens (Starting → Joining).
    ///
    /// This models token assignment during bootstrap and is promoted
    /// to owned tokens once the node transitions to Normal.
    pub fn initialize_with_tokens(
        &self,
        tokens: impl IntoIterator<Item = Token>,
    ) -> Result<(), StorageServiceError> {
        let token_set = tokens.into_iter().collect::<BTreeSet<_>>();
        if token_set.is_empty() {
            return Err(StorageServiceError::InitializationError(
                "bootstrap requires at least one token".to_string(),
            ));
        }

        let mut state = self.state.write();
        if *state != NodeState::Starting {
            return Err(StorageServiceError::InvalidTransition {
                from: state.to_string(),
                to: NodeState::Joining.to_string(),
            });
        }

        *self.pending_bootstrap_tokens.write() = token_set;
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

        {
            let mut pending = self.pending_bootstrap_tokens.write();
            if !pending.is_empty() {
                *self.owned_tokens.write() = pending.clone();
                pending.clear();
            }
        }

        *state = NodeState::Normal;
        info!(state = %NodeState::Normal, "StorageService state transition");
        Ok(())
    }

    /// Stage an in-place token migration while in Normal state.
    pub fn plan_token_migration(
        &self,
        target_tokens: impl IntoIterator<Item = Token>,
    ) -> Result<(), StorageServiceError> {
        let state = self.state.read();
        if *state != NodeState::Normal {
            return Err(StorageServiceError::OperationNotAllowed {
                state: state.to_string(),
            });
        }
        drop(state);

        let target = target_tokens.into_iter().collect::<BTreeSet<_>>();
        if target.is_empty() {
            return Err(StorageServiceError::InitializationError(
                "token migration requires at least one target token".to_string(),
            ));
        }
        *self.pending_token_migration.write() = target;
        Ok(())
    }

    /// Apply a planned token migration in Normal state.
    pub fn apply_token_migration(&self) -> Result<(), StorageServiceError> {
        let state = self.state.read();
        if *state != NodeState::Normal {
            return Err(StorageServiceError::OperationNotAllowed {
                state: state.to_string(),
            });
        }
        drop(state);

        let mut pending = self.pending_token_migration.write();
        if pending.is_empty() {
            return Err(StorageServiceError::InitializationError(
                "token migration has no planned target".to_string(),
            ));
        }

        *self.owned_tokens.write() = pending.clone();
        pending.clear();
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

        if !self.pending_token_migration.read().is_empty() {
            return Err(StorageServiceError::OperationNotAllowed {
                state: "NORMAL (pending token migration)".to_string(),
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
        *self.leaving_tokens.write() = self.owned_tokens.read().clone();

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

        self.owned_tokens.write().clear();
        self.pending_bootstrap_tokens.write().clear();
        self.pending_token_migration.write().clear();
        self.leaving_tokens.write().clear();
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
        assert!(svc.owned_tokens().is_empty());
        assert!(svc.pending_bootstrap_tokens().is_empty());
        assert!(svc.pending_token_migration().is_empty());
        assert!(svc.leaving_tokens().is_empty());
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

    #[test]
    fn initialize_with_tokens_promotes_tokens_on_normal() {
        let svc = StorageService::new();
        svc.initialize_with_tokens([Token::from_raw(-10), Token::from_raw(5)])
            .unwrap();
        assert_eq!(svc.state(), NodeState::Joining);
        assert_eq!(
            svc.pending_bootstrap_tokens(),
            vec![Token::from_raw(-10), Token::from_raw(5)]
        );
        assert!(svc.owned_tokens().is_empty());

        svc.set_normal().unwrap();
        assert_eq!(svc.state(), NodeState::Normal);
        assert_eq!(
            svc.owned_tokens(),
            vec![Token::from_raw(-10), Token::from_raw(5)]
        );
        assert!(svc.pending_bootstrap_tokens().is_empty());
    }

    #[test]
    fn initialize_with_tokens_requires_non_empty_input() {
        let svc = StorageService::new();
        let err = svc.initialize_with_tokens(Vec::<Token>::new()).unwrap_err();
        assert!(matches!(err, StorageServiceError::InitializationError(_)));
    }

    #[test]
    fn token_migration_plan_and_apply_updates_owned_tokens() {
        let svc = StorageService::new();
        svc.initialize_with_tokens([Token::from_raw(1)]).unwrap();
        svc.set_normal().unwrap();
        assert_eq!(svc.owned_tokens(), vec![Token::from_raw(1)]);

        svc.plan_token_migration([Token::from_raw(2), Token::from_raw(3)])
            .unwrap();
        assert_eq!(
            svc.pending_token_migration(),
            vec![Token::from_raw(2), Token::from_raw(3)]
        );

        svc.apply_token_migration().unwrap();
        assert!(svc.pending_token_migration().is_empty());
        assert_eq!(
            svc.owned_tokens(),
            vec![Token::from_raw(2), Token::from_raw(3)]
        );
    }

    #[test]
    fn apply_token_migration_requires_planned_target() {
        let svc = StorageService::new();
        svc.initialize().unwrap();
        svc.set_normal().unwrap();

        let err = svc.apply_token_migration().unwrap_err();
        assert!(matches!(err, StorageServiceError::InitializationError(_)));
    }

    #[test]
    fn start_leaving_rejects_pending_token_migration() {
        let svc = StorageService::new();
        svc.initialize_with_tokens([Token::from_raw(1)]).unwrap();
        svc.set_normal().unwrap();
        svc.plan_token_migration([Token::from_raw(2)]).unwrap();

        let err = svc.start_leaving().unwrap_err();
        assert!(matches!(
            err,
            StorageServiceError::OperationNotAllowed { .. }
        ));
    }

    #[test]
    fn leaving_tracks_and_clears_tokens() {
        let svc = StorageService::new();
        svc.initialize_with_tokens([Token::from_raw(7), Token::from_raw(9)])
            .unwrap();
        svc.set_normal().unwrap();
        svc.start_leaving().unwrap();
        assert_eq!(
            svc.leaving_tokens(),
            vec![Token::from_raw(7), Token::from_raw(9)]
        );
        svc.finish_leaving().unwrap();
        assert!(svc.owned_tokens().is_empty());
        assert!(svc.leaving_tokens().is_empty());
    }
}
