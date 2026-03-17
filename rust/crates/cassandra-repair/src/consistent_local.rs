// Licensed under Apache License, Version 2.0.

//! Participant-side state machine for consistent (incremental) repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.consistent.LocalSession`
//! - `org.apache.cassandra.repair.consistent.LocalSessions`
//!
//! ## Design
//!
//! Each participant in a consistent repair maintains a `LocalSession` that
//! tracks the repair lifecycle. Sessions are persisted so that incomplete
//! repairs can be recovered after a restart.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::messages::{
    ConsistentSessionState, FinalizePromise, PrepareConsistentRequest,
    PrepareConsistentResponse,
};

/// Participant-side session for consistent repair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSession {
    /// Parent session identifier (matches coordinator).
    pub parent_id: Uuid,
    /// Current state.
    pub state: ConsistentSessionState,
    /// Coordinator endpoint.
    pub coordinator: Endpoint,
    /// Keyspace being repaired.
    pub keyspace: String,
    /// Tables being repaired.
    pub tables: Vec<String>,
    /// Token ranges being repaired.
    pub ranges: Vec<(Token, Token)>,
    /// Timestamp of session creation (epoch millis for serde).
    pub created_at_millis: u64,
    /// Timestamp of last update (epoch millis for serde).
    pub last_update_millis: u64,
}

impl LocalSession {
    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Handle a prepare request from the coordinator.
    pub fn handle_prepare(
        req: &PrepareConsistentRequest,
        coordinator: Endpoint,
        this_endpoint: Endpoint,
    ) -> (Self, PrepareConsistentResponse) {
        let now = Self::now_millis();
        let session = Self {
            parent_id: req.parent_id,
            state: ConsistentSessionState::Prepared,
            coordinator,
            keyspace: req.keyspace.clone(),
            tables: req.tables.clone(),
            ranges: req.ranges.clone(),
            created_at_millis: now,
            last_update_millis: now,
        };

        let resp = PrepareConsistentResponse {
            parent_id: req.parent_id,
            endpoint: this_endpoint,
            success: true,
        };

        (session, resp)
    }

    /// Handle a finalize-propose message.
    pub fn handle_finalize_propose(
        &mut self,
        this_endpoint: Endpoint,
    ) -> FinalizePromise {
        if self.state == ConsistentSessionState::Prepared
            || self.state == ConsistentSessionState::Repairing
        {
            self.state = ConsistentSessionState::FinalizePromised;
            self.last_update_millis = Self::now_millis();
            FinalizePromise {
                parent_id: self.parent_id,
                endpoint: this_endpoint,
                success: true,
            }
        } else {
            FinalizePromise {
                parent_id: self.parent_id,
                endpoint: this_endpoint,
                success: false,
            }
        }
    }

    /// Handle a commit message.
    pub fn handle_commit(&mut self) -> Result<(), String> {
        if self.state != ConsistentSessionState::FinalizePromised {
            return Err(format!(
                "Cannot commit from state {}",
                self.state
            ));
        }
        self.state = ConsistentSessionState::Committed;
        self.last_update_millis = Self::now_millis();
        Ok(())
    }

    /// Handle a fail message.
    pub fn handle_fail(&mut self) {
        self.state = ConsistentSessionState::Failed;
        self.last_update_millis = Self::now_millis();
    }

    /// Mark as repairing (after prepare, before finalize).
    pub fn set_repairing(&mut self) {
        if self.state == ConsistentSessionState::Prepared {
            self.state = ConsistentSessionState::Repairing;
            self.last_update_millis = Self::now_millis();
        }
    }
}

/// Trait for persisting local sessions across restarts.
pub trait LocalSessionStore: Send + Sync {
    fn save(&self, session: &LocalSession) -> Result<(), String>;
    fn load(&self, parent_id: Uuid) -> Result<Option<LocalSession>, String>;
    fn delete(&self, parent_id: Uuid) -> Result<(), String>;
    fn list_pending(&self) -> Result<Vec<LocalSession>, String>;
}

/// In-memory session store (also useful as a test double).
#[derive(Debug, Default)]
pub struct InMemoryLocalSessionStore {
    sessions: parking_lot::Mutex<HashMap<Uuid, LocalSession>>,
}

impl LocalSessionStore for InMemoryLocalSessionStore {
    fn save(&self, session: &LocalSession) -> Result<(), String> {
        self.sessions
            .lock()
            .insert(session.parent_id, session.clone());
        Ok(())
    }

    fn load(&self, parent_id: Uuid) -> Result<Option<LocalSession>, String> {
        Ok(self.sessions.lock().get(&parent_id).cloned())
    }

    fn delete(&self, parent_id: Uuid) -> Result<(), String> {
        self.sessions.lock().remove(&parent_id);
        Ok(())
    }

    fn list_pending(&self) -> Result<Vec<LocalSession>, String> {
        Ok(self
            .sessions
            .lock()
            .values()
            .filter(|s| {
                !matches!(
                    s.state,
                    ConsistentSessionState::Committed | ConsistentSessionState::Failed
                )
            })
            .cloned()
            .collect())
    }
}

/// Manages all active local sessions on a participant node.
pub struct PendingRepairTracker {
    store: Box<dyn LocalSessionStore>,
}

impl std::fmt::Debug for PendingRepairTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRepairTracker").finish()
    }
}

impl PendingRepairTracker {
    pub fn new(store: Box<dyn LocalSessionStore>) -> Self {
        Self { store }
    }

    pub fn add_session(&self, session: &LocalSession) -> Result<(), String> {
        self.store.save(session)
    }

    pub fn get_session(&self, parent_id: Uuid) -> Result<Option<LocalSession>, String> {
        self.store.load(parent_id)
    }

    pub fn update_session(&self, session: &LocalSession) -> Result<(), String> {
        self.store.save(session)
    }

    pub fn remove_session(&self, parent_id: Uuid) -> Result<(), String> {
        self.store.delete(parent_id)
    }

    pub fn pending_sessions(&self) -> Result<Vec<LocalSession>, String> {
        self.store.list_pending()
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

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    fn make_prepare_req(parent_id: Uuid) -> PrepareConsistentRequest {
        PrepareConsistentRequest {
            parent_id,
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            ranges: vec![(tok(0), tok(100))],
            participants: vec![ep(7001), ep(7002)],
            is_forced: false,
        }
    }

    #[test]
    fn full_lifecycle() {
        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);

        // Prepare
        let (mut session, resp) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));
        assert!(resp.success);
        assert_eq!(session.state, ConsistentSessionState::Prepared);

        // Set repairing
        session.set_repairing();
        assert_eq!(session.state, ConsistentSessionState::Repairing);

        // Finalize propose
        let promise = session.handle_finalize_propose(ep(7001));
        assert!(promise.success);
        assert_eq!(session.state, ConsistentSessionState::FinalizePromised);

        // Commit
        session.handle_commit().unwrap();
        assert_eq!(session.state, ConsistentSessionState::Committed);
    }

    #[test]
    fn failure_cleanup() {
        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);
        let (mut session, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));

        session.handle_fail();
        assert_eq!(session.state, ConsistentSessionState::Failed);

        // Cannot commit after fail
        let result = session.handle_commit();
        assert!(result.is_err());
    }

    #[test]
    fn persistence_round_trip() {
        let store = InMemoryLocalSessionStore::default();
        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);
        let (session, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));

        store.save(&session).unwrap();
        let loaded = store.load(parent_id).unwrap().unwrap();
        assert_eq!(loaded.parent_id, parent_id);
        assert_eq!(loaded.keyspace, "ks");
    }

    #[test]
    fn concurrent_sessions() {
        let store = InMemoryLocalSessionStore::default();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let req1 = make_prepare_req(id1);
        let req2 = make_prepare_req(id2);

        let (s1, _) = LocalSession::handle_prepare(&req1, ep(9000), ep(7001));
        let (s2, _) = LocalSession::handle_prepare(&req2, ep(9000), ep(7001));

        store.save(&s1).unwrap();
        store.save(&s2).unwrap();

        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 2);

        store.delete(id1).unwrap();
        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn pending_repair_tracker() {
        let store = Box::new(InMemoryLocalSessionStore::default());
        let tracker = PendingRepairTracker::new(store);

        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);
        let (session, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));

        tracker.add_session(&session).unwrap();
        assert!(tracker.get_session(parent_id).unwrap().is_some());

        let pending = tracker.pending_sessions().unwrap();
        assert_eq!(pending.len(), 1);

        tracker.remove_session(parent_id).unwrap();
        assert!(tracker.get_session(parent_id).unwrap().is_none());
    }

    #[test]
    fn serde_round_trip() {
        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);
        let (session, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));

        let json = serde_json::to_string(&session).unwrap();
        let deser: LocalSession = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.parent_id, parent_id);
        assert_eq!(deser.state, ConsistentSessionState::Prepared);
    }

    #[test]
    fn finalize_propose_wrong_state() {
        let parent_id = Uuid::new_v4();
        let req = make_prepare_req(parent_id);
        let (mut session, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));

        // Commit (wrong state for propose after commit)
        session.state = ConsistentSessionState::Committed;
        let promise = session.handle_finalize_propose(ep(7001));
        assert!(!promise.success);
    }
}
