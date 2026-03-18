// Licensed under Apache License, Version 2.0.

//! Virtual tables and repair state surfaces for monitoring.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.virtual.RepairsTable`
//! - `org.apache.cassandra.repair.state.SessionState`
//!
//! ## Design
//!
//! Provides an in-memory, bounded repair history that implements
//! `RepairHistoryTracker`. Entries can be queried and formatted
//! for nodetool-style output.

use std::collections::VecDeque;
use std::fmt;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::coordinator::RepairType;
use crate::history::RepairHistoryTracker;
use crate::messages::ConsistentSessionState;
use crate::session::RepairSessionState;

/// Default maximum number of history entries retained.
pub const DEFAULT_HISTORY_CAPACITY: usize = 1000;

/// A single repair history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairHistoryEntry {
    pub id: Uuid,
    pub parent_id: Uuid,
    pub keyspace: String,
    pub tables: Vec<String>,
    pub range: Option<(Token, Token)>,
    pub coordinator: Option<Endpoint>,
    pub participants: Vec<Endpoint>,
    pub state: String,
    pub started_at_millis: u64,
    pub finished_at_millis: Option<u64>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

/// Query filter for repair history.
#[derive(Debug, Default)]
pub struct RepairHistoryQuery {
    pub keyspace: Option<String>,
    pub since_millis: Option<u64>,
    pub limit: Option<usize>,
}

/// In-memory bounded repair history implementing `RepairHistoryTracker`.
pub struct InMemoryRepairHistory {
    entries: Mutex<VecDeque<RepairHistoryEntry>>,
    capacity: usize,
}

impl fmt::Debug for InMemoryRepairHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRepairHistory")
            .field("capacity", &self.capacity)
            .field("len", &self.entries.lock().len())
            .finish()
    }
}

impl InMemoryRepairHistory {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_HISTORY_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn push_entry(&self, entry: RepairHistoryEntry) {
        let mut entries = self.entries.lock();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Query the history with optional filters.
    pub fn query(&self, filter: &RepairHistoryQuery) -> Vec<RepairHistoryEntry> {
        let entries = self.entries.lock();
        let iter = entries.iter().filter(|e| {
            if let Some(ks) = &filter.keyspace {
                if e.keyspace != *ks {
                    return false;
                }
            }
            if let Some(since) = filter.since_millis {
                if e.started_at_millis < since {
                    return false;
                }
            }
            true
        });

        match filter.limit {
            Some(limit) => iter.rev().take(limit).cloned().collect(),
            None => iter.cloned().collect(),
        }
    }

    /// Number of entries currently stored.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Whether the history is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

impl Default for InMemoryRepairHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl RepairHistoryTracker for InMemoryRepairHistory {
    fn record_parent_repair_start(
        &self,
        repair_id: Uuid,
        keyspace: &str,
        tables: &[String],
        _ranges: &[(Token, Token)],
        _repair_type: RepairType,
    ) {
        self.push_entry(RepairHistoryEntry {
            id: repair_id,
            parent_id: repair_id,
            keyspace: keyspace.to_string(),
            tables: tables.to_vec(),
            range: None,
            coordinator: None,
            participants: Vec::new(),
            state: "STARTED".to_string(),
            started_at_millis: Self::now_millis(),
            finished_at_millis: None,
            error: None,
            duration_ms: None,
        });
    }

    fn record_parent_repair_finish(
        &self,
        repair_id: Uuid,
        _successful_ranges: &[(Token, Token)],
        error: Option<String>,
    ) {
        let now = Self::now_millis();
        let mut entries = self.entries.lock();
        // Find and update the parent entry.
        for entry in entries.iter_mut().rev() {
            if entry.id == repair_id && entry.parent_id == repair_id {
                entry.state = if error.is_some() {
                    "FAILED".to_string()
                } else {
                    "COMPLETE".to_string()
                };
                entry.finished_at_millis = Some(now);
                entry.error = error;
                entry.duration_ms = Some(now.saturating_sub(entry.started_at_millis));
                return;
            }
        }
    }

    fn record_session_start(
        &self,
        session_id: Uuid,
        parent_id: Uuid,
        keyspace: &str,
        table: &str,
        range: (Token, Token),
        participants: &[Endpoint],
    ) {
        self.push_entry(RepairHistoryEntry {
            id: session_id,
            parent_id,
            keyspace: keyspace.to_string(),
            tables: vec![table.to_string()],
            range: Some(range),
            coordinator: None,
            participants: participants.to_vec(),
            state: "STARTED".to_string(),
            started_at_millis: Self::now_millis(),
            finished_at_millis: None,
            error: None,
            duration_ms: None,
        });
    }

    fn record_session_finish(
        &self,
        session_id: Uuid,
        state: RepairSessionState,
        error: Option<String>,
    ) {
        let now = Self::now_millis();
        let mut entries = self.entries.lock();
        for entry in entries.iter_mut().rev() {
            if entry.id == session_id {
                entry.state = state.to_string();
                entry.finished_at_millis = Some(now);
                entry.error = error;
                entry.duration_ms = Some(now.saturating_sub(entry.started_at_millis));
                return;
            }
        }
    }
}

// ── Formatters ──────────────────────────────────────────────

/// Format repair status for nodetool-style output.
pub fn format_repair_status(statuses: &[crate::coordinator::RepairStatus]) -> String {
    let mut out = String::new();
    for s in statuses {
        out.push_str(&format!(
            "repair {} [{}] keyspace={} tables={} ranges={}/{} (failed={})\n",
            s.repair_id,
            s.repair_type,
            s.keyspace,
            s.tables.join(","),
            s.completed_ranges,
            s.total_ranges,
            s.failed_ranges,
        ));
    }
    out
}

/// Format repair history entries for nodetool-style output.
pub fn format_repair_history(entries: &[RepairHistoryEntry]) -> String {
    let mut out = String::new();
    for e in entries {
        let tables = e.tables.join(",");
        let dur = e
            .duration_ms
            .map(|d| format!("{d}ms"))
            .unwrap_or_else(|| "in-progress".to_string());
        out.push_str(&format!(
            "{} parent={} ks={} tables={} state={} duration={}\n",
            e.id, e.parent_id, e.keyspace, tables, e.state, dur,
        ));
    }
    out
}

/// View of pending consistent repair sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRepairView {
    pub parent_id: Uuid,
    pub keyspace: String,
    pub tables: Vec<String>,
    pub state: ConsistentSessionState,
    pub participants: Vec<Endpoint>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_history(cap: usize) -> InMemoryRepairHistory {
        InMemoryRepairHistory::with_capacity(cap)
    }

    #[test]
    fn capacity_enforcement() {
        let h = make_history(3);
        for _ in 0..5 {
            h.record_parent_repair_start(
                Uuid::new_v4(),
                "ks",
                &["t1".into()],
                &[],
                RepairType::Full,
            );
        }
        assert_eq!(h.len(), 3);
    }

    #[test]
    fn query_by_keyspace() {
        let h = make_history(10);
        h.record_parent_repair_start(Uuid::new_v4(), "ks1", &[], &[], RepairType::Full);
        h.record_parent_repair_start(Uuid::new_v4(), "ks2", &[], &[], RepairType::Full);

        let results = h.query(&RepairHistoryQuery {
            keyspace: Some("ks1".into()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].keyspace, "ks1");
    }

    #[test]
    fn query_with_limit() {
        let h = make_history(10);
        for _ in 0..5 {
            h.record_parent_repair_start(Uuid::new_v4(), "ks", &[], &[], RepairType::Full);
        }

        let results = h.query(&RepairHistoryQuery {
            limit: Some(2),
            ..Default::default()
        });
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn nodetool_format_status() {
        use crate::coordinator::RepairStatus;
        let statuses = vec![RepairStatus {
            repair_id: Uuid::nil(),
            repair_type: RepairType::Full,
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            total_ranges: 10,
            completed_ranges: 7,
            failed_ranges: 1,
            active: true,
        }];
        let out = format_repair_status(&statuses);
        assert!(out.contains("ks"));
        assert!(out.contains("7/10"));
        assert!(out.contains("failed=1"));
    }

    #[test]
    fn nodetool_format_history() {
        let entries = vec![RepairHistoryEntry {
            id: Uuid::nil(),
            parent_id: Uuid::nil(),
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            range: None,
            coordinator: None,
            participants: Vec::new(),
            state: "COMPLETE".into(),
            started_at_millis: 1000,
            finished_at_millis: Some(2000),
            error: None,
            duration_ms: Some(1000),
        }];
        let out = format_repair_history(&entries);
        assert!(out.contains("COMPLETE"));
        assert!(out.contains("1000ms"));
    }

    #[test]
    fn ring_buffer_eviction_order() {
        let h = make_history(2);
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        h.record_parent_repair_start(id1, "ks", &[], &[], RepairType::Full);
        h.record_parent_repair_start(id2, "ks", &[], &[], RepairType::Full);
        h.record_parent_repair_start(id3, "ks", &[], &[], RepairType::Full);

        let all = h.query(&RepairHistoryQuery::default());
        assert_eq!(all.len(), 2);
        // Oldest (id1) should be evicted
        assert!(all.iter().all(|e| e.id != id1));
    }

    #[test]
    fn session_tracking() {
        let h = make_history(10);
        let parent = Uuid::new_v4();
        let session = Uuid::new_v4();
        let ep = Endpoint::new(std::net::SocketAddr::from(([127, 0, 0, 1], 9042)));

        h.record_session_start(
            session,
            parent,
            "ks",
            "t1",
            (Token::from_raw(0), Token::from_raw(100)),
            &[ep],
        );
        h.record_session_finish(session, RepairSessionState::Complete, None);

        let all = h.query(&RepairHistoryQuery::default());
        let entry = all.iter().find(|e| e.id == session).unwrap();
        assert_eq!(entry.state, "COMPLETE");
        assert!(entry.finished_at_millis.is_some());
    }

    #[test]
    fn pending_repair_view_serde() {
        let view = PendingRepairView {
            parent_id: Uuid::new_v4(),
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            state: ConsistentSessionState::Repairing,
            participants: Vec::new(),
        };
        let json = serde_json::to_string(&view).unwrap();
        let deser: PendingRepairView = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.keyspace, "ks");
    }
}
