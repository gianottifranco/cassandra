// Licensed under Apache License, Version 2.0.

//! Repair history tracking interface.
//!
//! Provides an abstraction for recording repair session states and history 
//! to `system_distributed.repair_history` and `system_distributed.parent_repair_history`.

use uuid::Uuid;

use crate::coordinator::RepairType;
use crate::session::RepairSessionState;
use cassandra_common::Token;
use cassandra_cluster_metadata::Endpoint;

/// Tracker for repair history events.
pub trait RepairHistoryTracker: Send + Sync {
    /// Record the start of a coordinator-level repair.
    fn record_parent_repair_start(
        &self,
        repair_id: Uuid,
        keyspace: &str,
        tables: &[String],
        ranges: &[(Token, Token)],
        repair_type: RepairType,
    );

    /// Record the completion or failure of a coordinator-level repair.
    fn record_parent_repair_finish(
        &self,
        repair_id: Uuid,
        successful_ranges: &[(Token, Token)],
        error: Option<String>,
    );

    /// Record the start of a specific session for a token range.
    fn record_session_start(
        &self,
        session_id: Uuid,
        parent_id: Uuid,
        keyspace: &str,
        table: &str,
        range: (Token, Token),
        participants: &[Endpoint],
    );

    /// Record the completion or failure of a specific session.
    fn record_session_finish(
        &self,
        session_id: Uuid,
        state: RepairSessionState,
        error: Option<String>,
    );
}

/// A dummy history tracker that just logs the events (default if not injected).
pub struct LoggingRepairHistoryTracker;

impl RepairHistoryTracker for LoggingRepairHistoryTracker {
    fn record_parent_repair_start(
        &self,
        repair_id: Uuid,
        keyspace: &str,
        _tables: &[String],
        _ranges: &[(Token, Token)],
        repair_type: RepairType,
    ) {
        tracing::debug!(
            "Parent repair {} started (keyspace: {}, type: {})",
            repair_id, keyspace, repair_type
        );
    }

    fn record_parent_repair_finish(
        &self,
        repair_id: Uuid,
        successful_ranges: &[(Token, Token)],
        error: Option<String>,
    ) {
        if let Some(e) = error {
            tracing::warn!("Parent repair {} finished with error: {}", repair_id, e);
        } else {
            tracing::debug!("Parent repair {} finished successfully on {} ranges", repair_id, successful_ranges.len());
        }
    }

    fn record_session_start(
        &self,
        session_id: Uuid,
        parent_id: Uuid,
        keyspace: &str,
        table: &str,
        range: (Token, Token),
        _participants: &[Endpoint],
    ) {
        tracing::debug!(
            "Repair session {} (parent: {}) started for {}.{} range {:?}",
            session_id, parent_id, keyspace, table, range
        );
    }

    fn record_session_finish(
        &self,
        session_id: Uuid,
        state: RepairSessionState,
        error: Option<String>,
    ) {
        tracing::debug!("Repair session {} finished with state: {} error: {:?}", session_id, state, error);
    }
}
