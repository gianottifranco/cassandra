// Licensed under Apache License, Version 2.0.

//! Audit logging subsystem.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.audit.AuditLogManager`
//! - `org.apache.cassandra.audit.IAuditLogger`
//! - `org.apache.cassandra.audit.FileAuditLogger`
//!
//! ## Design
//! Uses an async channel to decouple audit event emission from
//! the hot path. A background task drains the channel and writes
//! to the configured sink.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

// ─── Audit Event ───────────────────────────────────────────────────────────

/// Type of audited operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    AuthSuccess,
    AuthFailure,
    Query,
    DmlRead,
    DmlWrite,
    DdlCreate,
    DdlAlter,
    DdlDrop,
    DclGrant,
    DclRevoke,
    RoleCreate,
    RoleAlter,
    RoleDrop,
    Unauthorized,
    LoginError,
}

impl fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthSuccess => write!(f, "AUTH_SUCCESS"),
            Self::AuthFailure => write!(f, "AUTH_FAILURE"),
            Self::Query => write!(f, "QUERY"),
            Self::DmlRead => write!(f, "DML_READ"),
            Self::DmlWrite => write!(f, "DML_WRITE"),
            Self::DdlCreate => write!(f, "DDL_CREATE"),
            Self::DdlAlter => write!(f, "DDL_ALTER"),
            Self::DdlDrop => write!(f, "DDL_DROP"),
            Self::DclGrant => write!(f, "DCL_GRANT"),
            Self::DclRevoke => write!(f, "DCL_REVOKE"),
            Self::RoleCreate => write!(f, "ROLE_CREATE"),
            Self::RoleAlter => write!(f, "ROLE_ALTER"),
            Self::RoleDrop => write!(f, "ROLE_DROP"),
            Self::Unauthorized => write!(f, "UNAUTHORIZED"),
            Self::LoginError => write!(f, "LOGIN_ERROR"),
        }
    }
}

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub timestamp: u64, // epoch millis
    pub event_type: AuditEventType,
    pub user: String,
    pub source_address: String,
    pub keyspace: Option<String>,
    pub table: Option<String>,
    pub query: Option<String>,
    pub status: AuditStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditStatus {
    Success,
    Failure,
}

impl AuditEvent {
    pub fn now(
        event_type: AuditEventType,
        user: impl Into<String>,
        source_address: impl Into<String>,
    ) -> Self {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            timestamp: ts,
            event_type,
            user: user.into(),
            source_address: source_address.into(),
            keyspace: None,
            table: None,
            query: None,
            status: AuditStatus::Success,
        }
    }

    pub fn with_keyspace(mut self, ks: impl Into<String>) -> Self {
        self.keyspace = Some(ks.into());
        self
    }

    pub fn with_table(mut self, tbl: impl Into<String>) -> Self {
        self.table = Some(tbl.into());
        self
    }

    pub fn with_query(mut self, q: impl Into<String>) -> Self {
        self.query = Some(q.into());
        self
    }

    pub fn with_status(mut self, status: AuditStatus) -> Self {
        self.status = status;
        self
    }
}

// ─── Audit Logger Trait ────────────────────────────────────────────────────

/// Pluggable audit logging interface.
pub trait AuditLogger: Send + Sync {
    /// Log an audit event.
    fn log(&self, event: &AuditEvent);

    /// Whether audit logging is enabled.
    fn is_enabled(&self) -> bool;

    /// Name of this logger implementation.
    fn name(&self) -> &str;
}

// ─── NoOpAuditLogger ───────────────────────────────────────────────────────

/// Default audit logger that does nothing.
pub struct NoOpAuditLogger;

impl AuditLogger for NoOpAuditLogger {
    fn log(&self, _event: &AuditEvent) {}
    fn is_enabled(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "NoOpAuditLogger"
    }
}

// ─── FileAuditLogger ──────────────────────────────────────────────────────

/// Writes audit events as JSON lines to a rotating file.
pub struct FileAuditLogger {
    log_dir: PathBuf,
    max_file_size: u64,
}

impl FileAuditLogger {
    pub fn new(log_dir: PathBuf, max_file_size_mb: u64) -> Result<Self, std::io::Error> {
        fs::create_dir_all(&log_dir)?;
        Ok(Self {
            log_dir,
            max_file_size: max_file_size_mb * 1024 * 1024,
        })
    }

    fn current_log_path(&self) -> PathBuf {
        self.log_dir.join("audit.log")
    }

    fn should_rotate(&self, current_path: &Path) -> bool {
        if let Ok(metadata) = fs::metadata(current_path) {
            metadata.len() >= self.max_file_size
        } else {
            false
        }
    }

    fn rotate(&self) {
        let current = self.current_log_path();
        if !current.exists() {
            return;
        }
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let rotated = self.log_dir.join(format!("audit-{}.log", ts));
        if let Err(e) = fs::rename(&current, &rotated) {
            error!("Audit log rotation failed: {}", e);
        }
    }
}

impl AuditLogger for FileAuditLogger {
    fn log(&self, event: &AuditEvent) {
        let path = self.current_log_path();
        if self.should_rotate(&path) {
            self.rotate();
        }

        let line = match serde_json::to_string(event) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize audit event: {}", e);
                return;
            }
        };

        let result = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| writeln!(f, "{}", line));

        if let Err(e) = result {
            error!("Failed to write audit log: {}", e);
        }
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "FileAuditLogger"
    }
}

// ─── Async Audit Logger ───────────────────────────────────────────────────

/// Channel-based async wrapper that decouples audit emission from the hot path.
pub struct AsyncAuditLogger {
    sender: tokio::sync::mpsc::UnboundedSender<AuditEvent>,
}

impl AsyncAuditLogger {
    /// Create a new async audit logger wrapping the given sink.
    /// Returns the logger and a JoinHandle for the background drain task.
    pub fn new(sink: Box<dyn AuditLogger>) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AuditEvent>();

        let handle = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                sink.log(&event);
            }
            debug!("Async audit logger shutting down");
        });

        (Self { sender: tx }, handle)
    }

    /// Send an audit event (non-blocking).
    pub fn send_event(&self, event: AuditEvent) {
        if let Err(e) = self.sender.send(event) {
            warn!("Audit event dropped: {}", e);
        }
    }
}

impl AuditLogger for AsyncAuditLogger {
    fn log(&self, event: &AuditEvent) {
        self.send_event(event.clone());
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "AsyncAuditLogger"
    }
}

// ─── Audit Configuration ──────────────────────────────────────────────────

/// Configuration for audit logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLoggingOptions {
    pub enabled: bool,
    pub logger: String, // "FileAuditLogger" or "NoOpAuditLogger"
    pub audit_logs_dir: Option<String>,
    pub roll_cycle: Option<String>, // "DAILY", "HOURLY"
    pub max_log_size_mb: Option<u64>,
    pub included_keyspaces: Option<Vec<String>>,
    pub excluded_keyspaces: Option<Vec<String>>,
    pub included_categories: Option<Vec<String>>,
    pub excluded_categories: Option<Vec<String>>,
}

impl Default for AuditLoggingOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            logger: "NoOpAuditLogger".into(),
            audit_logs_dir: Some("logs/audit".into()),
            roll_cycle: Some("DAILY".into()),
            max_log_size_mb: Some(100),
            included_keyspaces: None,
            excluded_keyspaces: Some(vec!["system".into(), "system_schema".into()]),
            included_categories: None,
            excluded_categories: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn noop_logger() {
        let logger = NoOpAuditLogger;
        let event = AuditEvent::now(AuditEventType::AuthSuccess, "user", "127.0.0.1");
        logger.log(&event);
        assert!(!logger.is_enabled());
    }

    #[test]
    fn audit_event_builder() {
        let event = AuditEvent::now(AuditEventType::DmlWrite, "admin", "10.0.0.1")
            .with_keyspace("ks")
            .with_table("users")
            .with_query("INSERT INTO ks.users ...")
            .with_status(AuditStatus::Success);

        assert_eq!(event.keyspace.as_deref(), Some("ks"));
        assert_eq!(event.table.as_deref(), Some("users"));
        assert_eq!(event.event_type, AuditEventType::DmlWrite);
    }

    #[test]
    fn audit_event_serialization() {
        let event = AuditEvent::now(AuditEventType::Query, "reader", "::1")
            .with_query("SELECT * FROM ks.t");
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("Query"));
        assert!(json.contains("reader"));

        let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.user, "reader");
    }

    #[test]
    fn file_audit_logger_writes() {
        let dir = TempDir::new().unwrap();
        let logger = FileAuditLogger::new(dir.path().to_path_buf(), 10).unwrap();
        assert!(logger.is_enabled());

        let event = AuditEvent::now(AuditEventType::AuthSuccess, "admin", "127.0.0.1");
        logger.log(&event);

        let log_path = dir.path().join("audit.log");
        assert!(log_path.exists());
        let content = fs::read_to_string(log_path).unwrap();
        assert!(content.contains("admin"));
        assert!(content.contains("AuthSuccess"));
    }

    #[test]
    fn file_audit_logger_multiple_events() {
        let dir = TempDir::new().unwrap();
        let logger = FileAuditLogger::new(dir.path().to_path_buf(), 10).unwrap();

        for i in 0..5 {
            let event = AuditEvent::now(AuditEventType::Query, format!("user{}", i), "10.0.0.1")
                .with_query(format!("SELECT {}", i));
            logger.log(&event);
        }

        let content = fs::read_to_string(dir.path().join("audit.log")).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 5);
    }

    #[test]
    fn audit_event_type_display() {
        assert_eq!(format!("{}", AuditEventType::AuthSuccess), "AUTH_SUCCESS");
        assert_eq!(format!("{}", AuditEventType::DdlCreate), "DDL_CREATE");
    }

    #[tokio::test]
    async fn async_audit_logger() {
        let dir = TempDir::new().unwrap();
        let file_logger = FileAuditLogger::new(dir.path().to_path_buf(), 10).unwrap();
        let (async_logger, handle) = AsyncAuditLogger::new(Box::new(file_logger));

        let event =
            AuditEvent::now(AuditEventType::DmlRead, "reader", "::1").with_query("SELECT * FROM t");
        async_logger.log(event);

        // Drop the sender to signal shutdown
        drop(async_logger);
        handle.await.unwrap();

        let content = fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(content.contains("DmlRead"));
    }
}
