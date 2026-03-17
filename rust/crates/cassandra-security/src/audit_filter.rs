// Licensed under Apache License, Version 2.0.

//! Audit filter enforcement.
//!
//! Determines whether a given audit event should be logged based on the
//! configured [`AuditLoggingOptions`] — filtering by keyspace and category.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::audit::{AuditEventType, AuditLoggingOptions};

// ─── AuditCategory ───────────────────────────────────────────────────────────

/// High-level category for audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditCategory {
    Auth,
    Query,
    Dml,
    Ddl,
    Dcl,
    Other,
}

impl AuditCategory {
    /// Map an [`AuditEventType`] to its high-level category.
    pub fn from_event_type(event_type: &AuditEventType) -> Self {
        match event_type {
            AuditEventType::AuthSuccess
            | AuditEventType::AuthFailure
            | AuditEventType::LoginError => Self::Auth,

            AuditEventType::Query => Self::Query,

            AuditEventType::DmlRead | AuditEventType::DmlWrite => Self::Dml,

            AuditEventType::DdlCreate
            | AuditEventType::DdlAlter
            | AuditEventType::DdlDrop => Self::Ddl,

            AuditEventType::DclGrant
            | AuditEventType::DclRevoke
            | AuditEventType::RoleCreate
            | AuditEventType::RoleAlter
            | AuditEventType::RoleDrop => Self::Dcl,

            AuditEventType::Unauthorized => Self::Other,
        }
    }
}

impl fmt::Display for AuditCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auth => write!(f, "AUTH"),
            Self::Query => write!(f, "QUERY"),
            Self::Dml => write!(f, "DML"),
            Self::Ddl => write!(f, "DDL"),
            Self::Dcl => write!(f, "DCL"),
            Self::Other => write!(f, "OTHER"),
        }
    }
}

// ─── AuditLogContext ─────────────────────────────────────────────────────────

/// Contextual information about an audit-worthy operation.
#[derive(Debug, Clone)]
pub struct AuditLogContext {
    pub user: String,
    pub source_address: String,
    pub keyspace: Option<String>,
    pub table: Option<String>,
    pub event_type: AuditEventType,
}

// ─── AuditFilter ─────────────────────────────────────────────────────────────

/// Decides whether a given audit event should be logged based on the
/// configured include/exclude lists for keyspaces and categories.
#[derive(Debug, Clone)]
pub struct AuditFilter {
    options: AuditLoggingOptions,
}

impl AuditFilter {
    pub fn new(options: AuditLoggingOptions) -> Self {
        Self { options }
    }

    /// Returns `true` if the event described by `ctx` should be recorded.
    pub fn should_log(&self, ctx: &AuditLogContext) -> bool {
        if !self.options.enabled {
            return false;
        }

        // Keyspace inclusion filter
        if let Some(ref included) = self.options.included_keyspaces {
            match ctx.keyspace {
                Some(ref ks) if included.contains(ks) => {}
                _ => return false,
            }
        }

        // Keyspace exclusion filter
        if let Some(ref excluded) = self.options.excluded_keyspaces {
            if let Some(ref ks) = ctx.keyspace {
                if excluded.contains(ks) {
                    return false;
                }
            }
        }

        let category = AuditCategory::from_event_type(&ctx.event_type).to_string();

        // Category inclusion filter
        if let Some(ref included) = self.options.included_categories {
            if !included.contains(&category) {
                return false;
            }
        }

        // Category exclusion filter
        if let Some(ref excluded) = self.options.excluded_categories {
            if excluded.contains(&category) {
                return false;
            }
        }

        true
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_options() -> AuditLoggingOptions {
        AuditLoggingOptions {
            enabled: true,
            logger: "FileAuditLogger".into(),
            audit_logs_dir: None,
            roll_cycle: None,
            max_log_size_mb: None,
            included_keyspaces: None,
            excluded_keyspaces: None,
            included_categories: None,
            excluded_categories: None,
        }
    }

    fn ctx(event_type: AuditEventType, keyspace: Option<&str>) -> AuditLogContext {
        AuditLogContext {
            user: "admin".into(),
            source_address: "127.0.0.1".into(),
            keyspace: keyspace.map(String::from),
            table: None,
            event_type,
        }
    }

    #[test]
    fn disabled_returns_false() {
        let mut opts = default_options();
        opts.enabled = false;
        let filter = AuditFilter::new(opts);
        assert!(!filter.should_log(&ctx(AuditEventType::Query, Some("ks"))));
    }

    #[test]
    fn enabled_no_filters_returns_true() {
        let filter = AuditFilter::new(default_options());
        assert!(filter.should_log(&ctx(AuditEventType::Query, Some("ks"))));
    }

    #[test]
    fn included_keyspaces_allows_matching() {
        let mut opts = default_options();
        opts.included_keyspaces = Some(vec!["ks1".into(), "ks2".into()]);
        let filter = AuditFilter::new(opts);

        assert!(filter.should_log(&ctx(AuditEventType::Query, Some("ks1"))));
        assert!(!filter.should_log(&ctx(AuditEventType::Query, Some("ks3"))));
    }

    #[test]
    fn included_keyspaces_rejects_none_keyspace() {
        let mut opts = default_options();
        opts.included_keyspaces = Some(vec!["ks1".into()]);
        let filter = AuditFilter::new(opts);
        assert!(!filter.should_log(&ctx(AuditEventType::Query, None)));
    }

    #[test]
    fn excluded_keyspaces_blocks_matching() {
        let mut opts = default_options();
        opts.excluded_keyspaces = Some(vec!["system".into()]);
        let filter = AuditFilter::new(opts);

        assert!(!filter.should_log(&ctx(AuditEventType::Query, Some("system"))));
        assert!(filter.should_log(&ctx(AuditEventType::Query, Some("ks1"))));
    }

    #[test]
    fn included_categories_allows_matching() {
        let mut opts = default_options();
        opts.included_categories = Some(vec!["AUTH".into(), "QUERY".into()]);
        let filter = AuditFilter::new(opts);

        assert!(filter.should_log(&ctx(AuditEventType::AuthSuccess, None)));
        assert!(filter.should_log(&ctx(AuditEventType::Query, None)));
        assert!(!filter.should_log(&ctx(AuditEventType::DmlRead, None)));
    }

    #[test]
    fn excluded_categories_blocks_matching() {
        let mut opts = default_options();
        opts.excluded_categories = Some(vec!["DML".into()]);
        let filter = AuditFilter::new(opts);

        assert!(!filter.should_log(&ctx(AuditEventType::DmlRead, None)));
        assert!(!filter.should_log(&ctx(AuditEventType::DmlWrite, None)));
        assert!(filter.should_log(&ctx(AuditEventType::Query, None)));
    }

    #[test]
    fn category_from_event_type_mappings() {
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::AuthSuccess), AuditCategory::Auth);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::AuthFailure), AuditCategory::Auth);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::LoginError), AuditCategory::Auth);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::Query), AuditCategory::Query);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DmlRead), AuditCategory::Dml);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DmlWrite), AuditCategory::Dml);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DdlCreate), AuditCategory::Ddl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DdlAlter), AuditCategory::Ddl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DdlDrop), AuditCategory::Ddl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DclGrant), AuditCategory::Dcl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::DclRevoke), AuditCategory::Dcl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::RoleCreate), AuditCategory::Dcl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::RoleAlter), AuditCategory::Dcl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::RoleDrop), AuditCategory::Dcl);
        assert_eq!(AuditCategory::from_event_type(&AuditEventType::Unauthorized), AuditCategory::Other);
    }

    #[test]
    fn category_display() {
        assert_eq!(AuditCategory::Auth.to_string(), "AUTH");
        assert_eq!(AuditCategory::Query.to_string(), "QUERY");
        assert_eq!(AuditCategory::Dml.to_string(), "DML");
        assert_eq!(AuditCategory::Ddl.to_string(), "DDL");
        assert_eq!(AuditCategory::Dcl.to_string(), "DCL");
        assert_eq!(AuditCategory::Other.to_string(), "OTHER");
    }

    #[test]
    fn combined_keyspace_and_category_filters() {
        let mut opts = default_options();
        opts.included_keyspaces = Some(vec!["ks1".into()]);
        opts.excluded_categories = Some(vec!["DML".into()]);
        let filter = AuditFilter::new(opts);

        // Right keyspace, allowed category
        assert!(filter.should_log(&ctx(AuditEventType::Query, Some("ks1"))));
        // Right keyspace, excluded category
        assert!(!filter.should_log(&ctx(AuditEventType::DmlRead, Some("ks1"))));
        // Wrong keyspace
        assert!(!filter.should_log(&ctx(AuditEventType::Query, Some("ks2"))));
    }
}
