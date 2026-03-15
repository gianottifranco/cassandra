// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! # cassandra-security
//!
//! Authentication, authorization, TLS/SSL, audit logging, CIDR access control,
//! dynamic data masking, and full query logging.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.auth` — authentication & authorization
//! - `org.apache.cassandra.security` — TLS/SSL factory
//! - `org.apache.cassandra.audit` — audit logging
//! - `org.apache.cassandra.fql` — full query logging
//!
//! ## Modules
//!
//! - [`tls`] — TLS configuration, cert loading, reloadable acceptor
//! - [`auth`] — Authenticator trait, PasswordAuthenticator, AllowAllAuthenticator
//! - [`roles`] — Role management (create/alter/drop/grant/revoke)
//! - [`authz`] — Authorization (permissions, resources, CassandraAuthorizer)
//! - [`masking`] — Dynamic Data Masking (DDM) functions
//! - [`cidr`] — CIDR-based network access control
//! - [`audit`] — Audit logging with configurable sinks
//! - [`fql`] — Full Query Logging (binary format)

pub mod tls;
pub mod auth;
pub mod roles;
pub mod authz;
pub mod masking;
pub mod cidr;
pub mod audit;
pub mod fql;

// Re-export key types
pub use auth::{Authenticator, AuthenticatedUser, Credentials, AllowAllAuthenticator, PasswordAuthenticator};
pub use roles::{Role, RoleManager, RoleOptions, InMemoryRoleManager};
pub use authz::{Authorizer, AllowAllAuthorizer, CassandraAuthorizer, Permission, Resource};
pub use masking::{MaskingFunction, MaskingRegistry};
pub use cidr::CidrAuthorizer;
pub use audit::{AuditEvent, AuditEventType, AuditLogger, AuditStatus, NoOpAuditLogger, FileAuditLogger, AsyncAuditLogger, AuditLoggingOptions};
pub use fql::{FqlLogger, FqlReader, FqlRecord, FqlOptions};
pub use tls::{TlsConfig, TlsVersion, ReloadableTlsAcceptor};

// ─── Security Error ────────────────────────────────────────────────────────

/// Unified error type for the security crate.
#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error("authentication error: {0}")]
    AuthError(String),
    #[error("authorization error: {0}")]
    AuthzError(String),
    #[error("TLS error: {0}")]
    TlsError(String),
    #[error("configuration error: {0}")]
    ConfigError(String),
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        assert!(true);
    }
}
