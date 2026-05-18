// Licensed under Apache License, Version 2.0.

//! Network authorizer — DC-based access control.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.INetworkAuthorizer`
//! - `org.apache.cassandra.auth.CassandraNetworkAuthorizer`

use std::collections::HashSet;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::SecurityError;

/// DC-level permissions for a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DCPermissions {
    /// If true, the role can access all DCs.
    pub all_access: bool,
    /// Specific DCs the role is allowed to access.
    pub allowed_dcs: HashSet<String>,
}

impl DCPermissions {
    pub fn allow_all() -> Self {
        Self {
            all_access: true,
            allowed_dcs: HashSet::new(),
        }
    }

    pub fn restricted(dcs: HashSet<String>) -> Self {
        Self {
            all_access: false,
            allowed_dcs: dcs,
        }
    }

    pub fn can_access(&self, dc: &str) -> bool {
        self.all_access || self.allowed_dcs.contains(dc)
    }
}

/// Network-level authorization: controls which DCs a role can access.
pub trait NetworkAuthorizer: Send + Sync {
    /// Check if `role` can access datacenter `dc`.
    fn authorize(&self, role: &str, dc: &str) -> Result<(), SecurityError>;

    /// Set DC permissions for a role.
    fn set_permissions(&self, role: &str, permissions: DCPermissions);

    /// Get DC permissions for a role.
    fn get_permissions(&self, role: &str) -> DCPermissions;

    /// Drop permissions for a role.
    fn drop(&self, role: &str);

    fn name(&self) -> &str;
}

/// Allows all network access (default).
pub struct AllowAllNetworkAuthorizer;

impl NetworkAuthorizer for AllowAllNetworkAuthorizer {
    fn authorize(&self, _role: &str, _dc: &str) -> Result<(), SecurityError> {
        Ok(())
    }

    fn set_permissions(&self, _role: &str, _permissions: DCPermissions) {}

    fn get_permissions(&self, _role: &str) -> DCPermissions {
        DCPermissions::allow_all()
    }

    fn drop(&self, _role: &str) {}

    fn name(&self) -> &str {
        "AllowAllNetworkAuthorizer"
    }
}

/// DC-based network authorizer backed by DashMap.
///
/// Matches `org.apache.cassandra.auth.CassandraNetworkAuthorizer`.
pub struct CassandraNetworkAuthorizer {
    permissions: DashMap<String, DCPermissions>,
}

impl CassandraNetworkAuthorizer {
    pub fn new() -> Self {
        Self {
            permissions: DashMap::new(),
        }
    }
}

impl Default for CassandraNetworkAuthorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkAuthorizer for CassandraNetworkAuthorizer {
    fn authorize(&self, role: &str, dc: &str) -> Result<(), SecurityError> {
        match self.permissions.get(role) {
            Some(perms) => {
                if perms.can_access(dc) {
                    Ok(())
                } else {
                    Err(SecurityError::AuthzError(format!(
                        "role '{}' is not authorized to access DC '{}'",
                        role, dc
                    )))
                }
            }
            None => Ok(()), // No restrictions configured = allow all
        }
    }

    fn set_permissions(&self, role: &str, permissions: DCPermissions) {
        self.permissions.insert(role.to_string(), permissions);
    }

    fn get_permissions(&self, role: &str) -> DCPermissions {
        self.permissions
            .get(role)
            .map(|p| p.clone())
            .unwrap_or_else(DCPermissions::allow_all)
    }

    fn drop(&self, role: &str) {
        self.permissions.remove(role);
    }

    fn name(&self) -> &str {
        "CassandraNetworkAuthorizer"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_all_permits_everything() {
        let authz = AllowAllNetworkAuthorizer;
        assert!(authz.authorize("any_role", "dc1").is_ok());
    }

    #[test]
    fn restricted_dc_access() {
        let authz = CassandraNetworkAuthorizer::new();
        let mut dcs = HashSet::new();
        dcs.insert("dc1".to_string());
        authz.set_permissions("user1", DCPermissions::restricted(dcs));

        assert!(authz.authorize("user1", "dc1").is_ok());
        assert!(authz.authorize("user1", "dc2").is_err());
    }

    #[test]
    fn all_access_flag() {
        let authz = CassandraNetworkAuthorizer::new();
        authz.set_permissions("admin", DCPermissions::allow_all());
        assert!(authz.authorize("admin", "any_dc").is_ok());
    }

    #[test]
    fn no_config_allows_all() {
        let authz = CassandraNetworkAuthorizer::new();
        assert!(authz.authorize("unconfigured", "dc1").is_ok());
    }

    #[test]
    fn drop_permissions() {
        let authz = CassandraNetworkAuthorizer::new();
        let mut dcs = HashSet::new();
        dcs.insert("dc1".to_string());
        authz.set_permissions("user1", DCPermissions::restricted(dcs));
        authz.drop("user1");
        assert!(authz.authorize("user1", "dc2").is_ok());
    }
}
