// Licensed under Apache License, Version 2.0.

//! Role management subsystem.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.IRoleManager`
//! - `org.apache.cassandra.auth.CassandraRoleManager`
//! - `org.apache.cassandra.auth.RoleResource`

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::SecurityError;

// ─── Role ──────────────────────────────────────────────────────────────────

/// A Cassandra role (user/role/service account).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    /// bcrypt-hashed password. None if login is not allowed or password-less.
    pub hashed_password: Option<String>,
    /// Direct parent roles this role is a member of.
    pub member_of: Vec<String>,
    /// Optional network access restrictions.
    pub network_permissions: Option<NetworkPermissions>,
}

/// Network-level access restrictions for a role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkPermissions {
    /// Allowed CIDR ranges for client connections.
    pub allowed_cidrs: Vec<String>,
}

/// Options for creating or altering a role.
#[derive(Debug, Clone, Default)]
pub struct RoleOptions {
    pub is_superuser: Option<bool>,
    pub can_login: Option<bool>,
    pub password: Option<String>,
    pub network_permissions: Option<NetworkPermissions>,
}

// ─── RoleManager Trait ──────────────────────────────────────────────────────

/// Interface for managing roles (create, alter, drop, grant, revoke).
///
/// Matches `org.apache.cassandra.auth.IRoleManager`.
pub trait RoleManager: Send + Sync {
    /// Create a new role.
    fn create_role(&self, role: Role);

    /// Alter an existing role.
    fn alter_role(&self, name: &str, options: RoleOptions) -> Result<(), SecurityError>;

    /// Drop a role.
    fn drop_role(&self, name: &str) -> Result<(), SecurityError>;

    /// Get a role by name.
    fn get_role(&self, name: &str) -> Option<Role>;

    /// List all roles.
    fn list_roles(&self) -> Vec<Role>;

    /// Grant role `grantee` membership in role `role`.
    fn grant_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError>;

    /// Revoke role `grantee` membership from role `role`.
    fn revoke_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError>;

    /// Get all roles that `name` transitively belongs to (including itself).
    fn get_all_roles(&self, name: &str) -> Vec<String>;

    /// Check whether `name` exists.
    fn role_exists(&self, name: &str) -> bool;
}

impl<T: ?Sized + RoleManager> RoleManager for std::sync::Arc<T> {
    fn create_role(&self, role: Role) { (**self).create_role(role) }
    fn alter_role(&self, name: &str, options: RoleOptions) -> Result<(), SecurityError> { (**self).alter_role(name, options) }
    fn drop_role(&self, name: &str) -> Result<(), SecurityError> { (**self).drop_role(name) }
    fn get_role(&self, name: &str) -> Option<Role> { (**self).get_role(name) }
    fn list_roles(&self) -> Vec<Role> { (**self).list_roles() }
    fn grant_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> { (**self).grant_role(role, grantee) }
    fn revoke_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> { (**self).revoke_role(role, grantee) }
    fn get_all_roles(&self, name: &str) -> Vec<String> { (**self).get_all_roles(name) }
    fn role_exists(&self, name: &str) -> bool { (**self).role_exists(name) }
}

// ─── InMemoryRoleManager ──────────────────────────────────────────────────

/// In-memory role manager backed by DashMap.
///
/// Suitable for single-node and testing. For production multi-node,
/// roles should be persisted to system_auth keyspace tables.
///
/// TODO(production): Persist roles to storage via cassandra-storage crate.
pub struct InMemoryRoleManager {
    roles: DashMap<String, Role>,
}

impl InMemoryRoleManager {
    pub fn new() -> Self {
        let mgr = Self {
            roles: DashMap::new(),
        };
        // Always create the default cassandra superuser
        mgr.create_role(Role {
            name: "cassandra".into(),
            is_superuser: true,
            can_login: true,
            hashed_password: Some(
                bcrypt::hash("cassandra", 4).expect("bcrypt hash for default user"),
            ),
            member_of: vec![],
            network_permissions: None,
        });
        mgr
    }
}

impl Default for InMemoryRoleManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RoleManager for InMemoryRoleManager {
    fn create_role(&self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    fn alter_role(&self, name: &str, options: RoleOptions) -> Result<(), SecurityError> {
        let mut entry = self
            .roles
            .get_mut(name)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", name)))?;

        if let Some(su) = options.is_superuser {
            entry.is_superuser = su;
        }
        if let Some(login) = options.can_login {
            entry.can_login = login;
        }
        if let Some(ref password) = options.password {
            let hashed = crate::auth::hash_password(password)?;
            entry.hashed_password = Some(hashed);
        }
        if let Some(np) = options.network_permissions {
            entry.network_permissions = Some(np);
        }

        Ok(())
    }

    fn drop_role(&self, name: &str) -> Result<(), SecurityError> {
        self.roles
            .remove(name)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", name)))?;

        // Remove from member_of lists of other roles
        for mut entry in self.roles.iter_mut() {
            entry.value_mut().member_of.retain(|r| r != name);
        }

        Ok(())
    }

    fn get_role(&self, name: &str) -> Option<Role> {
        self.roles.get(name).map(|r| r.clone())
    }

    fn list_roles(&self) -> Vec<Role> {
        self.roles.iter().map(|r| r.value().clone()).collect()
    }

    fn grant_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        if !self.roles.contains_key(role) {
            return Err(SecurityError::AuthError(format!(
                "role '{}' doesn't exist",
                role
            )));
        }
        let mut entry = self
            .roles
            .get_mut(grantee)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", grantee)))?;

        if !entry.member_of.contains(&role.to_string()) {
            entry.member_of.push(role.to_string());
        }
        Ok(())
    }

    fn revoke_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        let mut entry = self
            .roles
            .get_mut(grantee)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", grantee)))?;

        entry.member_of.retain(|r| r != role);
        Ok(())
    }

    fn get_all_roles(&self, name: &str) -> Vec<String> {
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![name.to_string()];

        while let Some(current) = stack.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if let Some(role) = self.roles.get(&current) {
                for parent in &role.member_of {
                    if !visited.contains(parent) {
                        stack.push(parent.clone());
                    }
                }
            }
        }

        visited.into_iter().collect()
    }

    fn role_exists(&self, name: &str) -> bool {
        self.roles.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_superuser_exists() {
        let mgr = InMemoryRoleManager::new();
        let role = mgr.get_role("cassandra").unwrap();
        assert!(role.is_superuser);
        assert!(role.can_login);
    }

    #[test]
    fn create_and_get_role() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "reader".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        assert!(mgr.role_exists("reader"));
        let role = mgr.get_role("reader").unwrap();
        assert!(!role.is_superuser);
    }

    #[test]
    fn alter_role() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "worker".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        mgr.alter_role(
            "worker",
            RoleOptions {
                can_login: Some(true),
                is_superuser: Some(true),
                ..Default::default()
            },
        )
        .unwrap();

        let role = mgr.get_role("worker").unwrap();
        assert!(role.can_login);
        assert!(role.is_superuser);
    }

    #[test]
    fn drop_role() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "temp".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        assert!(mgr.role_exists("temp"));
        mgr.drop_role("temp").unwrap();
        assert!(!mgr.role_exists("temp"));
    }

    #[test]
    fn drop_nonexistent_role_fails() {
        let mgr = InMemoryRoleManager::new();
        assert!(mgr.drop_role("ghost").is_err());
    }

    #[test]
    fn grant_and_revoke() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "parent_role".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        mgr.create_role(Role {
            name: "child_role".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });

        mgr.grant_role("parent_role", "child_role").unwrap();
        let child = mgr.get_role("child_role").unwrap();
        assert!(child.member_of.contains(&"parent_role".to_string()));

        mgr.revoke_role("parent_role", "child_role").unwrap();
        let child = mgr.get_role("child_role").unwrap();
        assert!(!child.member_of.contains(&"parent_role".to_string()));
    }

    #[test]
    fn transitive_role_membership() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "top".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        mgr.create_role(Role {
            name: "mid".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec!["top".into()],
            network_permissions: None,
        });
        mgr.create_role(Role {
            name: "bottom".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec!["mid".into()],
            network_permissions: None,
        });

        let all = mgr.get_all_roles("bottom");
        assert!(all.contains(&"bottom".to_string()));
        assert!(all.contains(&"mid".to_string()));
        assert!(all.contains(&"top".to_string()));
    }

    #[test]
    fn list_roles() {
        let mgr = InMemoryRoleManager::new();
        mgr.create_role(Role {
            name: "extra".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        // Default 'cassandra' + 'extra'
        assert!(mgr.list_roles().len() >= 2);
    }
}
