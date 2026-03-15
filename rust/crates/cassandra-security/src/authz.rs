// Licensed under Apache License, Version 2.0.

//! Authorization subsystem.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.IAuthorizer`
//! - `org.apache.cassandra.auth.CassandraAuthorizer`
//! - `org.apache.cassandra.auth.Permission`
//! - `org.apache.cassandra.auth.Resources`

use std::fmt;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::roles::RoleManager;
use crate::SecurityError;

// ─── Permission ────────────────────────────────────────────────────────────

/// CQL permissions matching Java's `org.apache.cassandra.auth.Permission`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    Create,
    Alter,
    Drop,
    Select,
    Modify,
    Authorize,
    Describe,
    Execute,
    Unmask,
    SelectMasked,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create => write!(f, "CREATE"),
            Self::Alter => write!(f, "ALTER"),
            Self::Drop => write!(f, "DROP"),
            Self::Select => write!(f, "SELECT"),
            Self::Modify => write!(f, "MODIFY"),
            Self::Authorize => write!(f, "AUTHORIZE"),
            Self::Describe => write!(f, "DESCRIBE"),
            Self::Execute => write!(f, "EXECUTE"),
            Self::Unmask => write!(f, "UNMASK"),
            Self::SelectMasked => write!(f, "SELECT_MASKED"),
        }
    }
}

impl Permission {
    /// All permissions.
    pub fn all() -> &'static [Permission] {
        &[
            Permission::Create,
            Permission::Alter,
            Permission::Drop,
            Permission::Select,
            Permission::Modify,
            Permission::Authorize,
            Permission::Describe,
            Permission::Execute,
            Permission::Unmask,
            Permission::SelectMasked,
        ]
    }
}

// ─── Resource ──────────────────────────────────────────────────────────────

/// A resource that can be protected by permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Resource {
    /// Root of all resources — `ALL`.
    Root,
    /// A keyspace — `ALL KEYSPACES` or `KEYSPACE ks`.
    Keyspace(String),
    /// A table — `TABLE ks.table`.
    Table { keyspace: String, table: String },
    /// A function — `FUNCTION ks.fn(args)`.
    Function { keyspace: String, name: String },
    /// A role — `ROLE name`.
    Role(String),
    /// JMX/admin resource.
    Jmx(String),
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => write!(f, "ALL"),
            Self::Keyspace(ks) => write!(f, "KEYSPACE {}", ks),
            Self::Table { keyspace, table } => write!(f, "TABLE {}.{}", keyspace, table),
            Self::Function { keyspace, name } => write!(f, "FUNCTION {}.{}", keyspace, name),
            Self::Role(name) => write!(f, "ROLE {}", name),
            Self::Jmx(name) => write!(f, "JMX/{}", name),
        }
    }
}

impl Resource {
    /// Get the parent resource for hierarchical permission checks.
    pub fn parent(&self) -> Option<Resource> {
        match self {
            Resource::Root => None,
            Resource::Keyspace(_) => Some(Resource::Root),
            Resource::Table { keyspace, .. } => Some(Resource::Keyspace(keyspace.clone())),
            Resource::Function { keyspace, .. } => Some(Resource::Keyspace(keyspace.clone())),
            Resource::Role(_) => Some(Resource::Root),
            Resource::Jmx(_) => Some(Resource::Root),
        }
    }
}

// ─── Authorizer Trait ──────────────────────────────────────────────────────

/// Pluggable authorization interface.
pub trait Authorizer: Send + Sync {
    /// Check if `user` has `permission` on `resource`.
    fn authorize(
        &self,
        user: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError>;

    /// Grant permission on resource to role.
    fn grant(
        &self,
        grantor: &str,
        grantee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError>;

    /// Revoke permission on resource from role.
    fn revoke(
        &self,
        revoker: &str,
        revokee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError>;

    /// List permissions for a role on a resource.
    fn list_permissions(
        &self,
        role: &str,
        resource: &Resource,
    ) -> Vec<(Permission, Resource)>;

    /// Whether authorization is required.
    fn require_authorization(&self) -> bool;

    fn name(&self) -> &str;
}

// ─── AllowAllAuthorizer ────────────────────────────────────────────────────

/// Authorizer that permits all operations. Default when authorization is disabled.
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(
        &self,
        _user: &str,
        _resource: &Resource,
        _permission: Permission,
    ) -> Result<(), SecurityError> {
        Ok(())
    }

    fn grant(
        &self,
        _grantor: &str,
        _grantee: &str,
        _resource: &Resource,
        _permission: Permission,
    ) -> Result<(), SecurityError> {
        Ok(())
    }

    fn revoke(
        &self,
        _revoker: &str,
        _revokee: &str,
        _resource: &Resource,
        _permission: Permission,
    ) -> Result<(), SecurityError> {
        Ok(())
    }

    fn list_permissions(
        &self,
        _role: &str,
        _resource: &Resource,
    ) -> Vec<(Permission, Resource)> {
        Permission::all()
            .iter()
            .map(|&p| (p, Resource::Root))
            .collect()
    }

    fn require_authorization(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "AllowAllAuthorizer"
    }
}

// ─── CassandraAuthorizer ──────────────────────────────────────────────────

/// Permission grant key: (role, resource).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GrantKey {
    role: String,
    resource: Resource,
}

/// Role-based authorizer matching `org.apache.cassandra.auth.CassandraAuthorizer`.
///
/// Checks permissions with role inheritance: if user is a member of a role
/// (directly or transitively), they inherit that role's permissions.
///
/// TODO(production): Persist permissions to system_auth.role_permissions.
pub struct CassandraAuthorizer<R: RoleManager> {
    /// Map of (role, resource) → set of granted permissions.
    grants: DashMap<GrantKey, Vec<Permission>>,
    role_manager: R,
}

impl<R: RoleManager> CassandraAuthorizer<R> {
    pub fn new(role_manager: R) -> Self {
        Self {
            grants: DashMap::new(),
            role_manager,
        }
    }
}

impl<R: RoleManager + Send + Sync> Authorizer for CassandraAuthorizer<R> {
    fn authorize(
        &self,
        user: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        // Superusers bypass authorization
        if let Some(role) = self.role_manager.get_role(user) {
            if role.is_superuser {
                return Ok(());
            }
        }

        // Check all roles the user belongs to (transitively)
        let all_roles = self.role_manager.get_all_roles(user);

        // Check permission on this resource and all parent resources
        let mut current = Some(resource.clone());
        while let Some(ref res) = current {
            for role_name in &all_roles {
                let key = GrantKey {
                    role: role_name.clone(),
                    resource: res.clone(),
                };
                if let Some(perms) = self.grants.get(&key) {
                    if perms.contains(&permission) {
                        return Ok(());
                    }
                }
            }
            current = res.parent();
        }

        Err(SecurityError::AuthzError(format!(
            "User '{}' has no {} permission on {}",
            user, permission, resource
        )))
    }

    fn grant(
        &self,
        _grantor: &str,
        grantee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        let key = GrantKey {
            role: grantee.to_string(),
            resource: resource.clone(),
        };
        let mut entry = self.grants.entry(key).or_insert_with(Vec::new);
        if !entry.contains(&permission) {
            entry.push(permission);
        }
        Ok(())
    }

    fn revoke(
        &self,
        _revoker: &str,
        revokee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        let key = GrantKey {
            role: revokee.to_string(),
            resource: resource.clone(),
        };
        if let Some(mut entry) = self.grants.get_mut(&key) {
            entry.retain(|p| *p != permission);
        }
        Ok(())
    }

    fn list_permissions(
        &self,
        role: &str,
        resource: &Resource,
    ) -> Vec<(Permission, Resource)> {
        let all_roles = self.role_manager.get_all_roles(role);
        let mut result = Vec::new();

        for entry in self.grants.iter() {
            if all_roles.contains(&entry.key().role) {
                // Check if the grant's resource matches or is a parent
                if entry.key().resource == *resource || *resource == Resource::Root {
                    for perm in entry.value() {
                        result.push((*perm, entry.key().resource.clone()));
                    }
                }
            }
        }
        result
    }

    fn require_authorization(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "CassandraAuthorizer"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::{InMemoryRoleManager, Role};

    fn setup() -> (InMemoryRoleManager, CassandraAuthorizer<InMemoryRoleManager>) {
        let mgr = InMemoryRoleManager::new();
        // Note: InMemoryRoleManager uses DashMap, can't clone easily.
        // For testing, we build two separate managers with same data.
        // In reality the authorizer borrows/shares the role manager.

        // Create a non-super user role
        // We need to work around the ownership issue.
        // Let's use a fresh manager dedicated to the authorizer.

        // Actually, let's just create a single one and test with known roles.
        // The default 'cassandra' superuser already exists.
        let mgr2 = InMemoryRoleManager::new();
        mgr2.create_role(Role {
            name: "reader".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });
        mgr2.create_role(Role {
            name: "writer".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        });

        let authz = CassandraAuthorizer::new(mgr2);
        (mgr, authz)
    }

    #[test]
    fn superuser_bypasses_auth() {
        let (_, authz) = setup();
        let result = authz.authorize(
            "cassandra",
            &Resource::Keyspace("test_ks".into()),
            Permission::Select,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn unauthorized_access_denied() {
        let (_, authz) = setup();
        let result = authz.authorize(
            "reader",
            &Resource::Keyspace("test_ks".into()),
            Permission::Select,
        );
        assert!(result.is_err());
    }

    #[test]
    fn grant_and_check() {
        let (_, authz) = setup();
        let resource = Resource::Keyspace("my_ks".into());
        authz
            .grant("cassandra", "reader", &resource, Permission::Select)
            .unwrap();

        assert!(authz
            .authorize("reader", &resource, Permission::Select)
            .is_ok());
        assert!(authz
            .authorize("reader", &resource, Permission::Modify)
            .is_err());
    }

    #[test]
    fn revoke_removes_permission() {
        let (_, authz) = setup();
        let resource = Resource::Table {
            keyspace: "ks".into(),
            table: "tbl".into(),
        };

        authz
            .grant("cassandra", "writer", &resource, Permission::Modify)
            .unwrap();
        assert!(authz
            .authorize("writer", &resource, Permission::Modify)
            .is_ok());

        authz
            .revoke("cassandra", "writer", &resource, Permission::Modify)
            .unwrap();
        assert!(authz
            .authorize("writer", &resource, Permission::Modify)
            .is_err());
    }

    #[test]
    fn hierarchical_permission_check() {
        let (_, authz) = setup();
        // Grant SELECT on ALL (Root)
        authz
            .grant("cassandra", "reader", &Resource::Root, Permission::Select)
            .unwrap();

        // Should have access to any keyspace/table through root grant
        assert!(authz
            .authorize(
                "reader",
                &Resource::Table {
                    keyspace: "any".into(),
                    table: "any".into()
                },
                Permission::Select
            )
            .is_ok());
    }

    #[test]
    fn allow_all_authorizer() {
        let authz = AllowAllAuthorizer;
        assert!(authz
            .authorize("anyone", &Resource::Root, Permission::Drop)
            .is_ok());
        assert!(!authz.require_authorization());
    }

    #[test]
    fn permission_display() {
        assert_eq!(format!("{}", Permission::Select), "SELECT");
        assert_eq!(format!("{}", Permission::Modify), "MODIFY");
    }

    #[test]
    fn resource_display() {
        assert_eq!(
            format!(
                "{}",
                Resource::Table {
                    keyspace: "ks".into(),
                    table: "t".into()
                }
            ),
            "TABLE ks.t"
        );
    }

    #[test]
    fn resource_parent_hierarchy() {
        let table = Resource::Table {
            keyspace: "ks".into(),
            table: "t".into(),
        };
        assert_eq!(table.parent(), Some(Resource::Keyspace("ks".into())));
        assert_eq!(
            Resource::Keyspace("ks".into()).parent(),
            Some(Resource::Root)
        );
        assert_eq!(Resource::Root.parent(), None);
    }
}
