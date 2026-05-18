// Licensed under Apache License, Version 2.0.

//! Persistent `system_auth`-style stores.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.CassandraRoleManager`
//! - `org.apache.cassandra.auth.CassandraAuthorizer`

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::{
    Authorizer, Permission, Resource, Role, RoleManager, RoleOptions, SecurityError,
    auth::hash_password,
    cidr::{CidrGroup, CidrGroupsManager},
    identity_mapping::IdentityRoleMapper,
    network_auth::{DCPermissions, NetworkAuthorizer},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthStoreData {
    #[serde(default)]
    roles: HashMap<String, Role>,
    #[serde(default)]
    grants: Vec<PermissionGrant>,
    #[serde(default)]
    identity_roles: HashMap<String, String>,
    #[serde(default)]
    cidr_groups: HashMap<String, CidrGroup>,
    #[serde(default)]
    network_permissions: HashMap<String, DCPermissions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionGrant {
    pub role: String,
    pub resource: String,
    pub permissions: Vec<Permission>,
}

pub struct PersistentAuthStore {
    path: PathBuf,
    data: RwLock<AuthStoreData>,
}

impl PersistentAuthStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SecurityError> {
        let path = path.into();
        let data = if path.exists() {
            let bytes = fs::read(&path).map_err(store_io_error)?;
            serde_json::from_slice(&bytes).map_err(store_json_error)?
        } else {
            AuthStoreData::default()
        };
        Ok(Self {
            path,
            data: RwLock::new(data),
        })
    }

    pub fn create_role(&self, role: Role) -> Result<(), SecurityError> {
        self.data
            .write()
            .map_err(lock_error)?
            .roles
            .insert(role.name.clone(), role);
        self.flush()
    }

    pub fn get_role(&self, name: &str) -> Result<Option<Role>, SecurityError> {
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .roles
            .get(name)
            .cloned())
    }

    pub fn list_roles(&self) -> Result<Vec<Role>, SecurityError> {
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .roles
            .values()
            .cloned()
            .collect())
    }

    pub fn grant(
        &self,
        role: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        let mut data = self.data.write().map_err(lock_error)?;
        let resource = resource.to_cql_string();
        match data
            .grants
            .iter_mut()
            .find(|grant| grant.role == role && grant.resource == resource)
        {
            Some(grant) => {
                if !grant.permissions.contains(&permission) {
                    grant.permissions.push(permission);
                    grant
                        .permissions
                        .sort_by_key(|permission| permission.to_string());
                }
            }
            None => data.grants.push(PermissionGrant {
                role: role.to_string(),
                resource,
                permissions: vec![permission],
            }),
        }
        drop(data);
        self.flush()
    }

    pub fn revoke(
        &self,
        role: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        let resource = resource.to_cql_string();
        let mut data = self.data.write().map_err(lock_error)?;
        for grant in data
            .grants
            .iter_mut()
            .filter(|grant| grant.role == role && grant.resource == resource)
        {
            grant
                .permissions
                .retain(|candidate| *candidate != permission);
        }
        data.grants.retain(|grant| !grant.permissions.is_empty());
        drop(data);
        self.flush()
    }

    pub fn list_permissions(
        &self,
        role: &str,
        resource: &Resource,
    ) -> Result<Vec<Permission>, SecurityError> {
        let resource = resource.to_cql_string();
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .grants
            .iter()
            .find(|grant| grant.role == role && grant.resource == resource)
            .map(|grant| grant.permissions.clone())
            .unwrap_or_default())
    }

    pub fn grants(&self) -> Result<Vec<PermissionGrant>, SecurityError> {
        Ok(self.data.read().map_err(lock_error)?.grants.clone())
    }

    pub fn set_identity_mapping(&self, identity: &str, role: &str) -> Result<(), SecurityError> {
        self.data
            .write()
            .map_err(lock_error)?
            .identity_roles
            .insert(identity.to_string(), role.to_string());
        self.flush()
    }

    pub fn get_identity_mapping(&self, identity: &str) -> Result<Option<String>, SecurityError> {
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .identity_roles
            .get(identity)
            .cloned())
    }

    pub fn remove_identity_mapping(&self, identity: &str) -> Result<(), SecurityError> {
        self.data
            .write()
            .map_err(lock_error)?
            .identity_roles
            .remove(identity);
        self.flush()
    }

    pub fn list_identity_mappings(&self) -> Result<Vec<(String, String)>, SecurityError> {
        let mut mappings = self
            .data
            .read()
            .map_err(lock_error)?
            .identity_roles
            .iter()
            .map(|(identity, role)| (identity.clone(), role.clone()))
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(mappings)
    }

    pub fn create_cidr_group(&self, group: CidrGroup) -> Result<(), SecurityError> {
        validate_cidr_ranges(&group.ranges)?;
        let mut data = self.data.write().map_err(lock_error)?;
        if data.cidr_groups.contains_key(&group.name) {
            return Err(SecurityError::AuthError(format!(
                "CIDR group '{}' already exists",
                group.name
            )));
        }
        data.cidr_groups.insert(group.name.clone(), group);
        drop(data);
        self.flush()
    }

    pub fn get_cidr_group(&self, name: &str) -> Result<Option<CidrGroup>, SecurityError> {
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .cidr_groups
            .get(name)
            .cloned())
    }

    pub fn update_cidr_group(&self, name: &str, ranges: Vec<String>) -> Result<(), SecurityError> {
        validate_cidr_ranges(&ranges)?;
        let mut data = self.data.write().map_err(lock_error)?;
        let group = data
            .cidr_groups
            .get_mut(name)
            .ok_or_else(|| SecurityError::AuthError(format!("CIDR group '{}' not found", name)))?;
        group.ranges = ranges;
        drop(data);
        self.flush()
    }

    pub fn delete_cidr_group(&self, name: &str) -> Result<(), SecurityError> {
        let mut data = self.data.write().map_err(lock_error)?;
        data.cidr_groups
            .remove(name)
            .ok_or_else(|| SecurityError::AuthError(format!("CIDR group '{}' not found", name)))?;
        drop(data);
        self.flush()
    }

    pub fn list_cidr_groups(&self) -> Result<Vec<CidrGroup>, SecurityError> {
        let mut groups = self
            .data
            .read()
            .map_err(lock_error)?
            .cidr_groups
            .values()
            .cloned()
            .collect::<Vec<_>>();
        groups.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(groups)
    }

    pub fn set_network_permissions(
        &self,
        role: &str,
        permissions: DCPermissions,
    ) -> Result<(), SecurityError> {
        self.data
            .write()
            .map_err(lock_error)?
            .network_permissions
            .insert(role.to_string(), permissions);
        self.flush()
    }

    pub fn get_network_permissions(
        &self,
        role: &str,
    ) -> Result<Option<DCPermissions>, SecurityError> {
        Ok(self
            .data
            .read()
            .map_err(lock_error)?
            .network_permissions
            .get(role)
            .cloned())
    }

    pub fn drop_network_permissions(&self, role: &str) -> Result<(), SecurityError> {
        self.data
            .write()
            .map_err(lock_error)?
            .network_permissions
            .remove(role);
        self.flush()
    }

    pub fn flush(&self) -> Result<(), SecurityError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(store_io_error)?;
        }
        let bytes = serde_json::to_vec_pretty(&*self.data.read().map_err(lock_error)?)
            .map_err(store_json_error)?;
        fs::write(&self.path, bytes).map_err(store_io_error)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct PersistentCidrGroupsManager {
    store: Arc<PersistentAuthStore>,
}

impl PersistentCidrGroupsManager {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SecurityError> {
        Ok(Self {
            store: Arc::new(PersistentAuthStore::open(path)?),
        })
    }

    pub fn from_store(store: Arc<PersistentAuthStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &PersistentAuthStore {
        self.store.as_ref()
    }
}

impl CidrGroupsManager for PersistentCidrGroupsManager {
    fn create_group(&self, group: CidrGroup) -> Result<(), SecurityError> {
        self.store.create_cidr_group(group)
    }

    fn get_group(&self, name: &str) -> Option<CidrGroup> {
        self.store.get_cidr_group(name).ok().flatten()
    }

    fn update_group(&self, name: &str, ranges: Vec<String>) -> Result<(), SecurityError> {
        self.store.update_cidr_group(name, ranges)
    }

    fn delete_group(&self, name: &str) -> Result<(), SecurityError> {
        self.store.delete_cidr_group(name)
    }

    fn list_groups(&self) -> Vec<CidrGroup> {
        self.store.list_cidr_groups().unwrap_or_default()
    }
}

pub struct PersistentNetworkAuthorizer {
    store: Arc<PersistentAuthStore>,
}

impl PersistentNetworkAuthorizer {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SecurityError> {
        Ok(Self {
            store: Arc::new(PersistentAuthStore::open(path)?),
        })
    }

    pub fn from_store(store: Arc<PersistentAuthStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &PersistentAuthStore {
        self.store.as_ref()
    }
}

impl NetworkAuthorizer for PersistentNetworkAuthorizer {
    fn authorize(&self, role: &str, dc: &str) -> Result<(), SecurityError> {
        let permissions = self.get_permissions(role);
        if permissions.can_access(dc) {
            Ok(())
        } else {
            Err(SecurityError::AuthzError(format!(
                "role '{}' is not authorized to access DC '{}'",
                role, dc
            )))
        }
    }

    fn set_permissions(&self, role: &str, permissions: DCPermissions) {
        self.store
            .set_network_permissions(role, permissions)
            .expect("persistent network permissions update should succeed");
    }

    fn get_permissions(&self, role: &str) -> DCPermissions {
        self.store
            .get_network_permissions(role)
            .ok()
            .flatten()
            .unwrap_or_else(DCPermissions::allow_all)
    }

    fn drop(&self, role: &str) {
        self.store
            .drop_network_permissions(role)
            .expect("persistent network permissions drop should succeed");
    }

    fn name(&self) -> &str {
        "PersistentNetworkAuthorizer"
    }
}

pub struct PersistentIdentityRoleMapper {
    store: Arc<PersistentAuthStore>,
}

impl PersistentIdentityRoleMapper {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SecurityError> {
        Ok(Self {
            store: Arc::new(PersistentAuthStore::open(path)?),
        })
    }

    pub fn from_store(store: Arc<PersistentAuthStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &PersistentAuthStore {
        self.store.as_ref()
    }
}

impl IdentityRoleMapper for PersistentIdentityRoleMapper {
    fn get_role_for_identity(&self, identity: &str) -> Option<String> {
        self.store.get_identity_mapping(identity).ok().flatten()
    }

    fn set_mapping(&self, identity: &str, role: &str) {
        self.store
            .set_identity_mapping(identity, role)
            .expect("persistent identity mapping update should succeed");
    }

    fn remove_mapping(&self, identity: &str) {
        self.store
            .remove_identity_mapping(identity)
            .expect("persistent identity mapping removal should succeed");
    }

    fn list_mappings(&self) -> Vec<(String, String)> {
        self.store.list_identity_mappings().unwrap_or_default()
    }
}

pub struct PersistentRoleManager {
    store: Arc<PersistentAuthStore>,
}

impl PersistentRoleManager {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, SecurityError> {
        Ok(Self {
            store: Arc::new(PersistentAuthStore::open(path)?),
        })
    }

    pub fn from_store(store: Arc<PersistentAuthStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &PersistentAuthStore {
        self.store.as_ref()
    }

    pub fn shared_store(&self) -> Arc<PersistentAuthStore> {
        Arc::clone(&self.store)
    }
}

impl RoleManager for PersistentRoleManager {
    fn create_role(&self, role: Role) {
        self.store
            .create_role(role)
            .expect("persistent role create should succeed");
    }

    fn alter_role(&self, name: &str, options: RoleOptions) -> Result<(), SecurityError> {
        let mut role = self
            .store
            .get_role(name)?
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", name)))?;
        if let Some(is_superuser) = options.is_superuser {
            role.is_superuser = is_superuser;
        }
        if let Some(can_login) = options.can_login {
            role.can_login = can_login;
        }
        if let Some(password) = options.password {
            role.hashed_password = Some(hash_password(&password)?);
        }
        if let Some(network_permissions) = options.network_permissions {
            role.network_permissions = Some(network_permissions);
        }
        self.store.create_role(role)
    }

    fn drop_role(&self, name: &str) -> Result<(), SecurityError> {
        let mut data = self.store.data.write().map_err(lock_error)?;
        data.roles
            .remove(name)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", name)))?;
        for role in data.roles.values_mut() {
            role.member_of.retain(|parent| parent != name);
        }
        data.grants.retain(|grant| grant.role != name);
        drop(data);
        self.store.flush()
    }

    fn get_role(&self, name: &str) -> Option<Role> {
        self.store.get_role(name).ok().flatten()
    }

    fn list_roles(&self) -> Vec<Role> {
        self.store.list_roles().unwrap_or_default()
    }

    fn grant_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        let mut data = self.store.data.write().map_err(lock_error)?;
        if !data.roles.contains_key(role) {
            return Err(SecurityError::AuthError(format!(
                "role '{}' doesn't exist",
                role
            )));
        }
        let grantee_role = data
            .roles
            .get_mut(grantee)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", grantee)))?;
        if !grantee_role.member_of.iter().any(|parent| parent == role) {
            grantee_role.member_of.push(role.to_string());
        }
        drop(data);
        self.store.flush()
    }

    fn revoke_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        let mut data = self.store.data.write().map_err(lock_error)?;
        let grantee_role = data
            .roles
            .get_mut(grantee)
            .ok_or_else(|| SecurityError::AuthError(format!("role '{}' doesn't exist", grantee)))?;
        grantee_role.member_of.retain(|parent| parent != role);
        drop(data);
        self.store.flush()
    }

    fn get_all_roles(&self, name: &str) -> Vec<String> {
        let Ok(data) = self.store.data.read() else {
            return Vec::new();
        };
        let mut visited = HashSet::new();
        let mut stack = vec![name.to_string()];
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(role) = data.roles.get(&current) {
                for parent in &role.member_of {
                    stack.push(parent.clone());
                }
            }
        }
        visited.into_iter().collect()
    }

    fn role_exists(&self, name: &str) -> bool {
        self.store
            .data
            .read()
            .map(|data| data.roles.contains_key(name))
            .unwrap_or(false)
    }
}

pub struct PersistentAuthorizer {
    store: Arc<PersistentAuthStore>,
    role_manager: Arc<dyn RoleManager>,
}

impl PersistentAuthorizer {
    pub fn new(store: Arc<PersistentAuthStore>, role_manager: Arc<dyn RoleManager>) -> Self {
        Self {
            store,
            role_manager,
        }
    }

    fn permissions_for_role(&self, role: &str, resource: &Resource) -> Vec<Permission> {
        self.store
            .list_permissions(role, resource)
            .unwrap_or_default()
    }
}

impl Authorizer for PersistentAuthorizer {
    fn authorize(
        &self,
        user: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        if let Some(role) = self.role_manager.get_role(user) {
            if role.is_superuser {
                return Ok(());
            }
        }

        let all_roles = self.role_manager.get_all_roles(user);
        let mut current = Some(resource.clone());
        while let Some(ref candidate) = current {
            for role in &all_roles {
                if self
                    .permissions_for_role(role, candidate)
                    .contains(&permission)
                {
                    return Ok(());
                }
            }
            current = candidate.parent();
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
        self.store.grant(grantee, resource, permission)
    }

    fn revoke(
        &self,
        _revoker: &str,
        revokee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        self.store.revoke(revokee, resource, permission)
    }

    fn list_permissions(&self, role: &str, resource: &Resource) -> Vec<(Permission, Resource)> {
        let all_roles = self.role_manager.get_all_roles(role);
        let Ok(grants) = self.store.grants() else {
            return Vec::new();
        };

        let mut result = Vec::new();
        for grant in grants {
            if !all_roles.iter().any(|candidate| candidate == &grant.role) {
                continue;
            }
            let Some(stored_resource) = Resource::from_cql_string(&grant.resource) else {
                continue;
            };
            if *resource != Resource::Root && stored_resource != *resource {
                continue;
            }
            for permission in grant.permissions {
                result.push((permission, stored_resource.clone()));
            }
        }
        result
    }

    fn require_authorization(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "PersistentAuthorizer"
    }
}

fn store_io_error(err: std::io::Error) -> SecurityError {
    SecurityError::AuthzError(format!("system_auth store I/O error: {err}"))
}

fn store_json_error(err: serde_json::Error) -> SecurityError {
    SecurityError::AuthzError(format!("system_auth store JSON error: {err}"))
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> SecurityError {
    SecurityError::AuthzError("system_auth store lock poisoned".to_string())
}

fn validate_cidr_ranges(ranges: &[String]) -> Result<(), SecurityError> {
    for range in ranges {
        range.parse::<ipnet::IpNet>().map_err(|err| {
            SecurityError::AuthError(format!("invalid CIDR '{}': {}", range, err))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn role(name: &str) -> Role {
        Role {
            name: name.to_string(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: Vec::new(),
            network_permissions: None,
        }
    }

    #[test]
    fn persists_roles_and_grants() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_auth.json");
        let manager = PersistentRoleManager::open(&path).unwrap();
        manager.create_role(role("reader"));
        manager.create_role(role("analyst"));
        manager.grant_role("reader", "analyst").unwrap();
        manager
            .store()
            .grant(
                "reader",
                &Resource::Table {
                    keyspace: "ks".into(),
                    table: "tbl".into(),
                },
                Permission::Select,
            )
            .unwrap();

        let reopened = PersistentRoleManager::open(&path).unwrap();
        assert_eq!(
            reopened.get_role("analyst").unwrap().member_of,
            vec!["reader".to_string()]
        );
        assert_eq!(
            reopened
                .store()
                .list_permissions(
                    "reader",
                    &Resource::Table {
                        keyspace: "ks".into(),
                        table: "tbl".into()
                    }
                )
                .unwrap(),
            vec![Permission::Select]
        );
    }

    #[test]
    fn persistent_authorizer_enforces_reopened_grants_and_role_inheritance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_auth.json");
        let store = Arc::new(PersistentAuthStore::open(&path).unwrap());
        let manager = Arc::new(PersistentRoleManager::from_store(Arc::clone(&store)));
        manager.create_role(role("reader"));
        manager.create_role(role("analyst"));
        manager.grant_role("reader", "analyst").unwrap();

        let authz = PersistentAuthorizer::new(
            Arc::clone(&store),
            Arc::clone(&manager) as Arc<dyn RoleManager>,
        );
        let table = Resource::Table {
            keyspace: "ks".into(),
            table: "tbl".into(),
        };
        authz
            .grant("cassandra", "reader", &table, Permission::Select)
            .unwrap();
        assert!(
            authz
                .authorize("analyst", &table, Permission::Select)
                .is_ok()
        );

        let reopened_store = Arc::new(PersistentAuthStore::open(&path).unwrap());
        let reopened_manager = Arc::new(PersistentRoleManager::from_store(Arc::clone(
            &reopened_store,
        )));
        let reopened_authz = PersistentAuthorizer::new(
            Arc::clone(&reopened_store),
            Arc::clone(&reopened_manager) as Arc<dyn RoleManager>,
        );
        assert!(
            reopened_authz
                .authorize("analyst", &table, Permission::Select)
                .is_ok()
        );
        reopened_authz
            .revoke("cassandra", "reader", &table, Permission::Select)
            .unwrap();
        assert!(
            reopened_authz
                .authorize("analyst", &table, Permission::Select)
                .is_err()
        );
    }

    #[test]
    fn persists_identity_to_role_mappings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_auth.json");
        let mapper = PersistentIdentityRoleMapper::open(&path).unwrap();

        mapper.set_mapping("spiffe://example/service-a", "service_role");
        mapper.set_mapping("cn=admin", "admin_role");
        assert_eq!(
            mapper.get_role_for_identity("spiffe://example/service-a"),
            Some("service_role".to_string())
        );
        assert_eq!(
            mapper.list_mappings(),
            vec![
                ("cn=admin".to_string(), "admin_role".to_string()),
                (
                    "spiffe://example/service-a".to_string(),
                    "service_role".to_string()
                )
            ]
        );

        let reopened = PersistentIdentityRoleMapper::open(&path).unwrap();
        assert_eq!(
            reopened.get_role_for_identity("cn=admin"),
            Some("admin_role".to_string())
        );
        reopened.remove_mapping("cn=admin");

        let after_remove = PersistentIdentityRoleMapper::open(&path).unwrap();
        assert!(after_remove.get_role_for_identity("cn=admin").is_none());
        assert_eq!(
            after_remove.get_role_for_identity("spiffe://example/service-a"),
            Some("service_role".to_string())
        );
    }

    #[test]
    fn persists_cidr_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_auth.json");
        let manager = PersistentCidrGroupsManager::open(&path).unwrap();

        manager
            .create_group(CidrGroup {
                name: "office".to_string(),
                ranges: vec!["10.0.0.0/8".to_string()],
            })
            .unwrap();
        assert!(manager.get_group("office").is_some());
        assert!(
            manager
                .create_group(CidrGroup {
                    name: "office".to_string(),
                    ranges: vec!["192.168.0.0/16".to_string()],
                })
                .is_err()
        );
        assert!(
            manager
                .create_group(CidrGroup {
                    name: "bad".to_string(),
                    ranges: vec!["not-a-cidr".to_string()],
                })
                .is_err()
        );

        let reopened = PersistentCidrGroupsManager::open(&path).unwrap();
        assert_eq!(
            reopened.get_group("office").unwrap().ranges,
            vec!["10.0.0.0/8".to_string()]
        );
        reopened
            .update_group("office", vec!["172.16.0.0/12".to_string()])
            .unwrap();
        assert_eq!(
            reopened.list_groups()[0].ranges,
            vec!["172.16.0.0/12".to_string()]
        );
        reopened.delete_group("office").unwrap();

        let after_delete = PersistentCidrGroupsManager::open(&path).unwrap();
        assert!(after_delete.get_group("office").is_none());
    }

    #[test]
    fn persists_network_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system_auth.json");
        let authorizer = PersistentNetworkAuthorizer::open(&path).unwrap();
        let allowed_dcs = HashSet::from(["dc1".to_string()]);

        authorizer.set_permissions("analyst", DCPermissions::restricted(allowed_dcs));
        assert!(authorizer.authorize("analyst", "dc1").is_ok());
        assert!(authorizer.authorize("analyst", "dc2").is_err());

        let reopened = PersistentNetworkAuthorizer::open(&path).unwrap();
        assert!(reopened.authorize("analyst", "dc1").is_ok());
        assert!(reopened.authorize("analyst", "dc2").is_err());
        reopened.drop("analyst");

        let after_drop = PersistentNetworkAuthorizer::open(&path).unwrap();
        assert!(after_drop.authorize("analyst", "dc2").is_ok());
    }
}
