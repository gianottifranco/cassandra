// Licensed under Apache License, Version 2.0.

//! Durable Role and Authorization managers backed by the `system_auth` keyspace.

use std::sync::Arc;

use cassandra_security::{
    Authorizer, Permission, Resource, Role, RoleManager, RoleOptions, SecurityError,
};
use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow};
use cassandra_storage::engine::StorageEngine;

const SYSTEM_AUTH: &str = "system_auth";
const ROLES_TABLE: &str = "roles";
const ROLE_MEMBERS_TABLE: &str = "role_members";
const PERMISSIONS_TABLE: &str = "role_permissions";
const RESOURCE_ROLE_INDEX_TABLE: &str = "resource_role_index";

// ─── SystemAuthRoleManager ──────────────────────────────────────────────────

/// Durable RoleManager backed by `system_auth.roles`.
pub struct SystemAuthRoleManager {
    engine: Arc<StorageEngine>,
}

impl SystemAuthRoleManager {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        let mgr = Self { engine };
        // Ensure default cassandra user exists
        if !mgr.role_exists("cassandra") {
            mgr.create_role(Role {
                name: "cassandra".into(),
                is_superuser: true,
                can_login: true,
                hashed_password: Some(
                    cassandra_security::auth::hash_password("cassandra")
                        .expect("hash default cassandra password"),
                ),
                member_of: vec![],
                network_permissions: None,
            });
        }
        mgr
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    /// Read member_of for a role by scanning role_members where this role is a member.
    fn read_member_of(&self, grantee: &str) -> Vec<String> {
        // role_members is keyed by parent role with clustering on member.
        // To find which roles a grantee belongs to, we need to scan.
        // For now, read from the roles table's member_of set cells.
        let pk = grantee.as_bytes().to_vec();
        let partition = match self.engine.read_partition(SYSTEM_AUTH, ROLES_TABLE, &pk) {
            Some(p) => p,
            None => return vec![],
        };

        let now_secs = (Self::now() / 1_000_000) as i32;
        let live = partition.live_rows(now_secs);
        if live.is_empty() {
            return vec![];
        }

        let mut member_of = Vec::new();
        for cell in &live[0].cells {
            if !cell.is_live_at(now_secs) {
                continue;
            }
            // Set elements stored as "member_of:role_name"
            if let Some(role_name) = cell.column.strip_prefix("member_of:") {
                member_of.push(role_name.to_string());
            }
        }
        member_of
    }
}

impl RoleManager for SystemAuthRoleManager {
    fn create_role(&self, role: Role) {
        let mut cells = Vec::new();

        cells.push(CellMutation {
            column: "is_superuser".into(),
            value: Some(vec![if role.is_superuser { 1 } else { 0 }]),
            timestamp: Self::now(),
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        });

        cells.push(CellMutation {
            column: "can_login".into(),
            value: Some(vec![if role.can_login { 1 } else { 0 }]),
            timestamp: Self::now(),
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        });

        if let Some(ref pw) = role.hashed_password {
            cells.push(CellMutation {
                column: "salted_hash".into(),
                value: Some(pw.as_bytes().to_vec()),
                timestamp: Self::now(),
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            });
        }

        // We skip member_of for now (implemented in separate table if needed)

        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLES_TABLE.into(),
            partition_key: role.name.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells,
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: Self::now(),
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        if let Err(e) = self.engine.apply_mutation(&mutation) {
            tracing::error!("Failed to create role {}: {}", role.name, e);
        }
    }

    fn alter_role(&self, name: &str, options: RoleOptions) -> Result<(), SecurityError> {
        let mut cells = Vec::new();

        if let Some(su) = options.is_superuser {
            cells.push(CellMutation {
                column: "is_superuser".into(),
                value: Some(vec![if su { 1 } else { 0 }]),
                timestamp: Self::now(),
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            });
        }

        if let Some(login) = options.can_login {
            cells.push(CellMutation {
                column: "can_login".into(),
                value: Some(vec![if login { 1 } else { 0 }]),
                timestamp: Self::now(),
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            });
        }

        if let Some(pw) = options.password {
            let hashed = cassandra_security::auth::hash_password(&pw)?;
            cells.push(CellMutation {
                column: "salted_hash".into(),
                value: Some(hashed.as_bytes().to_vec()),
                timestamp: Self::now(),
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            });
        }

        if cells.is_empty() {
            return Ok(());
        }

        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLES_TABLE.into(),
            partition_key: name.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells,
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: Self::now(),
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn drop_role(&self, name: &str) -> Result<(), SecurityError> {
        let now = Self::now();
        let now_secs = (now / 1_000_000) as i32;

        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLES_TABLE.into(),
            partition_key: name.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![],
                is_tombstone: true,
                local_deletion_time: Some(now_secs),
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn get_role(&self, name: &str) -> Option<Role> {
        let pk = name.as_bytes().to_vec();
        let partition = self.engine.read_partition(SYSTEM_AUTH, ROLES_TABLE, &pk)?;

        let now_secs = (Self::now() / 1_000_000) as i32;
        let live = partition.live_rows(now_secs);
        if live.is_empty() {
            return None;
        }

        let row = live[0];
        let mut is_superuser = false;
        let mut can_login = false;
        let mut hashed_password = None;

        for cell in &row.cells {
            if !cell.is_live_at(now_secs) {
                continue;
            }
            if let Some(ref val) = cell.value {
                match cell.column.as_str() {
                    "is_superuser" => is_superuser = val.first().copied() == Some(1),
                    "can_login" => can_login = val.first().copied() == Some(1),
                    "salted_hash" => {
                        hashed_password = String::from_utf8(val.clone()).ok();
                    }
                    _ => {}
                }
            }
        }

        let member_of = self.read_member_of(name);

        Some(Role {
            name: name.to_string(),
            is_superuser,
            can_login,
            hashed_password,
            member_of,
            network_permissions: None,
        })
    }

    fn list_roles(&self) -> Vec<Role> {
        // Full table scan is not supported by StorageEngine's current API.
        // Roles must be looked up by name. Return known roles by scanning
        // the in-memory index if available, or return empty.
        // GAP(gap_guard_paging): Add scan_partitions to StorageEngine for full table iteration — tracked in gap_guards.rs
        tracing::warn!("list_roles: full table scan not yet supported by storage engine");
        vec![]
    }

    fn grant_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        // Write to role_members: partition=role, clustering=grantee
        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLE_MEMBERS_TABLE.into(),
            partition_key: role.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: grantee.as_bytes().to_vec(),
                cells: vec![],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: Self::now(),
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))?;

        // Also update the roles table member_of set (for get_role)
        let mutation2 = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLES_TABLE.into(),
            partition_key: grantee.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: format!("member_of:{}", role), // set element
                    value: Some(vec![]),
                    timestamp: Self::now(),
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: Self::now(),
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation2)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn revoke_role(&self, role: &str, grantee: &str) -> Result<(), SecurityError> {
        let now = Self::now();
        let now_secs = (now / 1_000_000) as i32;

        // Tombstone the role_members row
        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLE_MEMBERS_TABLE.into(),
            partition_key: role.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: grantee.as_bytes().to_vec(),
                cells: vec![],
                is_tombstone: true,
                local_deletion_time: Some(now_secs),
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))?;

        // Remove from roles table member_of set
        let mutation2 = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: ROLES_TABLE.into(),
            partition_key: grantee.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: format!("member_of:{}", role),
                    value: None,
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: Some(now_secs),
                    is_tombstone: true,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation2)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn get_all_roles(&self, name: &str) -> Vec<String> {
        // BFS traversal of role membership
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![name.to_string()];

        while let Some(current) = stack.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            let member_of = self.read_member_of(&current);
            for parent in member_of {
                if !visited.contains(&parent) {
                    stack.push(parent);
                }
            }
        }

        visited.into_iter().collect()
    }

    fn role_exists(&self, name: &str) -> bool {
        self.get_role(name).is_some()
    }
}

// ─── SystemAuthAuthorizer ───────────────────────────────────────────────────

pub struct SystemAuthAuthorizer {
    #[allow(dead_code)]
    engine: Arc<StorageEngine>,
    role_manager: Arc<dyn RoleManager>,
}

impl SystemAuthAuthorizer {
    pub fn new(engine: Arc<StorageEngine>, role_manager: Arc<dyn RoleManager>) -> Self {
        Self {
            engine,
            role_manager,
        }
    }
}

impl SystemAuthAuthorizer {
    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    /// Read permissions for a role on a specific resource from storage.
    fn read_permissions(&self, role: &str, resource: &Resource) -> Vec<Permission> {
        let pk = role.as_bytes().to_vec();
        let partition = match self.engine.read_partition(SYSTEM_AUTH, PERMISSIONS_TABLE, &pk) {
            Some(p) => p,
            None => return vec![],
        };

        let now_secs = (Self::now() / 1_000_000) as i32;
        let resource_str = resource.to_cql_string();
        let mut perms = Vec::new();

        for row in partition.live_rows(now_secs) {
            // Clustering key is the resource string
            if let Ok(stored_resource) = std::str::from_utf8(&row.clustering_key) {
                if stored_resource == resource_str {
                    // Read permission set elements from cells
                    for cell in &row.cells {
                        if !cell.is_live_at(now_secs) {
                            continue;
                        }
                        if let Some(perm_name) = cell.column.strip_prefix("permissions:") {
                            if let Ok(p) = perm_name.parse::<Permission>() {
                                perms.push(p);
                            }
                        }
                    }
                }
            }
        }

        perms
    }
}

impl Authorizer for SystemAuthAuthorizer {
    fn authorize(
        &self,
        user: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        // Superusers bypass
        if let Some(role) = self.role_manager.get_role(user) {
            if role.is_superuser {
                return Ok(());
            }
        }

        // Check permissions for all transitive roles
        let all_roles = self.role_manager.get_all_roles(user);

        // Check permission on this resource and all parent resources
        let mut current = Some(resource.clone());
        while let Some(ref res) = current {
            for role_name in &all_roles {
                let perms = self.read_permissions(role_name, res);
                if perms.contains(&permission) {
                    return Ok(());
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
        let now = Self::now();
        let resource_str = resource.to_cql_string();

        // Write to role_permissions: pk=role, ck=resource, set element=permission
        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: PERMISSIONS_TABLE.into(),
            partition_key: grantee.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: resource_str.as_bytes().to_vec(),
                cells: vec![CellMutation {
                    column: format!("permissions:{}", permission),
                    value: Some(vec![]),
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))?;

        // Write to resource_role_index: pk=resource, ck=role
        let idx_mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: RESOURCE_ROLE_INDEX_TABLE.into(),
            partition_key: resource_str.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: grantee.as_bytes().to_vec(),
                cells: vec![],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&idx_mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn revoke(
        &self,
        _revoker: &str,
        revokee: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        let now = Self::now();
        let now_secs = (now / 1_000_000) as i32;
        let resource_str = resource.to_cql_string();

        // Tombstone the permission set element
        let mutation = Mutation {
            keyspace: SYSTEM_AUTH.into(),
            table: PERMISSIONS_TABLE.into(),
            partition_key: revokee.as_bytes().to_vec(),
            rows: vec![MutationRow {
                clustering_key: resource_str.as_bytes().to_vec(),
                cells: vec![CellMutation {
                    column: format!("permissions:{}", permission),
                    value: None,
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: Some(now_secs),
                    is_tombstone: true,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: now,
            cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| SecurityError::AuthError(format!("storage error: {}", e)))
    }

    fn list_permissions(&self, role: &str, resource: &Resource) -> Vec<(Permission, Resource)> {
        let all_roles = self.role_manager.get_all_roles(role);
        let mut result = Vec::new();

        for role_name in &all_roles {
            let pk = role_name.as_bytes().to_vec();
            let partition = match self.engine.read_partition(SYSTEM_AUTH, PERMISSIONS_TABLE, &pk) {
                Some(p) => p,
                None => continue,
            };

            let now_secs = (Self::now() / 1_000_000) as i32;
            for row in partition.live_rows(now_secs) {
                if let Ok(stored_resource_str) = std::str::from_utf8(&row.clustering_key) {
                    if let Some(stored_resource) = Resource::from_cql_string(stored_resource_str) {
                        // Match if resource is Root (list all) or matches
                        if *resource == Resource::Root || stored_resource == *resource {
                            for cell in &row.cells {
                                if !cell.is_live_at(now_secs) {
                                    continue;
                                }
                                if let Some(perm_name) = cell.column.strip_prefix("permissions:") {
                                    if let Ok(p) = perm_name.parse::<Permission>() {
                                        result.push((p, stored_resource.clone()));
                                    }
                                }
                            }
                        }
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
        "SystemAuthAuthorizer"
    }
}
