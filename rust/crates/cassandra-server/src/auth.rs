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
const PERMISSIONS_TABLE: &str = "role_permissions";

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
        };

        self.engine.apply_mutation(&mutation)
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
        };

        self.engine.apply_mutation(&mutation)
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
                    "is_superuser" => is_superuser = val.get(0).copied() == Some(1),
                    "can_login" => can_login = val.get(0).copied() == Some(1),
                    "salted_hash" => {
                        hashed_password = String::from_utf8(val.clone()).ok();
                    }
                    _ => {}
                }
            }
        }

        Some(Role {
            name: name.to_string(),
            is_superuser,
            can_login,
            hashed_password,
            member_of: vec![], // member_of requires querying `role_members`
            network_permissions: None,
        })
    }

    fn list_roles(&self) -> Vec<Role> {
        // Not fully supported by typical partition read unless we iterate SSTables.
        // For now, return what we can or a stub.
        // To do this properly requires an index or full table scan.
        vec![]
    }

    fn grant_role(&self, _role: &str, _grantee: &str) -> Result<(), SecurityError> {
        Ok(()) // stub
    }

    fn revoke_role(&self, _role: &str, _grantee: &str) -> Result<(), SecurityError> {
        Ok(()) // stub
    }

    fn get_all_roles(&self, name: &str) -> Vec<String> {
        vec![name.to_string()] // stub
    }

    fn role_exists(&self, name: &str) -> bool {
        self.get_role(name).is_some()
    }
}

// ─── SystemAuthAuthorizer ───────────────────────────────────────────────────

pub struct SystemAuthAuthorizer {
    engine: Arc<StorageEngine>,
    role_manager: Arc<dyn RoleManager>,
}

impl SystemAuthAuthorizer {
    pub fn new(engine: Arc<StorageEngine>, role_manager: Arc<dyn RoleManager>) -> Self {
        Self { engine, role_manager }
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
        
        // TODO: check `role_permissions` table (stub)
        Err(SecurityError::AuthzError(format!(
            "User '{}' has no {} permission on {}",
            user, permission, resource
        )))
    }

    fn grant(
        &self,
        _grantor: &str,
        _grantee: &str,
        _resource: &Resource,
        _permission: Permission,
    ) -> Result<(), SecurityError> {
        Ok(()) // stub
    }

    fn revoke(
        &self,
        _revoker: &str,
        _revokee: &str,
        _resource: &Resource,
        _permission: Permission,
    ) -> Result<(), SecurityError> {
        Ok(()) // stub
    }

    fn list_permissions(
        &self,
        _role: &str,
        _resource: &Resource,
    ) -> Vec<(Permission, Resource)> {
        vec![]
    }

    fn require_authorization(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "SystemAuthAuthorizer"
    }
}
