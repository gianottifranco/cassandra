// Licensed under Apache License, Version 2.0.

//! Identity-to-role mapping for mTLS authentication.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator`
//! - `system_auth.identity_to_roles`

use dashmap::DashMap;

/// Maps certificate identities (CN, SPIFFE URI, etc.) to Cassandra roles.
pub trait IdentityRoleMapper: Send + Sync {
    /// Get the role name for a given identity.
    fn get_role_for_identity(&self, identity: &str) -> Option<String>;

    /// Set a mapping from identity to role.
    fn set_mapping(&self, identity: &str, role: &str);

    /// Remove a mapping.
    fn remove_mapping(&self, identity: &str);

    /// List all mappings as (identity, role) pairs.
    fn list_mappings(&self) -> Vec<(String, String)>;
}

/// In-memory identity-to-role mapper backed by DashMap.
pub struct InMemoryIdentityRoleMapper {
    mappings: DashMap<String, String>,
}

impl InMemoryIdentityRoleMapper {
    pub fn new() -> Self {
        Self {
            mappings: DashMap::new(),
        }
    }
}

impl Default for InMemoryIdentityRoleMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentityRoleMapper for InMemoryIdentityRoleMapper {
    fn get_role_for_identity(&self, identity: &str) -> Option<String> {
        self.mappings.get(identity).map(|v| v.clone())
    }

    fn set_mapping(&self, identity: &str, role: &str) {
        self.mappings.insert(identity.to_string(), role.to_string());
    }

    fn remove_mapping(&self, identity: &str) {
        self.mappings.remove(identity);
    }

    fn list_mappings(&self) -> Vec<(String, String)> {
        self.mappings
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_mapping() {
        let mapper = InMemoryIdentityRoleMapper::new();
        mapper.set_mapping("spiffe://example/service-a", "service_role");
        assert_eq!(
            mapper.get_role_for_identity("spiffe://example/service-a"),
            Some("service_role".to_string())
        );
    }

    #[test]
    fn remove_mapping() {
        let mapper = InMemoryIdentityRoleMapper::new();
        mapper.set_mapping("cn=admin", "admin_role");
        mapper.remove_mapping("cn=admin");
        assert!(mapper.get_role_for_identity("cn=admin").is_none());
    }

    #[test]
    fn list_mappings() {
        let mapper = InMemoryIdentityRoleMapper::new();
        mapper.set_mapping("a", "role_a");
        mapper.set_mapping("b", "role_b");
        let list = mapper.list_mappings();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn unknown_identity_returns_none() {
        let mapper = InMemoryIdentityRoleMapper::new();
        assert!(mapper.get_role_for_identity("unknown").is_none());
    }
}
