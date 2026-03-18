// Licensed under Apache License, Version 2.0.

//! Permissions cache — caches (role, resource) → permissions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.PermissionsCache`

use std::sync::Arc;

use crate::authz::{Permission, Resource};
use crate::cache::{AuthCache, AuthCacheConfig};

/// Key for the permissions cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermissionsCacheKey {
    pub role: String,
    pub resource: Resource,
}

/// Cache of permission lookups to avoid repeated storage reads.
pub struct PermissionsCache {
    cache: Arc<AuthCache<PermissionsCacheKey, Vec<Permission>>>,
}

impl PermissionsCache {
    pub fn new(config: AuthCacheConfig) -> Self {
        Self {
            cache: Arc::new(AuthCache::new("PermissionsCache", config)),
        }
    }

    pub fn get(&self, role: &str, resource: &Resource) -> Option<Vec<Permission>> {
        let key = PermissionsCacheKey {
            role: role.to_string(),
            resource: resource.clone(),
        };
        self.cache.get(&key)
    }

    pub fn put(&self, role: &str, resource: &Resource, permissions: Vec<Permission>) {
        let key = PermissionsCacheKey {
            role: role.to_string(),
            resource: resource.clone(),
        };
        self.cache.put(key, permissions);
    }

    pub fn invalidate_role(&self, role: &str) {
        // Invalidate all entries for this role
        let keys: Vec<PermissionsCacheKey> = self.cache.entries_matching(|k| k.role == role);
        for key in keys {
            self.cache.invalidate(&key);
        }
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_permissions() {
        let cache = PermissionsCache::new(AuthCacheConfig::default());
        let resource = Resource::Keyspace("test_ks".to_string());

        cache.put("reader", &resource, vec![Permission::Select]);
        let perms = cache.get("reader", &resource).unwrap();
        assert_eq!(perms, vec![Permission::Select]);
    }

    #[test]
    fn cache_miss() {
        let cache = PermissionsCache::new(AuthCacheConfig::default());
        assert!(cache.get("nobody", &Resource::Root).is_none());
    }

    #[test]
    fn invalidate_all_clears() {
        let cache = PermissionsCache::new(AuthCacheConfig::default());
        cache.put("r", &Resource::Root, vec![Permission::Select]);
        cache.invalidate_all();
        assert!(cache.get("r", &Resource::Root).is_none());
    }
}
