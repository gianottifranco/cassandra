// Licensed under Apache License, Version 2.0.

//! Roles cache — caches role name → transitive role set.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.RolesCache`

use std::sync::Arc;

use crate::cache::{AuthCache, AuthCacheConfig};

/// Cache of transitive role membership lookups.
pub struct RolesCache {
    cache: Arc<AuthCache<String, Vec<String>>>,
}

impl RolesCache {
    pub fn new(config: AuthCacheConfig) -> Self {
        Self {
            cache: Arc::new(AuthCache::new("RolesCache", config)),
        }
    }

    pub fn get(&self, role: &str) -> Option<Vec<String>> {
        self.cache.get(&role.to_string())
    }

    pub fn put(&self, role: &str, all_roles: Vec<String>) {
        self.cache.put(role.to_string(), all_roles);
    }

    pub fn invalidate(&self, role: &str) {
        self.cache.invalidate(&role.to_string());
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_roles() {
        let cache = RolesCache::new(AuthCacheConfig::default());
        cache.put("user1", vec!["user1".into(), "admin".into()]);
        let roles = cache.get("user1").unwrap();
        assert!(roles.contains(&"admin".to_string()));
    }

    #[test]
    fn invalidate() {
        let cache = RolesCache::new(AuthCacheConfig::default());
        cache.put("user1", vec!["user1".into()]);
        cache.invalidate("user1");
        assert!(cache.get("user1").is_none());
    }
}
