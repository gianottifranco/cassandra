// Licensed under Apache License, Version 2.0.

//! Credentials cache — caches username → hashed password.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.PasswordAuthenticator.CredentialsCache`

use std::sync::Arc;

use crate::cache::{AuthCache, AuthCacheConfig};

/// Cache of credential lookups (username → bcrypt hash).
pub struct CredentialsCache {
    cache: Arc<AuthCache<String, String>>,
}

impl CredentialsCache {
    pub fn new(config: AuthCacheConfig) -> Self {
        Self {
            cache: Arc::new(AuthCache::new("CredentialsCache", config)),
        }
    }

    pub fn get(&self, username: &str) -> Option<String> {
        self.cache.get(&username.to_string())
    }

    pub fn put(&self, username: &str, hashed_password: String) {
        self.cache.put(username.to_string(), hashed_password);
    }

    pub fn invalidate(&self, username: &str) {
        self.cache.invalidate(&username.to_string());
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_credentials() {
        let cache = CredentialsCache::new(AuthCacheConfig::default());
        cache.put("admin", "$2b$12$hash".to_string());
        assert_eq!(cache.get("admin"), Some("$2b$12$hash".to_string()));
    }

    #[test]
    fn invalidate() {
        let cache = CredentialsCache::new(AuthCacheConfig::default());
        cache.put("admin", "hash".to_string());
        cache.invalidate("admin");
        assert!(cache.get("admin").is_none());
    }
}
