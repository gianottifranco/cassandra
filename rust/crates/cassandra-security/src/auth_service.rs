// Licensed under Apache License, Version 2.0.

//! Auth service — orchestrates all auth components.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.AuthCacheMBean` (cache management)
//! - `org.apache.cassandra.service.StorageService` (auth initialization)

use std::net::IpAddr;
use std::sync::Arc;

use crate::SecurityError;
use crate::auth::{AuthenticatedUser, Authenticator, Credentials};
use crate::authz::{Authorizer, Permission, Resource};
use crate::cache::AuthCacheConfig;
use crate::cidr::CidrAuthorizer;
use crate::credentials_cache::CredentialsCache;
use crate::network_auth::NetworkAuthorizer;
use crate::permissions_cache::PermissionsCache;
use crate::roles::RoleManager;
use crate::roles_cache::RolesCache;

/// Unified auth manager owning all auth components and caches.
///
/// Provides a single entry point for authentication, authorization,
/// and cache management.
pub struct AuthManager {
    authenticator: Arc<dyn Authenticator>,
    authorizer: Arc<dyn Authorizer>,
    role_manager: Arc<dyn RoleManager>,
    network_authorizer: Arc<dyn NetworkAuthorizer>,
    cidr_authorizer: Option<Arc<CidrAuthorizer>>,
    permissions_cache: PermissionsCache,
    roles_cache: RolesCache,
    credentials_cache: CredentialsCache,
}

impl AuthManager {
    pub fn new(
        authenticator: Arc<dyn Authenticator>,
        authorizer: Arc<dyn Authorizer>,
        role_manager: Arc<dyn RoleManager>,
        network_authorizer: Arc<dyn NetworkAuthorizer>,
        cidr_authorizer: Option<Arc<CidrAuthorizer>>,
        cache_config: AuthCacheConfig,
    ) -> Self {
        Self {
            authenticator,
            authorizer,
            role_manager,
            network_authorizer,
            cidr_authorizer,
            permissions_cache: PermissionsCache::new(cache_config.clone()),
            roles_cache: RolesCache::new(cache_config.clone()),
            credentials_cache: CredentialsCache::new(cache_config),
        }
    }

    /// Authenticate a user with credentials.
    pub fn authenticate(
        &self,
        credentials: &Credentials,
    ) -> Result<AuthenticatedUser, SecurityError> {
        self.authenticator.authenticate(credentials)
    }

    /// Check if a user has permission on a resource.
    pub fn check_access(
        &self,
        user: &str,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), SecurityError> {
        self.authorizer.authorize(user, resource, permission)
    }

    /// Check network authorization (DC-level access).
    pub fn check_network_access(&self, role: &str, dc: &str) -> Result<(), SecurityError> {
        self.network_authorizer.authorize(role, dc)
    }

    /// Check CIDR access if CIDR authorizer is configured.
    pub fn check_cidr_access(&self, addr: IpAddr, role: &str) -> Result<(), SecurityError> {
        match &self.cidr_authorizer {
            Some(cidr) => cidr.check_access(addr, role),
            None => Ok(()),
        }
    }

    /// Full access check: authenticate + authorize + network + CIDR.
    pub fn full_access_check(
        &self,
        credentials: &Credentials,
        resource: &Resource,
        permission: Permission,
        dc: &str,
    ) -> Result<AuthenticatedUser, SecurityError> {
        let user = self.authenticate(credentials)?;

        // CIDR check (if configured)
        if let Some(addr) = credentials.source_address {
            self.check_cidr_access(addr, &user.role_name)?;
        }

        // Network (DC) check
        self.check_network_access(&user.role_name, dc)?;

        // Permission check
        self.check_access(&user.role_name, resource, permission)?;

        Ok(user)
    }

    /// Whether authentication is required.
    pub fn require_authentication(&self) -> bool {
        self.authenticator.require_authentication()
    }

    /// Whether authorization is required.
    pub fn require_authorization(&self) -> bool {
        self.authorizer.require_authorization()
    }

    // ─── Cache Management ────────────────────────────────────────────────

    pub fn invalidate_permissions_cache(&self) {
        self.permissions_cache.invalidate_all();
    }

    pub fn invalidate_roles_cache(&self) {
        self.roles_cache.invalidate_all();
    }

    pub fn invalidate_credentials_cache(&self) {
        self.credentials_cache.invalidate_all();
    }

    pub fn invalidate_all_caches(&self) {
        self.permissions_cache.invalidate_all();
        self.roles_cache.invalidate_all();
        self.credentials_cache.invalidate_all();
    }

    // ─── Component Accessors ─────────────────────────────────────────────

    pub fn authenticator(&self) -> &Arc<dyn Authenticator> {
        &self.authenticator
    }

    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.authorizer
    }

    pub fn role_manager(&self) -> &Arc<dyn RoleManager> {
        &self.role_manager
    }

    pub fn network_authorizer(&self) -> &Arc<dyn NetworkAuthorizer> {
        &self.network_authorizer
    }

    pub fn permissions_cache(&self) -> &PermissionsCache {
        &self.permissions_cache
    }

    pub fn roles_cache(&self) -> &RolesCache {
        &self.roles_cache
    }

    pub fn credentials_cache(&self) -> &CredentialsCache {
        &self.credentials_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AllowAllAuthenticator;
    use crate::authz::AllowAllAuthorizer;
    use crate::network_auth::AllowAllNetworkAuthorizer;
    use crate::roles::InMemoryRoleManager;

    fn make_auth_manager() -> AuthManager {
        AuthManager::new(
            Arc::new(AllowAllAuthenticator),
            Arc::new(AllowAllAuthorizer),
            Arc::new(InMemoryRoleManager::new()),
            Arc::new(AllowAllNetworkAuthorizer),
            None,
            AuthCacheConfig::default(),
        )
    }

    #[test]
    fn authenticate_allow_all() {
        let mgr = make_auth_manager();
        let creds = Credentials {
            username: "anyone".into(),
            password: "any".into(),
            source_address: None,
        };
        let user = mgr.authenticate(&creds).unwrap();
        assert!(user.is_anonymous);
    }

    #[test]
    fn check_access_allow_all() {
        let mgr = make_auth_manager();
        assert!(
            mgr.check_access("anyone", &Resource::Root, Permission::Select)
                .is_ok()
        );
    }

    #[test]
    fn full_access_check_allow_all() {
        let mgr = make_auth_manager();
        let creds = Credentials {
            username: "user".into(),
            password: "pass".into(),
            source_address: None,
        };
        let user = mgr
            .full_access_check(&creds, &Resource::Root, Permission::Select, "dc1")
            .unwrap();
        assert!(user.is_anonymous);
    }

    #[test]
    fn cache_invalidation() {
        let mgr = make_auth_manager();
        mgr.permissions_cache()
            .put("r", &Resource::Root, vec![Permission::Select]);
        mgr.roles_cache().put("r", vec!["r".into()]);
        mgr.credentials_cache().put("u", "hash".into());

        mgr.invalidate_all_caches();

        assert!(mgr.permissions_cache().get("r", &Resource::Root).is_none());
        assert!(mgr.roles_cache().get("r").is_none());
        assert!(mgr.credentials_cache().get("u").is_none());
    }

    #[test]
    fn require_flags() {
        let mgr = make_auth_manager();
        assert!(!mgr.require_authentication());
        assert!(!mgr.require_authorization());
    }
}
