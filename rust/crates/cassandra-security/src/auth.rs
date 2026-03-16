// Licensed under Apache License, Version 2.0.

//! Authentication subsystem.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.IAuthenticator`
//! - `org.apache.cassandra.auth.PasswordAuthenticator`
//! - `org.apache.cassandra.auth.AllowAllAuthenticator`

use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::SecurityError;
use crate::roles::RoleManager;

// ─── Types ─────────────────────────────────────────────────────────────────

/// Credentials presented during authentication.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub source_address: Option<IpAddr>,
}

/// An authenticated user/role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedUser {
    pub role_name: String,
    pub is_superuser: bool,
    pub is_anonymous: bool,
}

impl AuthenticatedUser {
    /// The anonymous user for AllowAllAuthenticator.
    pub fn anonymous() -> Self {
        Self {
            role_name: "anonymous".to_string(),
            is_superuser: true,
            is_anonymous: true,
        }
    }

    /// System internal user.
    pub fn system() -> Self {
        Self {
            role_name: "cassandra".to_string(),
            is_superuser: true,
            is_anonymous: false,
        }
    }
}

// ─── Authenticator Trait ───────────────────────────────────────────────────

/// Pluggable authentication interface.
///
/// Matches `org.apache.cassandra.auth.IAuthenticator`.
pub trait Authenticator: Send + Sync {
    /// Authenticate with the given credentials.
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser, SecurityError>;

    /// Whether authentication is required (false for AllowAll).
    fn require_authentication(&self) -> bool;

    /// Name of this authenticator for logging/config.
    fn name(&self) -> &str;
}

// ─── AllowAllAuthenticator ────────────────────────────────────────────────

/// Authenticator that permits all connections without credentials.
/// Default when no authentication is configured.
pub struct AllowAllAuthenticator;

impl Authenticator for AllowAllAuthenticator {
    fn authenticate(&self, _credentials: &Credentials) -> Result<AuthenticatedUser, SecurityError> {
        Ok(AuthenticatedUser::anonymous())
    }

    fn require_authentication(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "AllowAllAuthenticator"
    }
}

// ─── PasswordAuthenticator ────────────────────────────────────────────────

/// Authenticates users against bcrypt-hashed passwords stored in a RoleManager.
///
/// Java equivalent: `org.apache.cassandra.auth.PasswordAuthenticator`
pub struct PasswordAuthenticator<R: RoleManager> {
    role_manager: R,
}

impl<R: RoleManager> PasswordAuthenticator<R> {
    pub fn new(role_manager: R) -> Self {
        Self { role_manager }
    }
}

impl<R: RoleManager + Send + Sync> Authenticator for PasswordAuthenticator<R> {
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser, SecurityError> {
        let role = self
            .role_manager
            .get_role(&credentials.username)
            .ok_or_else(|| {
                SecurityError::AuthError(format!(
                    "provided username '{}' and/or password are incorrect",
                    credentials.username
                ))
            })?;

        if !role.can_login {
            return Err(SecurityError::AuthError(format!(
                "'{}' is not permitted to log in",
                credentials.username
            )));
        }

        let stored_hash = role
            .hashed_password
            .as_ref()
            .ok_or_else(|| SecurityError::AuthError("no password set for role".to_string()))?;

        let valid = bcrypt::verify(&credentials.password, stored_hash)
            .map_err(|e| SecurityError::AuthError(format!("password verification error: {}", e)))?;

        if !valid {
            return Err(SecurityError::AuthError(format!(
                "provided username '{}' and/or password are incorrect",
                credentials.username
            )));
        }

        Ok(AuthenticatedUser {
            role_name: role.name.clone(),
            is_superuser: role.is_superuser,
            is_anonymous: false,
        })
    }

    fn require_authentication(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "PasswordAuthenticator"
    }
}

/// Hash a plaintext password for storage.
pub fn hash_password(password: &str) -> Result<String, SecurityError> {
    bcrypt::hash(password, bcrypt::DEFAULT_COST)
        .map_err(|e| SecurityError::AuthError(format!("bcrypt hash error: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::{InMemoryRoleManager, Role};

    #[test]
    fn allow_all_authenticator() {
        let auth = AllowAllAuthenticator;
        let creds = Credentials {
            username: "anyone".into(),
            password: "anything".into(),
            source_address: None,
        };
        let user = auth.authenticate(&creds).unwrap();
        assert!(user.is_anonymous);
        assert!(user.is_superuser);
        assert!(!auth.require_authentication());
    }

    #[test]
    fn password_auth_success() {
        let manager = InMemoryRoleManager::new();
        let hashed = hash_password("secret123").unwrap();
        manager.create_role(Role {
            name: "admin".into(),
            is_superuser: true,
            can_login: true,
            hashed_password: Some(hashed),
            member_of: vec![],
            network_permissions: None,
        });

        let auth = PasswordAuthenticator::new(manager);
        let creds = Credentials {
            username: "admin".into(),
            password: "secret123".into(),
            source_address: None,
        };
        let user = auth.authenticate(&creds).unwrap();
        assert_eq!(user.role_name, "admin");
        assert!(user.is_superuser);
    }

    #[test]
    fn password_auth_wrong_password() {
        let manager = InMemoryRoleManager::new();
        let hashed = hash_password("correct").unwrap();
        manager.create_role(Role {
            name: "user1".into(),
            is_superuser: false,
            can_login: true,
            hashed_password: Some(hashed),
            member_of: vec![],
            network_permissions: None,
        });

        let auth = PasswordAuthenticator::new(manager);
        let creds = Credentials {
            username: "user1".into(),
            password: "wrong".into(),
            source_address: None,
        };
        assert!(auth.authenticate(&creds).is_err());
    }

    #[test]
    fn password_auth_unknown_user() {
        let manager = InMemoryRoleManager::new();
        let auth = PasswordAuthenticator::new(manager);
        let creds = Credentials {
            username: "ghost".into(),
            password: "x".into(),
            source_address: None,
        };
        assert!(auth.authenticate(&creds).is_err());
    }

    #[test]
    fn password_auth_no_login_role() {
        let manager = InMemoryRoleManager::new();
        let hashed = hash_password("pass").unwrap();
        manager.create_role(Role {
            name: "nologin".into(),
            is_superuser: false,
            can_login: false,
            hashed_password: Some(hashed),
            member_of: vec![],
            network_permissions: None,
        });

        let auth = PasswordAuthenticator::new(manager);
        let creds = Credentials {
            username: "nologin".into(),
            password: "pass".into(),
            source_address: None,
        };
        let err = auth.authenticate(&creds).unwrap_err();
        assert!(err.to_string().contains("not permitted to log in"));
    }

    #[test]
    fn hash_and_verify() {
        let hashed = hash_password("testpass").unwrap();
        assert!(bcrypt::verify("testpass", &hashed).unwrap());
        assert!(!bcrypt::verify("wrongpass", &hashed).unwrap());
    }
}
