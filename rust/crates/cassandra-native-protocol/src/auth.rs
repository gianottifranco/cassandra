// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Server-side authentication.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.IAuthenticator`
//! - `org.apache.cassandra.auth.AllowAllAuthenticator`
//! - `org.apache.cassandra.auth.PasswordAuthenticator`

use std::sync::Arc;

use cassandra_security::auth::{
    Authenticator as SecurityAuthenticator, Credentials,
    PasswordAuthenticator as SecurityPasswordAuthenticator,
};
use cassandra_security::roles::{InMemoryRoleManager, RoleManager};

/// Authenticator trait for pluggable authentication backends.
pub trait Authenticator: Send + Sync {
    /// The authenticator class name sent in AUTHENTICATE response.
    fn class_name(&self) -> &str;

    /// Whether authentication is required at all.
    fn requires_auth(&self) -> bool;

    /// Process an AUTH_RESPONSE token.
    /// Returns Ok(Some(challenge)) for multi-step auth, Ok(None) for success.
    fn authenticate(&self, token: Option<&[u8]>) -> Result<AuthResult, String>;
}

/// Result of an authentication step.
#[derive(Debug)]
pub enum AuthResult {
    /// Authentication succeeded, optionally returning the authenticated username and a final token.
    Success(Option<String>, Option<Vec<u8>>),
    /// Multi-step auth: send this challenge to the client.
    Challenge(Vec<u8>),
}

/// AllowAllAuthenticator — no authentication required (default).
///
/// ## Java Oracle
/// - `org.apache.cassandra.auth.AllowAllAuthenticator`
pub struct AllowAllAuthenticator;

impl Authenticator for AllowAllAuthenticator {
    fn class_name(&self) -> &str {
        "org.apache.cassandra.auth.AllowAllAuthenticator"
    }

    fn requires_auth(&self) -> bool {
        false
    }

    fn authenticate(&self, _token: Option<&[u8]>) -> Result<AuthResult, String> {
        Ok(AuthResult::Success(Some("anonymous".to_string()), None))
    }
}

/// Credentials decoded from a SASL PLAIN authentication token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainCredentials {
    /// Optional authorization identity from the first PLAIN field.
    pub authzid: String,
    /// Authentication identity from the second PLAIN field.
    pub username: String,
    /// Password from the third PLAIN field.
    pub password: String,
}

/// Parse a native protocol SASL PLAIN token.
///
/// The wire layout is `[authzid] NUL username NUL password`.
pub fn parse_plain_credentials(token: &[u8]) -> Result<PlainCredentials, String> {
    let parts: Vec<&[u8]> = token.splitn(3, |&b| b == 0).collect();
    if parts.len() < 3 {
        return Err("Invalid PLAIN credentials format".to_string());
    }

    let authzid = std::str::from_utf8(parts[0]).map_err(|_| "Invalid UTF-8 in authzid")?;
    let username = std::str::from_utf8(parts[1]).map_err(|_| "Invalid UTF-8 in username")?;
    let password = std::str::from_utf8(parts[2]).map_err(|_| "Invalid UTF-8 in password")?;

    if username.is_empty() || password.is_empty() {
        return Err("Username and password must not be empty".to_string());
    }

    Ok(PlainCredentials {
        authzid: authzid.to_string(),
        username: username.to_string(),
        password: password.to_string(),
    })
}

/// PasswordAuthenticator — native protocol adapter over `cassandra-security`.
///
/// Parses SASL PLAIN tokens and verifies credentials with the same bcrypt-backed
/// role authenticator used by the server layer. The default instance is backed
/// by an in-memory role manager containing Cassandra's initial `cassandra`
/// superuser; callers can inject any `cassandra-security` authenticator.
///
/// ## Java Oracle
/// - `org.apache.cassandra.auth.PasswordAuthenticator`
pub struct PasswordAuthenticator {
    inner: Arc<dyn SecurityAuthenticator>,
}

impl PasswordAuthenticator {
    /// Create a native password authenticator from a security authenticator.
    pub fn new(inner: Arc<dyn SecurityAuthenticator>) -> Self {
        Self { inner }
    }

    /// Create a native password authenticator from a role manager.
    pub fn with_role_manager<R>(role_manager: R) -> Self
    where
        R: RoleManager + Send + Sync + 'static,
    {
        Self::new(Arc::new(SecurityPasswordAuthenticator::new(role_manager)))
    }
}

impl Default for PasswordAuthenticator {
    fn default() -> Self {
        Self::with_role_manager(InMemoryRoleManager::new())
    }
}

impl Authenticator for PasswordAuthenticator {
    fn class_name(&self) -> &str {
        "org.apache.cassandra.auth.PasswordAuthenticator"
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn authenticate(&self, token: Option<&[u8]>) -> Result<AuthResult, String> {
        let token = token.ok_or("No authentication token provided")?;
        let parsed = parse_plain_credentials(token)?;

        let credentials = Credentials {
            username: parsed.username,
            password: parsed.password,
            source_address: None,
        };
        self.inner
            .authenticate(&credentials)
            .map(|user| AuthResult::Success(Some(user.role_name), None))
            .map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_all_no_auth() {
        let auth = AllowAllAuthenticator;
        assert!(!auth.requires_auth());
        let result = auth.authenticate(None).unwrap();
        assert!(matches!(result, AuthResult::Success(Some(_), None)));
    }

    #[test]
    fn password_auth_valid() {
        let auth = PasswordAuthenticator::default();
        assert!(auth.requires_auth());

        let token = b"\0cassandra\0cassandra";
        let result = auth.authenticate(Some(token)).unwrap();
        assert!(matches!(result, AuthResult::Success(Some(_), None)));
    }

    #[test]
    fn password_auth_empty_username() {
        let auth = PasswordAuthenticator::default();
        let token = b"\0\0password";
        let result = auth.authenticate(Some(token));
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_wrong_password() {
        let auth = PasswordAuthenticator::default();
        let token = b"\0cassandra\0wrong";
        let result = auth.authenticate(Some(token));
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_unknown_user() {
        let auth = PasswordAuthenticator::default();
        let token = b"\0alice\0cassandra";
        let result = auth.authenticate(Some(token));
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_no_token() {
        let auth = PasswordAuthenticator::default();
        let result = auth.authenticate(None);
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_class_name() {
        let auth = PasswordAuthenticator::default();
        assert_eq!(
            auth.class_name(),
            "org.apache.cassandra.auth.PasswordAuthenticator"
        );
    }

    #[test]
    fn parse_plain_credentials_rejects_malformed_token() {
        assert!(parse_plain_credentials(b"cassandra").is_err());
        assert!(parse_plain_credentials(b"\0cassandra").is_err());
    }

    #[test]
    fn parse_plain_credentials_keeps_authzid() {
        let parsed = parse_plain_credentials(b"proxy\0cassandra\0cassandra").unwrap();
        assert_eq!(parsed.authzid, "proxy");
        assert_eq!(parsed.username, "cassandra");
        assert_eq!(parsed.password, "cassandra");
    }
}
