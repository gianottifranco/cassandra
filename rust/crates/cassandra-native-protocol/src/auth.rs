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

/// PasswordAuthenticator — PLAIN SASL credential parser.
///
/// Parses SASL PLAIN tokens (\0username\0password) for the native protocol.
/// In production, `cassandra-server::NativeAuthWrapper` wraps a
/// `cassandra-security::PasswordAuthenticator` for real bcrypt verification.
/// This standalone implementation accepts any non-empty credentials and is
/// only used when the native-protocol crate is used without the server layer.
///
/// ## Java Oracle
/// - `org.apache.cassandra.auth.PasswordAuthenticator`
pub struct PasswordAuthenticator;

impl Authenticator for PasswordAuthenticator {
    fn class_name(&self) -> &str {
        "org.apache.cassandra.auth.PasswordAuthenticator"
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn authenticate(&self, token: Option<&[u8]>) -> Result<AuthResult, String> {
        let token = token.ok_or("No authentication token provided")?;

        // PLAIN SASL: \0username\0password
        let parts: Vec<&[u8]> = token.splitn(3, |&b| b == 0).collect();
        if parts.len() < 3 {
            return Err("Invalid PLAIN credentials format".to_string());
        }

        let _authzid = std::str::from_utf8(parts[0]).map_err(|_| "Invalid UTF-8 in authzid")?;
        let username = std::str::from_utf8(parts[1]).map_err(|_| "Invalid UTF-8 in username")?;
        let password = std::str::from_utf8(parts[2]).map_err(|_| "Invalid UTF-8 in password")?;

        if username.is_empty() || password.is_empty() {
            return Err("Username and password must not be empty".to_string());
        }

        // TODO(phase-7): Real credential verification against system_auth.roles.
        // For now, accept any non-empty credentials (stub).
        tracing::info!("PasswordAuthenticator: accepted user '{}' (stub)", username);
        Ok(AuthResult::Success(Some(username.to_string()), None))
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
        let auth = PasswordAuthenticator;
        assert!(auth.requires_auth());

        let token = b"\0cassandra\0cassandra";
        let result = auth.authenticate(Some(token)).unwrap();
        assert!(matches!(result, AuthResult::Success(Some(_), None)));
    }

    #[test]
    fn password_auth_empty_username() {
        let auth = PasswordAuthenticator;
        let token = b"\0\0password";
        let result = auth.authenticate(Some(token));
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_no_token() {
        let auth = PasswordAuthenticator;
        let result = auth.authenticate(None);
        assert!(result.is_err());
    }

    #[test]
    fn password_auth_class_name() {
        let auth = PasswordAuthenticator;
        assert_eq!(
            auth.class_name(),
            "org.apache.cassandra.auth.PasswordAuthenticator"
        );
    }
}
