// Licensed under Apache License, Version 2.0.

//! External authentication adapters.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.LdapAuthenticator`
//! - Kerberos/SASL integration points used by Cassandra deployments

use std::collections::HashMap;

use crate::{AuthenticatedUser, Authenticator, Credentials, SecurityError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdentity {
    pub role_name: String,
    pub is_superuser: bool,
}

pub trait LdapDirectory: Send + Sync {
    fn bind(&self, username: &str, password: &str) -> Result<ExternalIdentity, SecurityError>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryLdapDirectory {
    users: HashMap<String, LdapUser>,
}

#[derive(Debug, Clone)]
struct LdapUser {
    password: String,
    identity: ExternalIdentity,
}

impl InMemoryLdapDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_user(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        role_name: impl Into<String>,
        is_superuser: bool,
    ) -> Self {
        self.users.insert(
            username.into(),
            LdapUser {
                password: password.into(),
                identity: ExternalIdentity {
                    role_name: role_name.into(),
                    is_superuser,
                },
            },
        );
        self
    }
}

impl LdapDirectory for InMemoryLdapDirectory {
    fn bind(&self, username: &str, password: &str) -> Result<ExternalIdentity, SecurityError> {
        let user = self.users.get(username).ok_or_else(|| {
            SecurityError::AuthError(format!("LDAP user '{}' was not found", username))
        })?;
        if user.password != password {
            return Err(SecurityError::AuthError(
                "LDAP bind failed: invalid credentials".to_string(),
            ));
        }
        Ok(user.identity.clone())
    }
}

pub struct LdapAuthenticator<D: LdapDirectory> {
    directory: D,
}

impl<D: LdapDirectory> LdapAuthenticator<D> {
    pub fn new(directory: D) -> Self {
        Self { directory }
    }
}

impl<D: LdapDirectory> Authenticator for LdapAuthenticator<D> {
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser, SecurityError> {
        let identity = self
            .directory
            .bind(&credentials.username, &credentials.password)?;
        Ok(AuthenticatedUser {
            role_name: identity.role_name,
            is_superuser: identity.is_superuser,
            is_anonymous: false,
        })
    }

    fn require_authentication(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "LdapAuthenticator"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KerberosPrincipal {
    pub principal: String,
    pub role_name: String,
    pub is_superuser: bool,
}

pub trait KerberosTicketValidator: Send + Sync {
    fn validate(
        &self,
        service_principal: &str,
        token: &[u8],
    ) -> Result<KerberosPrincipal, SecurityError>;
}

#[derive(Debug, Clone, Default)]
pub struct StaticKerberosValidator {
    tickets: HashMap<Vec<u8>, KerberosPrincipal>,
}

impl StaticKerberosValidator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_ticket(
        mut self,
        token: impl Into<Vec<u8>>,
        principal: impl Into<String>,
        role_name: impl Into<String>,
        is_superuser: bool,
    ) -> Self {
        self.tickets.insert(
            token.into(),
            KerberosPrincipal {
                principal: principal.into(),
                role_name: role_name.into(),
                is_superuser,
            },
        );
        self
    }
}

impl KerberosTicketValidator for StaticKerberosValidator {
    fn validate(
        &self,
        _service_principal: &str,
        token: &[u8],
    ) -> Result<KerberosPrincipal, SecurityError> {
        self.tickets.get(token).cloned().ok_or_else(|| {
            SecurityError::AuthError("Kerberos ticket validation failed".to_string())
        })
    }
}

pub struct KerberosAuthenticator<V: KerberosTicketValidator> {
    service_principal: String,
    validator: V,
}

impl<V: KerberosTicketValidator> KerberosAuthenticator<V> {
    pub fn new(service_principal: impl Into<String>, validator: V) -> Self {
        Self {
            service_principal: service_principal.into(),
            validator,
        }
    }

    pub fn authenticate_token(&self, token: &[u8]) -> Result<AuthenticatedUser, SecurityError> {
        let principal = self.validator.validate(&self.service_principal, token)?;
        Ok(AuthenticatedUser {
            role_name: principal.role_name,
            is_superuser: principal.is_superuser,
            is_anonymous: false,
        })
    }
}

impl<V: KerberosTicketValidator> Authenticator for KerberosAuthenticator<V> {
    fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser, SecurityError> {
        self.authenticate_token(credentials.password.as_bytes())
    }

    fn require_authentication(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "KerberosAuthenticator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ldap_authenticates_bound_user() {
        let directory = InMemoryLdapDirectory::new().add_user("ada", "secret", "analyst", false);
        let auth = LdapAuthenticator::new(directory);
        let user = auth
            .authenticate(&Credentials {
                username: "ada".into(),
                password: "secret".into(),
                source_address: None,
            })
            .unwrap();
        assert_eq!(user.role_name, "analyst");
        assert!(auth.require_authentication());
    }

    #[test]
    fn kerberos_authenticates_ticket_token() {
        let validator = StaticKerberosValidator::new().add_ticket(
            b"ticket".to_vec(),
            "ada@EXAMPLE.COM",
            "analyst",
            false,
        );
        let auth = KerberosAuthenticator::new("cassandra/host@EXAMPLE.COM", validator);
        let user = auth.authenticate_token(b"ticket").unwrap();
        assert_eq!(user.role_name, "analyst");
        assert!(auth.authenticate_token(b"bad-ticket").is_err());
    }
}
