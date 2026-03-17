// Licensed under Apache License, Version 2.0.

//! Mutual TLS authenticator and certificate validators.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.MutualTlsAuthenticator`
//! - `org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator`

use crate::auth::AuthenticatedUser;
use crate::identity_mapping::IdentityRoleMapper;
use crate::SecurityError;

/// Extracts an identity string from a client certificate.
pub trait CertificateValidator: Send + Sync {
    /// Extract the identity from a DER-encoded certificate.
    fn validate(&self, cert_der: &[u8]) -> Result<String, SecurityError>;

    fn name(&self) -> &str;
}

/// Extracts the Subject CN (Common Name) from the certificate.
pub struct SubjectCnValidator;

impl CertificateValidator for SubjectCnValidator {
    fn validate(&self, cert_der: &[u8]) -> Result<String, SecurityError> {
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|e| SecurityError::AuthError(format!("invalid X.509 certificate: {}", e)))?;

        for rdn in cert.subject().iter() {
            for attr in rdn.iter() {
                if attr.attr_type() == &x509_parser::oid_registry::OID_X509_COMMON_NAME {
                    let cn = attr
                        .as_str()
                        .map_err(|e| {
                            SecurityError::AuthError(format!("invalid CN encoding: {}", e))
                        })?;
                    return Ok(cn.to_string());
                }
            }
        }

        Err(SecurityError::AuthError(
            "no Common Name found in certificate subject".into(),
        ))
    }

    fn name(&self) -> &str {
        "SubjectCnValidator"
    }
}

/// Extracts a SPIFFE URI from the certificate's SAN extension.
pub struct SpiffeCertificateValidator;

impl CertificateValidator for SpiffeCertificateValidator {
    fn validate(&self, cert_der: &[u8]) -> Result<String, SecurityError> {
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|e| SecurityError::AuthError(format!("invalid X.509 certificate: {}", e)))?;

        for ext in cert.extensions() {
            if let x509_parser::extensions::ParsedExtension::SubjectAlternativeName(san) =
                ext.parsed_extension()
            {
                for name in &san.general_names {
                    if let x509_parser::extensions::GeneralName::URI(uri) = name {
                        if uri.starts_with("spiffe://") {
                            return Ok(uri.to_string());
                        }
                    }
                }
            }
        }

        Err(SecurityError::AuthError(
            "no SPIFFE URI found in certificate SAN".into(),
        ))
    }

    fn name(&self) -> &str {
        "SpiffeCertificateValidator"
    }
}

/// Authenticator that uses mTLS client certificates.
///
/// Extracts an identity from the certificate using a `CertificateValidator`,
/// then maps it to a Cassandra role via `IdentityRoleMapper`.
pub struct MutualTlsAuthenticator {
    validator: Box<dyn CertificateValidator>,
    mapper: Box<dyn IdentityRoleMapper>,
}

impl MutualTlsAuthenticator {
    pub fn new(
        validator: Box<dyn CertificateValidator>,
        mapper: Box<dyn IdentityRoleMapper>,
    ) -> Self {
        Self { validator, mapper }
    }

    /// Authenticate a client by their DER-encoded certificate.
    pub fn authenticate_cert(&self, cert_der: &[u8]) -> Result<AuthenticatedUser, SecurityError> {
        let identity = self.validator.validate(cert_der)?;

        let role_name = self
            .mapper
            .get_role_for_identity(&identity)
            .ok_or_else(|| {
                SecurityError::AuthError(format!(
                    "no role mapping found for identity '{}'",
                    identity
                ))
            })?;

        Ok(AuthenticatedUser {
            role_name,
            is_superuser: false, // caller should check via RoleManager
            is_anonymous: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity_mapping::InMemoryIdentityRoleMapper;

    fn make_self_signed_cert(cn: &str) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        cert.der().to_vec()
    }

    #[test]
    fn subject_cn_extraction() {
        let cert_der = make_self_signed_cert("testnode.example.com");
        let validator = SubjectCnValidator;
        let cn = validator.validate(&cert_der).unwrap();
        assert_eq!(cn, "testnode.example.com");
    }

    #[test]
    fn mtls_authenticator_maps_identity_to_role() {
        let cert_der = make_self_signed_cert("service-a");
        let mapper = InMemoryIdentityRoleMapper::new();
        mapper.set_mapping("service-a", "service_role");

        let auth = MutualTlsAuthenticator::new(Box::new(SubjectCnValidator), Box::new(mapper));

        let user = auth.authenticate_cert(&cert_der).unwrap();
        assert_eq!(user.role_name, "service_role");
        assert!(!user.is_anonymous);
    }

    #[test]
    fn mtls_authenticator_no_mapping_fails() {
        let cert_der = make_self_signed_cert("unknown-service");
        let mapper = InMemoryIdentityRoleMapper::new();
        let auth = MutualTlsAuthenticator::new(Box::new(SubjectCnValidator), Box::new(mapper));
        assert!(auth.authenticate_cert(&cert_der).is_err());
    }
}
