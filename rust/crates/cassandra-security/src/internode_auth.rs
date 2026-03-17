// Licensed under Apache License, Version 2.0.

//! Internode authenticator — validates peer nodes in the cluster.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.IInternodeAuthenticator`
//! - `org.apache.cassandra.auth.AllowAllInternodeAuthenticator`

use std::net::IpAddr;

use crate::SecurityError;

/// Authenticator for internode (server-to-server) connections.
pub trait InternodeAuthenticator: Send + Sync {
    /// Authenticate a connecting peer node.
    fn authenticate(&self, peer_address: IpAddr, peer_port: u16) -> Result<(), SecurityError>;

    /// Authenticate using a peer's TLS certificate (DER-encoded).
    fn authenticate_with_cert(
        &self,
        cert_der: &[u8],
        peer_address: IpAddr,
    ) -> Result<(), SecurityError>;

    fn name(&self) -> &str;
}

/// Allows all internode connections (default).
pub struct AllowAllInternodeAuthenticator;

impl InternodeAuthenticator for AllowAllInternodeAuthenticator {
    fn authenticate(&self, _peer_address: IpAddr, _peer_port: u16) -> Result<(), SecurityError> {
        Ok(())
    }

    fn authenticate_with_cert(
        &self,
        _cert_der: &[u8],
        _peer_address: IpAddr,
    ) -> Result<(), SecurityError> {
        Ok(())
    }

    fn name(&self) -> &str {
        "AllowAllInternodeAuthenticator"
    }
}

/// Validates internode connections using mTLS certificates.
///
/// Verifies that the peer certificate was signed by a trusted CA.
pub struct MutualTlsInternodeAuthenticator {
    /// Trusted CA certificates (DER-encoded) for peer verification.
    trusted_cas: Vec<Vec<u8>>,
}

impl MutualTlsInternodeAuthenticator {
    pub fn new(trusted_cas: Vec<Vec<u8>>) -> Self {
        Self { trusted_cas }
    }
}

impl InternodeAuthenticator for MutualTlsInternodeAuthenticator {
    fn authenticate(&self, _peer_address: IpAddr, _peer_port: u16) -> Result<(), SecurityError> {
        // Without a certificate, we cannot authenticate via mTLS.
        Err(SecurityError::AuthError(
            "mTLS internode auth requires a client certificate".into(),
        ))
    }

    fn authenticate_with_cert(
        &self,
        cert_der: &[u8],
        _peer_address: IpAddr,
    ) -> Result<(), SecurityError> {
        // Parse the peer certificate to verify it's valid X.509
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
            .map_err(|e| SecurityError::AuthError(format!("invalid peer certificate: {}", e)))?;

        // Verify the certificate was signed by one of our trusted CAs
        for ca_der in &self.trusted_cas {
            if let Ok((_, ca_cert)) = x509_parser::parse_x509_certificate(ca_der) {
                if cert.verify_signature(Some(&ca_cert.tbs_certificate.subject_pki)).is_ok() {
                    return Ok(());
                }
            }
        }

        Err(SecurityError::AuthError(
            "peer certificate not signed by a trusted CA".into(),
        ))
    }

    fn name(&self) -> &str {
        "MutualTlsInternodeAuthenticator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn allow_all_permits_any_peer() {
        let auth = AllowAllInternodeAuthenticator;
        assert!(
            auth.authenticate(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7000)
                .is_ok()
        );
    }

    #[test]
    fn allow_all_permits_any_cert() {
        let auth = AllowAllInternodeAuthenticator;
        assert!(
            auth.authenticate_with_cert(b"anything", IpAddr::V4(Ipv4Addr::LOCALHOST))
                .is_ok()
        );
    }

    #[test]
    fn mtls_internode_rejects_without_cert() {
        let auth = MutualTlsInternodeAuthenticator::new(vec![]);
        assert!(
            auth.authenticate(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)
                .is_err()
        );
    }

    #[test]
    fn mtls_internode_rejects_untrusted_cert() {
        let params = rcgen::CertificateParams::new(vec!["peer".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = cert.der().to_vec();

        // No trusted CAs configured
        let auth = MutualTlsInternodeAuthenticator::new(vec![]);
        assert!(
            auth.authenticate_with_cert(&cert_der, IpAddr::V4(Ipv4Addr::LOCALHOST))
                .is_err()
        );
    }

    #[test]
    fn mtls_internode_accepts_self_signed_as_trusted() {
        let params = rcgen::CertificateParams::new(vec!["node1".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = cert.der().to_vec();

        // Trust the self-signed cert as a CA
        let auth = MutualTlsInternodeAuthenticator::new(vec![cert_der.clone()]);
        assert!(
            auth.authenticate_with_cert(&cert_der, IpAddr::V4(Ipv4Addr::LOCALHOST))
                .is_ok()
        );
    }
}
