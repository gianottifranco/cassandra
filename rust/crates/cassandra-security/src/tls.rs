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

//! TLS configuration and connection factories.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.security.SSLFactory`
//! - `org.apache.cassandra.config.EncryptionOptions`
//!
//! ## Design
//! Uses `rustls` (pure Rust) instead of OpenSSL.
//! See ADR-012 for rationale.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use crate::SecurityError;

// ─── Configuration ─────────────────────────────────────────────────────────

/// TLS encryption options matching cassandra.yaml `client_encryption_options`
/// and `server_encryption_options`.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    pub enabled: bool,
    /// Path to the certificate chain PEM file.
    pub certificate_path: PathBuf,
    /// Path to the private key PEM file.
    pub key_path: PathBuf,
    /// Optional CA certificate path for peer verification.
    pub ca_certificate_path: Option<PathBuf>,
    /// Whether to require client certificates (mutual TLS).
    pub require_client_auth: bool,
    /// Minimum TLS version (default: TLS 1.2).
    pub min_tls_version: TlsVersion,
    /// Enable automatic certificate reload on file change.
    pub hot_reload: bool,
    /// How often to check for cert changes (seconds).
    pub reload_interval_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            certificate_path: PathBuf::from("conf/certs/node.crt"),
            key_path: PathBuf::from("conf/certs/node.key"),
            ca_certificate_path: None,
            require_client_auth: false,
            min_tls_version: TlsVersion::Tls12,
            hot_reload: false,
            reload_interval_secs: 600,
        }
    }
}

// ─── Certificate Loading ───────────────────────────────────────────────────

/// Load PEM certificates from a file.
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, SecurityError> {
    let file = fs::File::open(path).map_err(|e| {
        SecurityError::TlsError(format!("cannot open cert file {}: {}", path.display(), e))
    })?;
    let mut reader = std::io::BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(|r| r.ok())
        .collect();
    if certs.is_empty() {
        return Err(SecurityError::TlsError(format!(
            "no valid certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

/// Load a PEM private key from a file. Tries PKCS8, then RSA, then EC.
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, SecurityError> {
    let key_data = fs::read(path).map_err(|e| {
        SecurityError::TlsError(format!("cannot read key file {}: {}", path.display(), e))
    })?;
    let mut cursor = std::io::Cursor::new(&key_data);

    for item in rustls_pemfile::read_all(&mut cursor) {
        match item {
            Ok(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            _ => continue,
        }
    }

    Err(SecurityError::TlsError(format!(
        "no valid private key found in {}",
        path.display()
    )))
}

/// Build a root certificate store from a CA cert file.
pub fn build_root_store(ca_path: &Path) -> Result<RootCertStore, SecurityError> {
    let certs = load_certs(ca_path)?;
    let mut store = RootCertStore::empty();
    for cert in certs {
        store
            .add(cert)
            .map_err(|e| SecurityError::TlsError(format!("invalid CA certificate: {}", e)))?;
    }
    Ok(store)
}

// ─── Provider Setup ────────────────────────────────────────────────────────

/// Ensure the ring CryptoProvider is installed (required by rustls 0.23+).
fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

// ─── Server Config ─────────────────────────────────────────────────────────

/// Build a `rustls::ServerConfig` from our TLS config.
pub fn build_server_config(config: &TlsConfig) -> Result<ServerConfig, SecurityError> {
    ensure_crypto_provider();
    let certs = load_certs(&config.certificate_path)?;
    let key = load_private_key(&config.key_path)?;

    let builder = if config.require_client_auth {
        let ca_path = config.ca_certificate_path.as_ref().ok_or_else(|| {
            SecurityError::TlsError(
                "require_client_auth=true but no ca_certificate_path set".into(),
            )
        })?;
        let root_store = build_root_store(ca_path)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| SecurityError::TlsError(format!("client verifier error: {}", e)))?;
        ServerConfig::builder().with_client_cert_verifier(verifier)
    } else {
        ServerConfig::builder().with_no_client_auth()
    };

    let server_config = builder
        .with_single_cert(certs, key)
        .map_err(|e| SecurityError::TlsError(format!("server config error: {}", e)))?;

    Ok(server_config)
}

/// Build a `rustls::ClientConfig` (for internode connections).
pub fn build_client_config(config: &TlsConfig) -> Result<ClientConfig, SecurityError> {
    ensure_crypto_provider();
    let mut root_store = RootCertStore::empty();
    if let Some(ref ca_path) = config.ca_certificate_path {
        root_store = build_root_store(ca_path)?;
    }

    let builder = ClientConfig::builder().with_root_certificates(root_store);

    // If we have a client cert for mutual TLS
    let client_config = if config.require_client_auth {
        let certs = load_certs(&config.certificate_path)?;
        let key = load_private_key(&config.key_path)?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| SecurityError::TlsError(format!("client auth config: {}", e)))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(client_config)
}

// ─── Reloadable TLS Acceptor ───────────────────────────────────────────────

/// A TLS acceptor that can be hot-reloaded when certificates change.
pub struct ReloadableTlsAcceptor {
    inner: Arc<RwLock<Arc<ServerConfig>>>,
}

impl ReloadableTlsAcceptor {
    /// Create a new reloadable TLS acceptor.
    pub fn new(config: &TlsConfig) -> Result<Self, SecurityError> {
        let server_config = build_server_config(config)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(server_config))),
        })
    }

    /// Get a `TlsAcceptor` using the current config.
    pub fn acceptor(&self) -> TlsAcceptor {
        let config = self.inner.read().clone();
        TlsAcceptor::from(config)
    }

    /// Reload certificates from disk. Call this when files change.
    pub fn reload(&self, config: &TlsConfig) -> Result<(), SecurityError> {
        let new_config = build_server_config(config)?;
        let mut guard = self.inner.write();
        *guard = Arc::new(new_config);
        info!("TLS certificates reloaded successfully");
        Ok(())
    }

    /// Spawn a background task that reloads certs periodically.
    pub fn spawn_reload_task(self: &Arc<Self>, config: TlsConfig) -> tokio::task::JoinHandle<()> {
        let acceptor = Arc::clone(self);
        let interval = Duration::from_secs(config.reload_interval_secs.max(10));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                match acceptor.reload(&config) {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("TLS cert reload failed (keeping old certs): {}", e);
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn generate_self_signed() -> (String, String) {
        let cert_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = cert_params
            .self_signed(&rcgen::KeyPair::generate().unwrap())
            .unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert_params2 = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert2 = cert_params2.self_signed(&key_pair).unwrap();
        (cert2.pem(), key_pair.serialize_pem())
    }

    fn write_pem_files(dir: &TempDir) -> (PathBuf, PathBuf) {
        let (cert_pem, key_pem) = generate_self_signed();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        fs::write(&cert_path, cert_pem).unwrap();
        fs::write(&key_path, key_pem).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn load_valid_certs() {
        let dir = TempDir::new().unwrap();
        let (cert_path, _) = write_pem_files(&dir);
        let certs = load_certs(&cert_path).unwrap();
        assert!(!certs.is_empty());
    }

    #[test]
    fn load_valid_key() {
        let dir = TempDir::new().unwrap();
        let (_, key_path) = write_pem_files(&dir);
        let key = load_private_key(&key_path);
        assert!(key.is_ok());
    }

    #[test]
    fn load_nonexistent_cert_fails() {
        let result = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(result.is_err());
    }

    #[test]
    fn build_server_config_works() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = write_pem_files(&dir);
        let config = TlsConfig {
            enabled: true,
            certificate_path: cert_path,
            key_path,
            ..Default::default()
        };
        let result = build_server_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn build_client_config_with_ca() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = write_pem_files(&dir);
        let config = TlsConfig {
            enabled: true,
            certificate_path: cert_path.clone(),
            key_path,
            ca_certificate_path: Some(cert_path), // self-signed as CA
            require_client_auth: false,
            ..Default::default()
        };
        let result = build_client_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn reloadable_acceptor_works() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = write_pem_files(&dir);
        let config = TlsConfig {
            enabled: true,
            certificate_path: cert_path,
            key_path,
            ..Default::default()
        };
        let acceptor = ReloadableTlsAcceptor::new(&config);
        assert!(acceptor.is_ok());
        let acceptor = acceptor.unwrap();
        // Getting an acceptor should not panic
        let _ = acceptor.acceptor();
    }

    #[test]
    fn reloadable_acceptor_reload() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = write_pem_files(&dir);
        let config = TlsConfig {
            enabled: true,
            certificate_path: cert_path,
            key_path,
            ..Default::default()
        };
        let acceptor = ReloadableTlsAcceptor::new(&config).unwrap();
        // Reload should succeed with same certs
        assert!(acceptor.reload(&config).is_ok());
    }
}
