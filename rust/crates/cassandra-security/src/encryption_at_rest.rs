// Licensed under Apache License, Version 2.0.

//! Encryption-at-rest integration hooks for storage subsystems.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.security.EncryptionUtils`
//!
//! Defines the `StorageEncryptor` trait with hook points for commitlog
//! and SSTable write paths. Default implementation is `NoOpStorageEncryptor`.

use crate::encryption_context::EncryptionContext;
use crate::SecurityError;
use std::sync::Arc;

/// Trait for storage-level encryption hooks.
///
/// Implemented by both `NoOpStorageEncryptor` (default, no encryption)
/// and `TdeStorageEncryptor` (real encryption via EncryptionContext).
pub trait StorageEncryptor: Send + Sync {
    /// Encrypt a data segment (commitlog segment or SSTable data block).
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError>;

    /// Decrypt a data segment.
    fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError>;

    /// Whether encryption is enabled.
    fn is_enabled(&self) -> bool;
}

/// No-op encryptor — passes data through unchanged.
pub struct NoOpStorageEncryptor;

impl StorageEncryptor for NoOpStorageEncryptor {
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError> {
        Ok(data.to_vec())
    }

    fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError> {
        Ok(data.to_vec())
    }

    fn is_enabled(&self) -> bool {
        false
    }
}

/// TDE-backed storage encryptor using the EncryptionContext.
pub struct TdeStorageEncryptor {
    context: Arc<EncryptionContext>,
}

impl TdeStorageEncryptor {
    pub fn new(context: Arc<EncryptionContext>) -> Self {
        Self { context }
    }
}

impl StorageEncryptor for TdeStorageEncryptor {
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError> {
        self.context.encrypt_chunked(data)
    }

    fn decrypt_segment(&self, _data: &[u8]) -> Result<Vec<u8>, SecurityError> {
        // Full chunked decrypt requires parsing headers from the stream.
        // This is a placeholder — real implementation will parse header+ciphertext pairs.
        Err(SecurityError::ConfigError(
            "chunked decrypt not yet implemented; use EncryptionContext::decrypt_chunk with headers".to_string(),
        ))
    }

    fn is_enabled(&self) -> bool {
        self.context.is_enabled()
    }
}

/// Create the appropriate storage encryptor based on configuration.
pub fn create_encryptor(context: Option<Arc<EncryptionContext>>) -> Box<dyn StorageEncryptor> {
    match context {
        Some(ctx) if ctx.is_enabled() => Box::new(TdeStorageEncryptor::new(ctx)),
        _ => Box::new(NoOpStorageEncryptor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_passthrough() {
        let enc = NoOpStorageEncryptor;
        assert!(!enc.is_enabled());
        let data = b"test data";
        assert_eq!(enc.encrypt_segment(data).unwrap(), data);
        assert_eq!(enc.decrypt_segment(data).unwrap(), data);
    }

    #[test]
    fn create_encryptor_noop_when_none() {
        let enc = create_encryptor(None);
        assert!(!enc.is_enabled());
    }

    #[test]
    fn create_encryptor_noop_when_disabled() {
        let ctx = Arc::new(EncryptionContext::disabled());
        let enc = create_encryptor(Some(ctx));
        assert!(!enc.is_enabled());
    }

    #[test]
    fn tde_encryptor_enabled() {
        use crate::crypto::FileKeyProvider;
        use crate::tde::TransparentDataEncryptionOptions;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("testkey"), [0x42u8; 16]).unwrap();

        let opts = TransparentDataEncryptionOptions {
            enabled: true,
            cipher: "AES/CBC/PKCS5Padding".to_string(),
            key_alias: Some("testkey".to_string()),
            key_length: 128,
            ..Default::default()
        };
        let kp = Arc::new(FileKeyProvider::new(dir.path()));
        let ctx = Arc::new(EncryptionContext::new(opts, Some(kp)));

        let enc = create_encryptor(Some(ctx));
        assert!(enc.is_enabled());

        let data = b"some commitlog data to encrypt";
        let encrypted = enc.encrypt_segment(data).unwrap();
        assert_ne!(encrypted, data);
    }
}
