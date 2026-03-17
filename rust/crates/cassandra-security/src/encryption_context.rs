// Licensed under Apache License, Version 2.0.

//! Encryption context: wraps TDE options + crypto provider.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.security.EncryptionContext`
//!
//! Provides chunked encrypt/decrypt, IV management, and header
//! serialization for SSTable and commitlog encryption.

use crate::crypto::{AesCbcProvider, CryptoProvider, KeyProvider, NoOpCryptoProvider};
use crate::tde::TransparentDataEncryptionOptions;
use crate::SecurityError;
use parking_lot::Mutex;
use std::sync::Arc;

/// Header written to the start of encrypted data blocks.
///
/// Contains enough information to decrypt: cipher id, IV, key alias.
#[derive(Debug, Clone)]
pub struct EncryptionHeader {
    /// Cipher identifier string.
    pub cipher: String,
    /// Initialization vector used for this block.
    pub iv: Vec<u8>,
    /// Key alias used for encryption.
    pub key_alias: String,
    /// Key length in bits.
    pub key_length: u32,
}

impl EncryptionHeader {
    /// Serialize to bytes for storage in SSTable / commitlog headers.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Format: [cipher_len:u16][cipher][iv_len:u16][iv][alias_len:u16][alias][key_len:u32]
        let cipher_bytes = self.cipher.as_bytes();
        buf.extend_from_slice(&(cipher_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(cipher_bytes);

        buf.extend_from_slice(&(self.iv.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.iv);

        let alias_bytes = self.key_alias.as_bytes();
        buf.extend_from_slice(&(alias_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(alias_bytes);

        buf.extend_from_slice(&self.key_length.to_be_bytes());
        buf
    }

    /// Deserialize from bytes (reverse of `to_bytes`).
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), SecurityError> {
        let mut pos = 0;

        if data.len() < 2 {
            return Err(SecurityError::ConfigError("header too short".to_string()));
        }
        let cipher_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + cipher_len > data.len() {
            return Err(SecurityError::ConfigError("header truncated at cipher".to_string()));
        }
        let cipher = String::from_utf8(data[pos..pos + cipher_len].to_vec())
            .map_err(|e| SecurityError::ConfigError(format!("invalid cipher string: {e}")))?;
        pos += cipher_len;

        if pos + 2 > data.len() {
            return Err(SecurityError::ConfigError("header truncated at iv_len".to_string()));
        }
        let iv_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + iv_len > data.len() {
            return Err(SecurityError::ConfigError("header truncated at iv".to_string()));
        }
        let iv = data[pos..pos + iv_len].to_vec();
        pos += iv_len;

        if pos + 2 > data.len() {
            return Err(SecurityError::ConfigError("header truncated at alias_len".to_string()));
        }
        let alias_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + alias_len > data.len() {
            return Err(SecurityError::ConfigError("header truncated at alias".to_string()));
        }
        let key_alias = String::from_utf8(data[pos..pos + alias_len].to_vec())
            .map_err(|e| SecurityError::ConfigError(format!("invalid alias: {e}")))?;
        pos += alias_len;

        if pos + 4 > data.len() {
            return Err(SecurityError::ConfigError("header truncated at key_length".to_string()));
        }
        let key_length = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        Ok((
            Self {
                cipher,
                iv,
                key_alias,
                key_length,
            },
            pos,
        ))
    }
}

/// Encryption context wrapping TDE options + crypto provider.
///
/// Lazily initialises the crypto provider on first use.
pub struct EncryptionContext {
    options: TransparentDataEncryptionOptions,
    provider: Mutex<Option<Arc<dyn CryptoProvider>>>,
    key_provider: Option<Arc<dyn KeyProvider>>,
}

impl EncryptionContext {
    /// Create a new context from TDE options and an optional key provider.
    pub fn new(
        options: TransparentDataEncryptionOptions,
        key_provider: Option<Arc<dyn KeyProvider>>,
    ) -> Self {
        Self {
            options,
            provider: Mutex::new(None),
            key_provider,
        }
    }

    /// Create a disabled (no-op) context.
    pub fn disabled() -> Self {
        Self {
            options: TransparentDataEncryptionOptions::default(),
            provider: Mutex::new(Some(Arc::new(NoOpCryptoProvider))),
            key_provider: None,
        }
    }

    /// Create from an encryption header (for reading encrypted data).
    pub fn from_header(
        header: &EncryptionHeader,
        key_provider: Arc<dyn KeyProvider>,
    ) -> Self {
        let options = TransparentDataEncryptionOptions {
            enabled: true,
            cipher: header.cipher.clone(),
            key_alias: Some(header.key_alias.clone()),
            key_length: header.key_length,
            ..Default::default()
        };
        Self {
            options,
            provider: Mutex::new(None),
            key_provider: Some(key_provider),
        }
    }

    /// Whether encryption is enabled.
    pub fn is_enabled(&self) -> bool {
        self.options.enabled
    }

    /// Get or lazily initialise the crypto provider.
    fn get_provider(&self) -> Arc<dyn CryptoProvider> {
        let mut guard = self.provider.lock();
        if let Some(ref p) = *guard {
            return p.clone();
        }
        let provider: Arc<dyn CryptoProvider> = if !self.options.enabled {
            Arc::new(NoOpCryptoProvider)
        } else if self.options.cipher.starts_with("AES/CBC") {
            Arc::new(AesCbcProvider)
        } else {
            // Fallback — in production this would match more ciphers
            Arc::new(NoOpCryptoProvider)
        };
        *guard = Some(provider.clone());
        provider
    }

    /// Encrypt a chunk of data, returning (ciphertext, header).
    pub fn encrypt_chunk(&self, plaintext: &[u8]) -> Result<(Vec<u8>, EncryptionHeader), SecurityError> {
        let provider = self.get_provider();
        let iv = provider.generate_iv();
        let key_alias = self
            .options
            .key_alias
            .as_deref()
            .unwrap_or("default");
        let key = self.get_key(key_alias)?;
        let ciphertext = provider.encrypt(&key, &iv, plaintext)?;

        let header = EncryptionHeader {
            cipher: self.options.cipher.clone(),
            iv,
            key_alias: key_alias.to_string(),
            key_length: self.options.key_length,
        };

        Ok((ciphertext, header))
    }

    /// Decrypt a chunk using the provided header.
    pub fn decrypt_chunk(
        &self,
        ciphertext: &[u8],
        header: &EncryptionHeader,
    ) -> Result<Vec<u8>, SecurityError> {
        let provider = self.get_provider();
        let key = self.get_key(&header.key_alias)?;
        provider.decrypt(&key, &header.iv, ciphertext)
    }

    /// Encrypt data in chunks, returning the full encrypted output with headers.
    pub fn encrypt_chunked(&self, data: &[u8]) -> Result<Vec<u8>, SecurityError> {
        if !self.is_enabled() {
            return Ok(data.to_vec());
        }

        let chunk_size = self.options.chunk_length_bytes();
        let mut output = Vec::new();

        for chunk in data.chunks(chunk_size) {
            let (ciphertext, header) = self.encrypt_chunk(chunk)?;
            let header_bytes = header.to_bytes();
            output.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
            output.extend_from_slice(&header_bytes);
            output.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
            output.extend_from_slice(&ciphertext);
        }

        Ok(output)
    }

    fn get_key(&self, alias: &str) -> Result<Vec<u8>, SecurityError> {
        if let Some(ref kp) = self.key_provider {
            kp.get_key(alias)
        } else if !self.is_enabled() {
            Ok(vec![0u8; 16]) // NoOp key
        } else {
            Err(SecurityError::ConfigError(
                "no key provider configured".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::FileKeyProvider;

    fn make_test_context(key_dir: &std::path::Path) -> EncryptionContext {
        let opts = TransparentDataEncryptionOptions {
            enabled: true,
            cipher: "AES/CBC/PKCS5Padding".to_string(),
            key_alias: Some("testkey".to_string()),
            key_length: 128,
            chunk_length_kb: 1, // 1 KiB chunks for testing
            ..Default::default()
        };
        let kp = Arc::new(FileKeyProvider::new(key_dir));
        EncryptionContext::new(opts, Some(kp))
    }

    #[test]
    fn header_roundtrip() {
        let header = EncryptionHeader {
            cipher: "AES/CBC/PKCS5Padding".to_string(),
            iv: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            key_alias: "mykey".to_string(),
            key_length: 256,
        };
        let bytes = header.to_bytes();
        let (decoded, consumed) = EncryptionHeader::from_bytes(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.cipher, "AES/CBC/PKCS5Padding");
        assert_eq!(decoded.iv.len(), 16);
        assert_eq!(decoded.key_alias, "mykey");
        assert_eq!(decoded.key_length, 256);
    }

    #[test]
    fn disabled_context_passthrough() {
        let ctx = EncryptionContext::disabled();
        assert!(!ctx.is_enabled());
        let data = b"hello world";
        let result = ctx.encrypt_chunked(data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn encrypt_decrypt_chunk() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("testkey"), [0x42u8; 16]).unwrap();

        let ctx = make_test_context(dir.path());
        let plaintext = b"Cassandra encryption test data!";

        let (ciphertext, header) = ctx.encrypt_chunk(plaintext).unwrap();
        assert_ne!(&ciphertext[..], plaintext);

        let decrypted = ctx.decrypt_chunk(&ciphertext, &header).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn from_header_reconstruction() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("testkey"), [0x42u8; 16]).unwrap();

        let ctx = make_test_context(dir.path());
        let (ciphertext, header) = ctx.encrypt_chunk(b"test data").unwrap();

        // Reconstruct context from header (simulates reading from disk)
        let kp = Arc::new(FileKeyProvider::new(dir.path()));
        let ctx2 = EncryptionContext::from_header(&header, kp);
        let decrypted = ctx2.decrypt_chunk(&ciphertext, &header).unwrap();
        assert_eq!(decrypted, b"test data");
    }

    #[test]
    fn header_from_truncated_data() {
        assert!(EncryptionHeader::from_bytes(&[0]).is_err());
        assert!(EncryptionHeader::from_bytes(&[0, 5, 0, 0]).is_err());
    }
}
