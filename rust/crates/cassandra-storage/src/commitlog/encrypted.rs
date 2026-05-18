// Licensed under Apache License, Version 2.0.

//! Encrypted commit log segment support.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.commitlog.EncryptedSegment`
//!
//! This module provides the commitlog-side encryption adapter used to wrap
//! serialized commitlog payload blocks before they are written to a segment and
//! unwrap them during replay.

use std::sync::Arc;

/// Storage-level encryptor used by commitlog segment codecs.
pub trait CommitLogEncryptor: Send + Sync + std::fmt::Debug {
    /// Encrypt a serialized commitlog payload block.
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String>;

    /// Decrypt a serialized commitlog payload block.
    fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String>;

    /// Whether this encryptor transforms bytes.
    fn is_enabled(&self) -> bool;
}

/// Codec used by commitlog segment writers/readers.
pub trait EncryptedSegmentWriter {
    /// Whether this writer is performing encryption.
    fn is_encrypted(&self) -> bool;

    /// Transform plaintext bytes before writing to a segment.
    fn encode_block(&self, plaintext: &[u8]) -> Result<Vec<u8>, String>;

    /// Transform bytes read from a segment back into plaintext.
    fn decode_block(&self, ciphertext: &[u8]) -> Result<Vec<u8>, String>;
}

/// Pass-through writer for unencrypted segments.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlainSegmentWriter;

impl EncryptedSegmentWriter for PlainSegmentWriter {
    fn is_encrypted(&self) -> bool {
        false
    }

    fn encode_block(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        Ok(plaintext.to_vec())
    }

    fn decode_block(&self, ciphertext: &[u8]) -> Result<Vec<u8>, String> {
        Ok(ciphertext.to_vec())
    }
}

/// Segment writer backed by a configured storage encryptor.
#[derive(Debug, Clone)]
pub struct EncryptingSegmentWriter {
    encryptor: Arc<dyn CommitLogEncryptor>,
}

impl EncryptingSegmentWriter {
    pub fn new(encryptor: Arc<dyn CommitLogEncryptor>) -> Self {
        Self { encryptor }
    }
}

impl EncryptedSegmentWriter for EncryptingSegmentWriter {
    fn is_encrypted(&self) -> bool {
        self.encryptor.is_enabled()
    }

    fn encode_block(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        self.encryptor.encrypt_segment(plaintext)
    }

    fn decode_block(&self, ciphertext: &[u8]) -> Result<Vec<u8>, String> {
        self.encryptor.decrypt_segment(ciphertext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct XorEncryptor(u8);

    impl CommitLogEncryptor for XorEncryptor {
        fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            Ok(data.iter().map(|byte| byte ^ self.0).collect())
        }

        fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            self.encrypt_segment(data)
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[test]
    fn plain_writer_round_trips_without_transforming() {
        let writer = PlainSegmentWriter;
        let data = b"commitlog-entry";
        assert!(!writer.is_encrypted());
        assert_eq!(writer.encode_block(data).unwrap(), data);
        assert_eq!(writer.decode_block(data).unwrap(), data);
    }

    #[test]
    fn encrypting_writer_round_trips_payload() {
        let writer = EncryptingSegmentWriter::new(Arc::new(XorEncryptor(0x5a)));
        let data = b"commitlog-entry";

        let encoded = writer.encode_block(data).unwrap();
        assert!(writer.is_encrypted());
        assert_ne!(encoded, data);
        assert_eq!(writer.decode_block(&encoded).unwrap(), data);
    }
}
