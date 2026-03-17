// Licensed under Apache License, Version 2.0.

//! # Stream Receiver
//!
//! Accepts incoming stream data and writes SSTables to disk. Manages
//! in-progress receive buffers and handles the 3 incoming streaming verbs.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamReceiveTask`
//! - `org.apache.cassandra.db.streaming.CassandraStreamReceiver`
//! - `org.apache.cassandra.streaming.StreamDeserializingTask`

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tracing::{debug, warn};
use uuid::Uuid;

use cassandra_messaging::{Message, MessageHandler, MessagingService, Verb};
use cassandra_storage::sstable::{SSTableDescriptor, SSTableWriter};

use crate::manager::StreamManager;
use crate::metrics::StreamingMetrics;
use crate::protocol::*;
use crate::transport::{decompress, verify_chunk_checksum};

/// In-progress receive buffer for a single transfer.
#[derive(Debug)]
pub struct ReceiveBuffer {
    pub chunks: BTreeMap<u64, Vec<u8>>,
    pub total_bytes: u64,
    pub expected_last: Option<u64>,
    pub compressed: bool,
    pub checksum_algorithm: String,
    pub keyspace: String,
    pub table: String,
}

impl ReceiveBuffer {
    fn new() -> Self {
        Self {
            chunks: BTreeMap::new(),
            total_bytes: 0,
            expected_last: None,
            compressed: false,
            checksum_algorithm: "Crc32".into(),
            keyspace: String::new(),
            table: String::new(),
        }
    }

    fn is_complete(&self) -> bool {
        match self.expected_last {
            Some(last) => {
                self.chunks.len() as u64 == last + 1
                    && self.chunks.contains_key(&last)
            }
            None => false,
        }
    }

    fn assemble(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.total_bytes as usize);
        for (_seq, chunk) in &self.chunks {
            data.extend_from_slice(chunk);
        }
        data
    }
}

/// Receives streaming data and writes SSTables.
pub struct StreamReceiver {
    buffers: DashMap<(Uuid, Uuid), ReceiveBuffer>,
    manager: Arc<StreamManager>,
    metrics: Arc<StreamingMetrics>,
    data_dir: PathBuf,
}

impl StreamReceiver {
    pub fn new(
        manager: Arc<StreamManager>,
        metrics: Arc<StreamingMetrics>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            buffers: DashMap::new(),
            manager,
            metrics,
            data_dir: data_dir.into(),
        }
    }

    /// Register verb handlers on the messaging service.
    pub fn register_handlers(
        receiver: Arc<StreamReceiver>,
        messaging: &MessagingService,
    ) {
        let r1 = Arc::clone(&receiver);
        messaging.register_handler(
            Verb::StreamInit,
            Arc::new(move |msg| r1.handle_init(msg)) as MessageHandler,
        );

        let r2 = Arc::clone(&receiver);
        messaging.register_handler(
            Verb::StreamData,
            Arc::new(move |msg| r2.handle_data(msg)) as MessageHandler,
        );

        let r3 = Arc::clone(&receiver);
        messaging.register_handler(
            Verb::StreamComplete,
            Arc::new(move |msg| r3.handle_complete(msg)) as MessageHandler,
        );
    }

    fn handle_init(&self, msg: Message) -> Option<Message> {
        let msg_id = msg.header.message_id;
        let init = match StreamMessage::from_message(&msg) {
            Ok(StreamMessage::Init(init)) => init,
            _ => {
                warn!("failed to decode StreamInit");
                return None;
            }
        };

        debug!(session_id = %init.session_id, "received StreamInit");

        let resp = StreamMessage::InitResponse(StreamInitResponseMessage {
            session_id: init.session_id,
            accepted: true,
            reason: None,
        });
        resp.to_message(msg_id).ok()
    }

    fn handle_data(&self, msg: Message) -> Option<Message> {
        let msg_id = msg.header.message_id;
        let data = match StreamMessage::from_message(&msg) {
            Ok(StreamMessage::Data(d)) => d,
            _ => {
                warn!("failed to decode StreamData");
                return None;
            }
        };

        let key = (data.session_id, data.transfer_id);
        let seq = data.sequence;

        // Verify checksum
        if !verify_chunk_checksum(&data.data, &data.checksum, &data.checksum_algorithm) {
            self.metrics.record_checksum_failure();
            warn!(seq, "checksum verification failed");
            let resp = StreamMessage::DataResponse(StreamDataResponseMessage {
                session_id: data.session_id,
                transfer_id: data.transfer_id,
                sequence: seq,
                accepted: false,
                error: Some("checksum mismatch".into()),
            });
            return resp.to_message(msg_id).ok();
        }

        let chunk_len = data.data.len() as u64;

        // Insert chunk into buffer
        let mut entry = self.buffers.entry(key).or_insert_with(ReceiveBuffer::new);
        entry.compressed = data.compressed;
        entry.checksum_algorithm = data.checksum_algorithm.clone();
        if data.is_last {
            entry.expected_last = Some(seq);
        }
        entry.chunks.insert(seq, data.data);
        entry.total_bytes += chunk_len;

        self.metrics.record_bytes_received(chunk_len);
        self.metrics.record_chunk_received();

        // If transfer is complete, assemble and write
        if entry.is_complete() {
            let assembled = entry.assemble();
            let compressed = entry.compressed;
            drop(entry);

            if let Err(e) = self.write_sstable(key, &assembled, compressed) {
                warn!("failed to write SSTable: {e}");
                let resp = StreamMessage::DataResponse(StreamDataResponseMessage {
                    session_id: key.0,
                    transfer_id: key.1,
                    sequence: seq,
                    accepted: false,
                    error: Some(format!("write error: {e}")),
                });
                return resp.to_message(msg_id).ok();
            }

            self.buffers.remove(&key);
        }

        let resp = StreamMessage::DataResponse(StreamDataResponseMessage {
            session_id: key.0,
            transfer_id: key.1,
            sequence: seq,
            accepted: true,
            error: None,
        });
        resp.to_message(msg_id).ok()
    }

    fn write_sstable(
        &self,
        key: (Uuid, Uuid),
        data: &[u8],
        compressed: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = if compressed {
            decompress(data).map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?
        } else {
            data.to_vec()
        };

        let wire: WirePartitions = serde_json::from_slice(&payload)?;
        let partitions = wire.to_partitions();

        let ks_dir = self.data_dir.join(format!("stream_{}", key.0));
        std::fs::create_dir_all(&ks_dir)?;

        let descriptor = SSTableDescriptor::new(
            &ks_dir,
            &format!("stream_{}", key.0),
            &format!("transfer_{}", key.1),
            1,
        );
        let writer = SSTableWriter::new(descriptor);
        writer.write(&partitions)?;

        debug!(
            session = %key.0,
            transfer = %key.1,
            partitions = partitions.len(),
            "wrote received SSTable"
        );

        Ok(())
    }

    fn handle_complete(&self, msg: Message) -> Option<Message> {
        let msg_id = msg.header.message_id;
        let complete = match StreamMessage::from_message(&msg) {
            Ok(StreamMessage::Complete(c)) => c,
            _ => {
                warn!("failed to decode StreamComplete");
                return None;
            }
        };

        debug!(session_id = %complete.session_id, success = complete.success, "received StreamComplete");

        if complete.success {
            self.manager.complete_session(&complete.session_id);
        } else {
            let err = complete.error.unwrap_or_else(|| "unknown".into());
            self.manager.fail_session(&complete.session_id, err);
        }

        // Clean up any remaining buffers for this session
        self.buffers.retain(|k, _| k.0 != complete.session_id);

        let resp = StreamMessage::CompleteResponse(StreamCompleteResponseMessage {
            session_id: complete.session_id,
            acknowledged: true,
        });
        resp.to_message(msg_id).ok()
    }

    /// Number of in-progress transfer buffers.
    pub fn active_buffer_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::{ChecksumAlgorithm, ChunkChecksum};

    #[test]
    fn receive_buffer_single_chunk() {
        let mut buf = ReceiveBuffer::new();
        buf.expected_last = Some(0);
        buf.chunks.insert(0, vec![1, 2, 3]);
        buf.total_bytes = 3;
        assert!(buf.is_complete());
        assert_eq!(buf.assemble(), vec![1, 2, 3]);
    }

    #[test]
    fn receive_buffer_multi_chunk() {
        let mut buf = ReceiveBuffer::new();
        buf.chunks.insert(0, vec![1, 2]);
        buf.chunks.insert(1, vec![3, 4]);
        buf.expected_last = Some(1);
        buf.total_bytes = 4;
        assert!(buf.is_complete());
        assert_eq!(buf.assemble(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn receive_buffer_incomplete() {
        let mut buf = ReceiveBuffer::new();
        buf.chunks.insert(0, vec![1, 2]);
        // No expected_last set, so not complete
        assert!(!buf.is_complete());

        buf.expected_last = Some(2);
        // Missing chunks 1 and 2
        assert!(!buf.is_complete());
    }

    #[test]
    fn checksum_failure_detection() {
        let data = b"test payload";
        let checksum = ChunkChecksum::compute(data, ChecksumAlgorithm::Crc32);
        assert!(verify_chunk_checksum(data, &checksum.hash, "Crc32"));
        assert!(!verify_chunk_checksum(b"corrupted", &checksum.hash, "Crc32"));
    }

    #[test]
    fn receive_buffer_ordered_assembly() {
        let mut buf = ReceiveBuffer::new();
        // Insert out of order
        buf.chunks.insert(2, vec![5, 6]);
        buf.chunks.insert(0, vec![1, 2]);
        buf.chunks.insert(1, vec![3, 4]);
        buf.expected_last = Some(2);
        buf.total_bytes = 6;
        assert!(buf.is_complete());
        // BTreeMap ensures ordered assembly
        assert_eq!(buf.assemble(), vec![1, 2, 3, 4, 5, 6]);
    }
}
