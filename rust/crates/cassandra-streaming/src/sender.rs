// Licensed under Apache License, Version 2.0.

//! # Stream Sender
//!
//! Reads SSTables and sends data chunks over the network via [`StreamTransport`].
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.async.StreamingInboundHandler`
//! - `org.apache.cassandra.db.streaming.CassandraOutgoingFile`

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use cassandra_storage::sstable::{SSTableDescriptor, SSTableReader};

use crate::metrics::StreamingMetrics;
use crate::protocol::{StreamDataMessage, WirePartitions};
use crate::session::StreamSession;
use crate::transfer::{StreamTransfer, TransferState};
use crate::transport::{StreamTransport, StreamTransportError, compress};

/// Result of sending a single SSTable.
#[derive(Debug, Clone)]
pub struct SendResult {
    pub bytes_sent: u64,
    pub chunks_sent: u64,
    pub duration: Duration,
}

/// Errors from the stream sender.
#[derive(Debug, thiserror::Error)]
pub enum StreamSenderError {
    #[error("I/O error reading SSTable: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("transport error: {0}")]
    Transport(#[from] StreamTransportError),

    #[error("transfer failed after {retries} retries: {message}")]
    MaxRetriesExceeded { retries: u32, message: String },
}

/// Sends SSTable data to a remote peer.
pub struct StreamSender;

impl StreamSender {
    /// Send a single SSTable as chunked data over the transport.
    pub async fn send_sstable(
        transport: &StreamTransport,
        peer: SocketAddr,
        session_id: uuid::Uuid,
        transfer: &mut StreamTransfer,
        descriptor: &SSTableDescriptor,
        metrics: &Arc<StreamingMetrics>,
        use_compression: bool,
    ) -> Result<SendResult, StreamSenderError> {
        let start = Instant::now();
        transfer.state = TransferState::InProgress;

        let reader = SSTableReader::open(descriptor.clone())?;
        let partitions = reader.iter_partitions()?;
        let wire = WirePartitions::from_partitions(&partitions);
        let serialized = serde_json::to_vec(&wire)?;

        let data = if use_compression {
            compress(&serialized)
        } else {
            serialized
        };

        transfer.total_bytes = data.len() as u64;

        let chunks = StreamTransfer::chunkify_with_algorithm(
            transfer.id,
            &data,
            transfer.chunk_size,
            transfer.checksum_algorithm,
        );

        let total_chunks = chunks.len() as u64;
        for chunk in chunks {
            let seq = chunk.sequence;
            let chunk_len = chunk.data.len() as u64;
            let is_last = chunk.is_last;

            let msg = StreamDataMessage {
                session_id,
                transfer_id: transfer.id,
                sequence: seq,
                data: chunk.data,
                checksum: chunk.checksum.hash,
                checksum_algorithm: chunk.checksum.algorithm.to_string(),
                compressed: use_compression,
                is_last,
            };

            let mut last_err = None;
            let mut sent = false;
            for _attempt in 0..=3 {
                match transport.send_data(peer, msg.clone()).await {
                    Ok(()) => {
                        sent = true;
                        break;
                    }
                    Err(e) => {
                        if transfer.can_retry() {
                            transfer.record_retry();
                            metrics.record_retry();
                            warn!(seq, "chunk send failed, retrying: {e}");
                            last_err = Some(e);
                        } else {
                            return Err(StreamSenderError::MaxRetriesExceeded {
                                retries: transfer.retry_count,
                                message: e.to_string(),
                            });
                        }
                    }
                }
            }
            if !sent {
                return Err(StreamSenderError::MaxRetriesExceeded {
                    retries: transfer.retry_count,
                    message: last_err
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                });
            }

            transfer.record_progress(chunk_len);
            metrics.record_bytes_sent(chunk_len);
            metrics.record_chunk_sent();
        }

        transfer.state = TransferState::Complete;
        let duration = start.elapsed();

        debug!(
            bytes = transfer.bytes_transferred,
            chunks = total_chunks,
            ?duration,
            "SSTable transfer complete"
        );

        Ok(SendResult {
            bytes_sent: transfer.bytes_transferred,
            chunks_sent: total_chunks,
            duration,
        })
    }

    /// Send all outgoing transfers for a session.
    pub async fn send_session_outgoing(
        transport: &StreamTransport,
        session: &mut StreamSession,
        data_dir: &Path,
        metrics: &Arc<StreamingMetrics>,
        use_compression: bool,
    ) -> Result<(), StreamSenderError> {
        let session_id = session.id;
        let transfer_ids: Vec<_> = session.outgoing.keys().copied().collect();

        for transfer_id in transfer_ids {
            let transfer = session.outgoing.get(&transfer_id).unwrap();
            let descriptor = SSTableDescriptor::new(
                &data_dir.join(&transfer.keyspace).join(&transfer.table),
                &transfer.keyspace,
                &transfer.table,
                1,
            );

            let transfer = session.outgoing.get_mut(&transfer_id).unwrap();
            Self::send_sstable(
                transport,
                session.peer.addr(),
                session_id,
                transfer,
                &descriptor,
                metrics,
                use_compression,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::WirePartitions;
    use crate::transfer::{ChecksumAlgorithm, StreamTransfer};
    use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};

    #[test]
    fn partition_serialization_round_trip() {
        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "col1".into(),
                value: Some(b"val1".to_vec()),
                timestamp: 1000,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let partitions: Vec<(Vec<u8>, PartitionData)> = vec![(b"pk1".to_vec(), pd)];
        let wire = WirePartitions::from_partitions(&partitions);
        let serialized = serde_json::to_vec(&wire).unwrap();
        let deserialized: WirePartitions = serde_json::from_slice(&serialized).unwrap();
        let round_tripped = deserialized.to_partitions();
        assert_eq!(partitions.len(), round_tripped.len());
        assert_eq!(partitions[0].0, round_tripped[0].0);
    }

    #[test]
    fn chunked_send_progress_tracking() {
        let data = vec![0u8; 200];
        let mut transfer = StreamTransfer::new("ks".into(), "tbl".into(), vec![]);
        transfer.chunk_size = 64;
        transfer.total_bytes = data.len() as u64;

        let chunks = StreamTransfer::chunkify_with_algorithm(
            transfer.id,
            &data,
            transfer.chunk_size,
            ChecksumAlgorithm::Crc32,
        );

        assert_eq!(chunks.len(), 4); // 200 / 64 = 3 full + 1 partial (8 bytes)
        assert!(chunks.last().unwrap().is_last);

        for chunk in &chunks {
            transfer.record_progress(chunk.data.len() as u64);
        }
        assert_eq!(transfer.bytes_transferred, 200);
        assert_eq!(transfer.chunks_transferred, 4);
    }

    #[test]
    fn retry_on_failure() {
        let mut transfer = StreamTransfer::new("ks".into(), "t".into(), vec![]);
        assert!(transfer.can_retry());
        transfer.record_retry();
        assert!(transfer.can_retry());
        transfer.record_retry();
        assert!(transfer.can_retry());
        transfer.record_retry();
        assert!(!transfer.can_retry());
    }
}
