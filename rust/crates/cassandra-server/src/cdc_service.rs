// Licensed under Apache License, Version 2.0.

//! Background CDC follower service wiring for server runtime.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cassandra_storage::cdc::{
    CdcFollowerConfig, CdcFollowerState, cdc_follower_tick, cdc_follower_tick_with_codec,
};
use cassandra_storage::commitlog::encrypted::{CommitLogEncryptor, EncryptingSegmentWriter};
use cassandra_storage::commitlog::segment::CorruptionPolicy;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::shutdown::ShutdownCoordinator;

#[derive(Debug, Clone)]
pub struct CdcFollowerServiceConfig {
    pub enabled: bool,
    pub raw_directory: PathBuf,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub max_consecutive_nonempty_batches: Option<usize>,
    pub backpressure_sleep: Duration,
}

impl CdcFollowerServiceConfig {
    pub fn from_runtime(enabled: bool, raw_directory: PathBuf) -> Self {
        Self {
            enabled,
            raw_directory,
            poll_interval: Duration::from_millis(1000),
            batch_size: 128,
            max_consecutive_nonempty_batches: Some(16),
            backpressure_sleep: Duration::from_millis(10),
        }
    }
}

pub fn spawn_cdc_follower_service(
    config: CdcFollowerServiceConfig,
    shutdown: Arc<ShutdownCoordinator>,
    encryptor: Option<Arc<dyn CommitLogEncryptor>>,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        return None;
    }

    let cancel = shutdown.token();
    Some(tokio::spawn(async move {
        let mut state = CdcFollowerState::default();
        let follower_config = CdcFollowerConfig {
            batch_size: config.batch_size,
            max_consecutive_nonempty_batches: config.max_consecutive_nonempty_batches,
        };
        let codec = encryptor.map(EncryptingSegmentWriter::new);

        info!(
            path = %config.raw_directory.display(),
            batch_size = config.batch_size,
            poll_interval_ms = config.poll_interval.as_millis() as u64,
            "CDC follower service started"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(config.poll_interval) => {
                    let tick = match codec.as_ref() {
                        Some(codec) => cdc_follower_tick_with_codec(
                            &config.raw_directory,
                            CorruptionPolicy::SkipAndContinue,
                            &state,
                            &follower_config,
                            codec,
                        ),
                        None => cdc_follower_tick(
                            &config.raw_directory,
                            CorruptionPolicy::SkipAndContinue,
                            &state,
                            &follower_config,
                        ),
                    };

                    let tick = match tick {
                        Ok(tick) => tick,
                        Err(e) => {
                            warn!(error = %e, "CDC follower tick failed");
                            continue;
                        }
                    };

                    if !tick.batch.mutations.is_empty() {
                        info!(
                            mutations = tick.batch.mutations.len(),
                            exhausted = tick.batch.exhausted,
                            "CDC follower consumed mutations"
                        );
                    }

                    if tick.backpressure_recommended && !config.backpressure_sleep.is_zero() {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = tokio::time::sleep(config.backpressure_sleep) => {}
                        }
                    }

                    state = tick.next_state;
                }
            }
        }

        info!("CDC follower service stopped");
    }))
}
