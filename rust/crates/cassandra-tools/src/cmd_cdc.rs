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

//! CDC offline reader command.
//!
//! Supports reading completed or all CDC segments, optionally decoding
//! encrypted entries when TDE key material is provided.

use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cassandra_security::{
    EncryptionContext, FileKeyProvider, StorageEncryptor, TransparentDataEncryptionOptions,
    create_encryptor,
};
use cassandra_storage::cdc::{
    CdcFollowBatch, CdcFollowerConfig, CdcFollowerState, CdcReadCursor, cdc_follower_tick,
    cdc_follower_tick_with_codec, list_cdc_segment_infos, read_cdc_segment_mutations,
    read_cdc_segment_mutations_with_codec, read_completed_cdc_mutations_batch,
    read_completed_cdc_mutations_batch_with_codec, read_completed_cdc_mutations_follow_step,
    read_completed_cdc_mutations_follow_step_with_codec,
};
use cassandra_storage::commitlog::Mutation;
use cassandra_storage::commitlog::encrypted::{CommitLogEncryptor, EncryptingSegmentWriter};
use cassandra_storage::commitlog::segment::CorruptionPolicy;

struct StorageEncryptorAdapter {
    inner: Box<dyn StorageEncryptor>,
}

impl fmt::Debug for StorageEncryptorAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageEncryptorAdapter").finish()
    }
}

impl CommitLogEncryptor for StorageEncryptorAdapter {
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        self.inner.encrypt_segment(data).map_err(|e| e.to_string())
    }

    fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        self.inner.decrypt_segment(data).map_err(|e| e.to_string())
    }

    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
}

#[derive(Debug)]
enum CdcReadError {
    MissingKeyAlias,
    BuildCodec(String),
    BatchRead(String),
    InvalidBatchSize,
    BatchAllSegmentsUnsupported,
    CursorRequiresBatchSize,
    FollowRequiresBatchSize,
    InvalidPollInterval,
    InvalidMaxBatches,
    InvalidMaxConsecutiveNonEmptyBatches,
    BackpressureRequiresFollow,
    BackpressureRequiresThreshold,
    CursorRead { path: String, message: String },
    CursorWrite { path: String, message: String },
}

impl fmt::Display for CdcReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKeyAlias => write!(
                f,
                "--key-alias is required when --key-directory is provided"
            ),
            Self::BuildCodec(msg) => write!(f, "failed to build CDC decrypt codec: {msg}"),
            Self::BatchRead(msg) => write!(f, "failed to read CDC batch: {msg}"),
            Self::InvalidBatchSize => write!(f, "--batch-size must be greater than zero"),
            Self::BatchAllSegmentsUnsupported => write!(
                f,
                "--batch-size/--cursor-file currently support completed segments only (omit --all-segments)"
            ),
            Self::CursorRequiresBatchSize => {
                write!(f, "--cursor-file requires --batch-size")
            }
            Self::FollowRequiresBatchSize => {
                write!(f, "--follow requires --batch-size")
            }
            Self::InvalidPollInterval => {
                write!(f, "--poll-interval-ms must be greater than zero")
            }
            Self::InvalidMaxBatches => {
                write!(f, "--max-batches must be greater than zero when provided")
            }
            Self::InvalidMaxConsecutiveNonEmptyBatches => write!(
                f,
                "--max-consecutive-nonempty-batches must be greater than zero when provided"
            ),
            Self::BackpressureRequiresFollow => {
                write!(
                    f,
                    "--max-consecutive-nonempty-batches/--backpressure-sleep-ms require --follow"
                )
            }
            Self::BackpressureRequiresThreshold => write!(
                f,
                "--backpressure-sleep-ms requires --max-consecutive-nonempty-batches"
            ),
            Self::CursorRead { path, message } => {
                write!(f, "failed to read cursor file {}: {message}", path)
            }
            Self::CursorWrite { path, message } => {
                write!(f, "failed to update cursor file {}: {message}", path)
            }
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CursorFileState {
    segment_id: u64,
    mutation_index: usize,
}

fn load_cursor(path: &Path) -> Result<Option<CdcReadCursor>, CdcReadError> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path).map_err(|e| CdcReadError::CursorRead {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    let state: CursorFileState =
        serde_json::from_str(&contents).map_err(|e| CdcReadError::CursorRead {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
    Ok(Some(CdcReadCursor {
        segment_id: state.segment_id,
        mutation_index: state.mutation_index,
    }))
}

fn store_cursor(path: &Path, cursor: Option<&CdcReadCursor>) -> Result<(), CdcReadError> {
    if let Some(cursor) = cursor {
        let state = CursorFileState {
            segment_id: cursor.segment_id,
            mutation_index: cursor.mutation_index,
        };
        let payload = serde_json::to_vec_pretty(&state).map_err(|e| CdcReadError::CursorWrite {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        fs::write(path, payload).map_err(|e| CdcReadError::CursorWrite {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        return Ok(());
    }

    if path.exists() {
        fs::remove_file(path).map_err(|e| CdcReadError::CursorWrite {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
    }
    Ok(())
}

fn build_tde_codec(
    key_directory: &str,
    key_alias: Option<&str>,
    cipher: &str,
    chunk_length_kb: u32,
) -> Result<EncryptingSegmentWriter, CdcReadError> {
    let Some(alias) = key_alias else {
        return Err(CdcReadError::MissingKeyAlias);
    };

    let options = TransparentDataEncryptionOptions {
        enabled: true,
        chunk_length_kb,
        cipher: cipher.to_string(),
        key_alias: Some(alias.to_string()),
        ..Default::default()
    };
    let key_provider = Arc::new(FileKeyProvider::new(key_directory));
    let context = Arc::new(EncryptionContext::new(options, Some(key_provider)));
    let encryptor = create_encryptor(Some(context));
    if !encryptor.is_enabled() {
        return Err(CdcReadError::BuildCodec(
            "encryptor resolved as disabled".to_string(),
        ));
    }
    let adapter = StorageEncryptorAdapter { inner: encryptor };
    Ok(EncryptingSegmentWriter::new(Arc::new(adapter)))
}

fn print_mutation(mutation: &Mutation) {
    match serde_json::to_string(mutation) {
        Ok(line) => println!("{line}"),
        Err(e) => eprintln!("Failed to serialize mutation: {e}"),
    }
}

pub fn cdc_read(
    dir: &str,
    all_segments: bool,
    skip_corrupt: bool,
    key_directory: Option<&str>,
    key_alias: Option<&str>,
    cipher: &str,
    chunk_length_kb: u32,
    summary_only: bool,
    batch_size: Option<usize>,
    cursor_file: Option<&str>,
    follow: bool,
    poll_interval_ms: u64,
    max_batches: Option<usize>,
    max_consecutive_nonempty_batches: Option<usize>,
    backpressure_sleep_ms: u64,
) {
    if let Some(error) = validate_batch_mode(
        batch_size,
        cursor_file,
        all_segments,
        follow,
        poll_interval_ms,
        max_batches,
        max_consecutive_nonempty_batches,
        backpressure_sleep_ms,
    ) {
        eprintln!("Error: {error}");
        return;
    }

    if let Some(limit) = batch_size {
        if follow {
            cdc_read_batch_follow(
                dir,
                skip_corrupt,
                key_directory,
                key_alias,
                cipher,
                chunk_length_kb,
                summary_only,
                limit,
                cursor_file.map(Path::new),
                poll_interval_ms,
                max_batches,
                max_consecutive_nonempty_batches,
                backpressure_sleep_ms,
            );
        } else if let Err(e) = cdc_read_batch_once(
            dir,
            skip_corrupt,
            key_directory,
            key_alias,
            cipher,
            chunk_length_kb,
            summary_only,
            limit,
            cursor_file.map(Path::new),
            None,
            false,
        ) {
            eprintln!("Error: {e}");
        }
        return;
    }

    let path = Path::new(dir);
    let policy = if skip_corrupt {
        CorruptionPolicy::SkipAndContinue
    } else {
        CorruptionPolicy::StopOnCorrupt
    };

    let infos = match list_cdc_segment_infos(path) {
        Ok(infos) => infos,
        Err(e) => {
            eprintln!("Error listing CDC segments in {}: {e}", path.display());
            return;
        }
    };

    let selected: Vec<_> = infos
        .into_iter()
        .filter(|info| all_segments || info.completed)
        .collect();

    if selected.is_empty() {
        println!("No CDC segments matched the selection criteria.");
        return;
    }

    let mut total_mutations = 0usize;

    if let Some(key_dir) = key_directory {
        let codec = match build_tde_codec(key_dir, key_alias, cipher, chunk_length_kb) {
            Ok(codec) => codec,
            Err(e) => {
                eprintln!("Error: {e}");
                return;
            }
        };

        for info in &selected {
            let mutations = match read_cdc_segment_mutations_with_codec(&info.path, policy, &codec)
            {
                Ok(mutations) => mutations,
                Err(e) => {
                    eprintln!("Error reading CDC segment {}: {e}", info.path.display());
                    if !skip_corrupt {
                        return;
                    }
                    continue;
                }
            };
            total_mutations += mutations.len();
            println!(
                "segment={} completed={} durable_offset={:?} mutations={}",
                info.path.display(),
                info.completed,
                info.durable_offset,
                mutations.len()
            );
            if !summary_only {
                for mutation in &mutations {
                    print_mutation(mutation);
                }
            }
        }
    } else {
        for info in &selected {
            let mutations = match read_cdc_segment_mutations(&info.path, policy) {
                Ok(mutations) => mutations,
                Err(e) => {
                    eprintln!("Error reading CDC segment {}: {e}", info.path.display());
                    if !skip_corrupt {
                        return;
                    }
                    continue;
                }
            };
            total_mutations += mutations.len();
            println!(
                "segment={} completed={} durable_offset={:?} mutations={}",
                info.path.display(),
                info.completed,
                info.durable_offset,
                mutations.len()
            );
            if !summary_only {
                for mutation in &mutations {
                    print_mutation(mutation);
                }
            }
        }
    }

    println!(
        "CDC read complete: segments={} total_mutations={}",
        selected.len(),
        total_mutations
    );
}

fn validate_batch_mode(
    batch_size: Option<usize>,
    cursor_file: Option<&str>,
    all_segments: bool,
    follow: bool,
    poll_interval_ms: u64,
    max_batches: Option<usize>,
    max_consecutive_nonempty_batches: Option<usize>,
    backpressure_sleep_ms: u64,
) -> Option<CdcReadError> {
    if batch_size == Some(0) {
        return Some(CdcReadError::InvalidBatchSize);
    }
    if cursor_file.is_some() && batch_size.is_none() {
        return Some(CdcReadError::CursorRequiresBatchSize);
    }
    if batch_size.is_some() && all_segments {
        return Some(CdcReadError::BatchAllSegmentsUnsupported);
    }
    if follow && batch_size.is_none() {
        return Some(CdcReadError::FollowRequiresBatchSize);
    }
    if follow && poll_interval_ms == 0 {
        return Some(CdcReadError::InvalidPollInterval);
    }
    if max_batches == Some(0) {
        return Some(CdcReadError::InvalidMaxBatches);
    }
    if max_consecutive_nonempty_batches == Some(0) {
        return Some(CdcReadError::InvalidMaxConsecutiveNonEmptyBatches);
    }
    if !follow && (max_consecutive_nonempty_batches.is_some() || backpressure_sleep_ms > 0) {
        return Some(CdcReadError::BackpressureRequiresFollow);
    }
    if follow && backpressure_sleep_ms > 0 && max_consecutive_nonempty_batches.is_none() {
        return Some(CdcReadError::BackpressureRequiresThreshold);
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn cdc_read_batch_once(
    dir: &str,
    skip_corrupt: bool,
    key_directory: Option<&str>,
    key_alias: Option<&str>,
    cipher: &str,
    chunk_length_kb: u32,
    summary_only: bool,
    limit: usize,
    cursor_file: Option<&Path>,
    cursor_override: Option<CdcReadCursor>,
    retain_terminal_cursor: bool,
) -> Result<(), CdcReadError> {
    let path = Path::new(dir);
    let policy = if skip_corrupt {
        CorruptionPolicy::SkipAndContinue
    } else {
        CorruptionPolicy::StopOnCorrupt
    };

    let cursor = match cursor_override {
        Some(cursor) => Some(cursor),
        None => match cursor_file {
            Some(cursor_path) => load_cursor(cursor_path)?,
            None => None,
        },
    };

    let (mutations, exhausted, next_cursor) = if let Some(key_dir) = key_directory {
        let codec = build_tde_codec(key_dir, key_alias, cipher, chunk_length_kb)?;
        if retain_terminal_cursor {
            let step = read_completed_cdc_mutations_follow_step_with_codec(
                path,
                policy,
                limit,
                cursor.as_ref(),
                &codec,
            )
            .map_err(|e| CdcReadError::BatchRead(e.to_string()))?;
            (step.mutations, step.exhausted, step.next_cursor)
        } else {
            let batch = read_completed_cdc_mutations_batch_with_codec(
                path,
                policy,
                limit,
                cursor.as_ref(),
                &codec,
            )
            .map_err(|e| CdcReadError::BatchRead(e.to_string()))?;
            let exhausted = batch.next_cursor.is_none();
            (batch.mutations, exhausted, batch.next_cursor)
        }
    } else {
        if retain_terminal_cursor {
            let step =
                read_completed_cdc_mutations_follow_step(path, policy, limit, cursor.as_ref())
                    .map_err(|e| CdcReadError::BatchRead(e.to_string()))?;
            (step.mutations, step.exhausted, step.next_cursor)
        } else {
            let batch = read_completed_cdc_mutations_batch(path, policy, limit, cursor.as_ref())
                .map_err(|e| CdcReadError::BatchRead(e.to_string()))?;
            let exhausted = batch.next_cursor.is_none();
            (batch.mutations, exhausted, batch.next_cursor)
        }
    };

    if !summary_only {
        for mutation in &mutations {
            print_mutation(mutation);
        }
    }

    if let Some(cursor_path) = cursor_file {
        store_cursor(cursor_path, next_cursor.as_ref())?;
    }

    println!(
        "CDC batch read complete: mutations={} exhausted={}",
        mutations.len(),
        exhausted
    );
    if let Some(next) = next_cursor.as_ref() {
        println!(
            "next_cursor=segment:{} mutation_index:{}",
            next.segment_id, next.mutation_index
        );
    } else {
        println!("next_cursor=<none>");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cdc_read_batch_follow(
    dir: &str,
    skip_corrupt: bool,
    key_directory: Option<&str>,
    key_alias: Option<&str>,
    cipher: &str,
    chunk_length_kb: u32,
    summary_only: bool,
    limit: usize,
    cursor_file: Option<&Path>,
    poll_interval_ms: u64,
    max_batches: Option<usize>,
    max_consecutive_nonempty_batches: Option<usize>,
    backpressure_sleep_ms: u64,
) {
    let mut batches = 0usize;
    let mut cursor_state = match cursor_file {
        Some(path) => match load_cursor(path) {
            Ok(cursor) => cursor,
            Err(e) => {
                eprintln!("Error: {e}");
                return;
            }
        },
        None => None,
    };
    let mut state = CdcFollowerState {
        cursor: cursor_state.take(),
        consecutive_nonempty_batches: 0,
    };
    let config = CdcFollowerConfig {
        batch_size: limit,
        max_consecutive_nonempty_batches,
    };
    let path = Path::new(dir);
    let policy = if skip_corrupt {
        CorruptionPolicy::SkipAndContinue
    } else {
        CorruptionPolicy::StopOnCorrupt
    };
    let codec = match key_directory {
        Some(key_dir) => match build_tde_codec(key_dir, key_alias, cipher, chunk_length_kb) {
            Ok(codec) => Some(codec),
            Err(e) => {
                eprintln!("Error: {e}");
                return;
            }
        },
        None => None,
    };

    loop {
        let tick = match codec.as_ref() {
            Some(codec) => cdc_follower_tick_with_codec(path, policy, &state, &config, codec),
            None => cdc_follower_tick(path, policy, &state, &config),
        }
        .map_err(|e| CdcReadError::BatchRead(e.to_string()));

        let tick = match tick {
            Ok(tick) => tick,
            Err(e) => {
                eprintln!("Error: {e}");
                return;
            }
        };

        print_follow_tick(&tick.batch, summary_only);
        if let Some(cursor_path) = cursor_file
            && let Err(e) = store_cursor(cursor_path, tick.next_state.cursor.as_ref())
        {
            eprintln!("Error: {e}");
            return;
        }
        if tick.backpressure_recommended && backpressure_sleep_ms > 0 {
            println!(
                "CDC follow backpressure: consecutive_nonempty_batches={} sleep_ms={}",
                max_consecutive_nonempty_batches.unwrap_or_default(),
                backpressure_sleep_ms
            );
            thread::sleep(Duration::from_millis(backpressure_sleep_ms));
        }

        state = tick.next_state;
        batches += 1;
        if max_batches.is_some_and(|max| batches >= max) {
            println!("CDC follow mode finished: batches={batches}");
            return;
        }
        thread::sleep(Duration::from_millis(poll_interval_ms));
    }
}

fn print_follow_tick(batch: &CdcFollowBatch, summary_only: bool) {
    if !summary_only {
        for mutation in &batch.mutations {
            print_mutation(mutation);
        }
    }
    println!(
        "CDC batch read complete: mutations={} exhausted={}",
        batch.mutations.len(),
        batch.exhausted
    );
    if let Some(next) = batch.next_cursor.as_ref() {
        println!(
            "next_cursor=segment:{} mutation_index:{}",
            next.segment_id, next.mutation_index
        );
    } else {
        println!("next_cursor=<none>");
    }
}
