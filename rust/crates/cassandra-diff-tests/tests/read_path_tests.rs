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

//! Read path differential tests (Prompt 14, WU-16).
//!
//! Tests read path components: error codes, short-read protection,
//! paging state round-trip, tombstone thresholds, speculative retry
//! policy, and digest resolution.

use std::collections::HashMap;

use cassandra_coordinator::consistency::ConsistencyLevel;
use cassandra_coordinator::read::{
    DataResponse, Digest, DigestResolver, PagingState, ReadError, ShortReadProtection,
    SpeculativeRetryPolicy, TombstoneThresholds, TombstoneTracker,
};

// ═══════════════════════════════════════════════════════════════════
// Read Error Code Mapping
// ═══════════════════════════════════════════════════════════════════

#[test]
fn read_timeout_error_code() {
    let err = ReadError::Timeout {
        cl: ConsistencyLevel::Quorum,
        required: 2,
        received: 1,
        data_present: false,
    };
    assert_eq!(err.error_code(), 0x1200);
}

#[test]
fn read_failure_error_code() {
    let err = ReadError::ReadFailure {
        cl: ConsistencyLevel::One,
        required: 1,
        received: 0,
        num_failures: 1,
        data_present: false,
        failure_map: HashMap::new(),
    };
    assert_eq!(err.error_code(), 0x1300);
}

#[test]
fn unavailable_error_code() {
    let err = ReadError::Unavailable {
        cl: ConsistencyLevel::One,
        required: 1,
        alive: 0,
    };
    assert_eq!(err.error_code(), 0x1000);
}

#[test]
fn tombstone_overwhelming_error_code() {
    let err = ReadError::TombstoneOverwhelming {
        count: 100_000,
        threshold: 100_000,
    };
    assert_eq!(err.error_code(), 0x1300);
}

#[test]
fn internal_error_code() {
    assert_eq!(ReadError::Internal("test".into()).error_code(), 0x0000);
}

#[test]
fn digest_mismatch_error_code() {
    let err = ReadError::DigestMismatch {
        replicas_mismatched: 2,
    };
    // DigestMismatch maps to 0x1200 (ReadTimeout) since it's retried internally
    assert_eq!(err.error_code(), 0x1200);
}

// ═══════════════════════════════════════════════════════════════════
// Short-Read Protection
// ═══════════════════════════════════════════════════════════════════

#[test]
fn short_read_retry_with_tombstones() {
    let mut srp = ShortReadProtection::new();
    assert!(srp.needs_retry(100, 80, 20));
    srp.record_retry();
    assert_eq!(srp.retries, 1);
    let retry = srp.retry_bounds(b"last_ck", 20);
    assert_eq!(retry.start_after, b"last_ck");
    assert_eq!(retry.adjusted_limit, 20);
}

#[test]
fn short_read_no_retry_without_tombstones() {
    let srp = ShortReadProtection::new();
    assert!(!srp.needs_retry(100, 50, 0));
}

#[test]
fn short_read_no_retry_when_satisfied() {
    let srp = ShortReadProtection::new();
    assert!(!srp.needs_retry(100, 100, 5));
}

#[test]
fn short_read_max_retries_respected() {
    let mut srp = ShortReadProtection::new();
    srp.max_retries = 2;
    srp.record_retry();
    srp.record_retry();
    assert!(!srp.needs_retry(100, 50, 10));
    assert!(srp.max_retries_reached());
}

#[test]
fn short_read_disabled() {
    let srp = ShortReadProtection::disabled();
    assert!(!srp.needs_retry(100, 50, 50));
}

// ═══════════════════════════════════════════════════════════════════
// Paging State Round-Trip
// ═══════════════════════════════════════════════════════════════════

#[test]
fn paging_state_round_trip() {
    let state = PagingState::new(b"pk_data".to_vec(), b"ck_data".to_vec(), 42, 10);
    let serialized = state.serialize();
    let deserialized = PagingState::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.partition_key, b"pk_data");
    assert_eq!(deserialized.row_mark, b"ck_data");
    assert_eq!(deserialized.remaining, 42);
    assert_eq!(deserialized.remaining_in_partition, 10);
}

#[test]
fn paging_state_empty_clustering() {
    let state = PagingState::new(b"pk".to_vec(), Vec::new(), 100, 0);
    let serialized = state.serialize();
    let deserialized = PagingState::deserialize(&serialized).unwrap();
    assert!(deserialized.row_mark.is_empty());
    assert!(deserialized.has_more());
}

#[test]
fn paging_state_no_more() {
    let state = PagingState::new(b"pk".to_vec(), Vec::new(), 0, 0);
    assert!(!state.has_more());
}

#[test]
fn paging_state_invalid_data() {
    assert!(PagingState::deserialize(b"short").is_none());
}

// ═══════════════════════════════════════════════════════════════════
// Tombstone Thresholds
// ═══════════════════════════════════════════════════════════════════

#[test]
fn tombstone_tracker_warns_at_threshold() {
    let thresholds = TombstoneThresholds {
        warn_threshold: 5,
        fail_threshold: 100,
    };
    let mut tracker = TombstoneTracker::new(thresholds);
    for _ in 0..6 {
        tracker.track_row_tombstone();
    }
    assert!(tracker.warning_issued);
    assert!(!tracker.is_failure());
}

#[test]
fn tombstone_tracker_fails_at_threshold() {
    let thresholds = TombstoneThresholds {
        warn_threshold: 5,
        fail_threshold: 10,
    };
    let mut tracker = TombstoneTracker::new(thresholds);
    for _ in 0..11 {
        tracker.track_row_tombstone();
    }
    assert!(tracker.is_failure());
}

#[test]
fn tombstone_tracker_no_warning_below_threshold() {
    let thresholds = TombstoneThresholds::default();
    let mut tracker = TombstoneTracker::new(thresholds);
    tracker.track_row_tombstone();
    tracker.track_row_tombstone();
    assert!(!tracker.warning_issued);
    assert!(!tracker.is_failure());
}

// ═══════════════════════════════════════════════════════════════════
// Speculative Retry Policy
// ═══════════════════════════════════════════════════════════════════

#[test]
fn speculative_retry_none_never_speculates() {
    let policy = SpeculativeRetryPolicy::None;
    assert!(!policy.may_speculate());
}

#[test]
fn speculative_retry_always_speculates() {
    let policy = SpeculativeRetryPolicy::Always;
    assert!(policy.may_speculate());
}

#[test]
fn speculative_retry_parses_from_cql() {
    assert!(matches!(
        SpeculativeRetryPolicy::from_str_cql("NONE"),
        Some(SpeculativeRetryPolicy::None)
    ));
    assert!(matches!(
        SpeculativeRetryPolicy::from_str_cql("ALWAYS"),
        Some(SpeculativeRetryPolicy::Always)
    ));
}

// ═══════════════════════════════════════════════════════════════════
// Digest Resolver
// ═══════════════════════════════════════════════════════════════════

#[test]
fn digest_resolver_with_matching_digests() {
    let mut resolver = DigestResolver::new(2);
    let data = DataResponse {
        partitions: Vec::new(),
        tombstones_read: 0,
        is_short_read: false,
    };
    let digest = data.digest();
    resolver.add_data_response(data);
    resolver.add_digest_response(digest);
    let result = resolver.resolve();
    assert!(result.is_ok());
}

#[test]
fn digest_from_bytes_deterministic() {
    let d1 = Digest::from_bytes(b"hello");
    let d2 = Digest::from_bytes(b"hello");
    assert_eq!(d1, d2);
}

#[test]
fn digest_from_different_data_differs() {
    let d1 = Digest::from_bytes(b"hello");
    let d2 = Digest::from_bytes(b"world");
    assert_ne!(d1, d2);
}
