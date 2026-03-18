// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

use cassandra_coordinator::{PagingState, PartitionResult, TombstoneThresholds, TombstoneTracker};
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn make_partition(row_count: usize, tombstones_every: usize) -> PartitionData {
    let mut partition = PartitionData::new();
    for idx in 0..row_count {
        partition.apply_row(Row {
            clustering_key: format!("ck-{idx:04}").into_bytes(),
            cells: vec![Cell {
                column: "v".to_string(),
                value: if tombstones_every > 0 && idx % tombstones_every == 0 {
                    None
                } else {
                    Some(format!("value-{idx}").into_bytes())
                },
                timestamp: idx as i64,
                ttl: 0,
                local_deletion_time: if tombstones_every > 0 && idx % tombstones_every == 0 {
                    Some(1)
                } else {
                    None
                },
                is_tombstone: tombstones_every > 0 && idx % tombstones_every == 0,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
    }
    partition
}

fn bench_partition_filtering(c: &mut Criterion) {
    let partition = make_partition(4096, 8);
    c.bench_function("read.partition_filtering_4k_rows", |b| {
        b.iter(|| {
            let mut tracker = TombstoneTracker::new(TombstoneThresholds::default());
            let result = PartitionResult::from_partition_data(
                b"pk".to_vec(),
                black_box(partition.clone()),
                1_000_000,
                &mut tracker,
            );
            black_box((result.live_row_count, tracker.count))
        })
    });
}

fn bench_paging_state_roundtrip(c: &mut Criterion) {
    let state = PagingState::new(
        b"partition-key".to_vec(),
        b"last-row-mark".to_vec(),
        512,
        128,
    );
    c.bench_function("read.paging_state_roundtrip", |b| {
        b.iter(|| {
            let encoded = black_box(state.serialize());
            black_box(PagingState::deserialize(&encoded).unwrap())
        })
    });
}

criterion_group!(
    read_path,
    bench_partition_filtering,
    bench_paging_state_roundtrip
);
criterion_main!(read_path);
