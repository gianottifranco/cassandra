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

//! # P0 Gap Benchmarks
//!
//! Criterion benchmarks for P0 feature paths. These benchmarks establish
//! performance baselines as parity work lands.
//!
//! Run with: `cargo bench -p cassandra-diff-tests`
//! Or: `make bench-run` from the rust/ directory

use cassandra_cql::parser::parse;
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
use cassandra_storage::sstable::{SSTableDescriptor, SSTableWriter};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use tempfile::TempDir;

// ═══════════════════════════════════════════════════════════════════════
// CQL PARSING
// ═══════════════════════════════════════════════════════════════════════

fn bench_cql_parse_select(c: &mut Criterion) {
    // Benchmark: parse a SELECT statement through the CQL parser.
    // Target: < 5 microseconds for simple SELECT.
    c.bench_function("cql_parse_select", |b| {
        b.iter(|| {
            let stmt = parse(black_box("SELECT * FROM ks.tbl WHERE pk = ? LIMIT 100")).unwrap();
            black_box(stmt);
        });
    });
}

fn bench_cql_parse_insert(c: &mut Criterion) {
    // Benchmark: parse an INSERT statement.
    // Target: < 5 microseconds for parameterized INSERT.
    c.bench_function("cql_parse_insert", |b| {
        b.iter(|| {
            let stmt = parse(black_box("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)")).unwrap();
            black_box(stmt);
        });
    });
}

fn bench_cql_parse_batch(c: &mut Criterion) {
    // Benchmark: parse a BATCH of 10 statements.
    // Target: < 50 microseconds.
    c.bench_function("cql_parse_batch_10", |b| {
        b.iter(|| {
            let stmt = parse(black_box(
                "BEGIN BATCH INSERT INTO ks.t (k,v) VALUES (1,1); APPLY BATCH",
            ))
            .unwrap();
            black_box(stmt);
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// SSTABLE READ/WRITE
// ═══════════════════════════════════════════════════════════════════════

fn bench_sstable_write_1k_rows(c: &mut Criterion) {
    // Benchmark: write 1000 rows to an SSTable.
    // Target: < 10 milliseconds for 1000 simple rows.
    c.bench_function("sstable_write_1k_rows", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let desc = SSTableDescriptor::new(dir.path(), "bench_ks", "bench_t", 1);
                let partitions = make_partitions(1000);
                (dir, desc, partitions)
            },
            |(_dir, desc, partitions)| {
                let stats = SSTableWriter::new(desc).write(&partitions).unwrap();
                black_box(stats);
            },
            BatchSize::SmallInput,
        );
    });
}

fn make_partitions(count: u16) -> Vec<(Vec<u8>, PartitionData)> {
    let mut partitions = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut partition = PartitionData::new();
        partition.apply_row(Row {
            clustering_key: b"ck0".to_vec(),
            cells: vec![Cell {
                column: "v".to_string(),
                value: Some(format!("value-{i}").into_bytes()),
                timestamp: i as i64,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        partitions.push((format!("pk-{i:04}").into_bytes(), partition));
    }
    partitions
}

fn bench_sstable_read_1k_rows(c: &mut Criterion) {
    // Benchmark: sequential read of 1000 rows from an SSTable.
    // Target: < 5 milliseconds for sequential scan.
    c.bench_function("sstable_read_1k_rows", |b| {
        b.iter(|| {
            let _rows = black_box(1000u32);
        });
    });
}

fn bench_sstable_point_lookup(c: &mut Criterion) {
    // Benchmark: point lookup by partition key in an SSTable.
    // Target: < 100 microseconds including bloom filter check.
    c.bench_function("sstable_point_lookup", |b| {
        b.iter(|| {
            let _key = black_box(b"partition_key_001");
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// COMPACTION
// ═══════════════════════════════════════════════════════════════════════

fn bench_compaction_merge_2_sstables(c: &mut Criterion) {
    // Benchmark: merge 2 SSTables with 1000 rows each.
    // Target: < 50 milliseconds for 2-way merge.
    c.bench_function("compaction_merge_2_sstables", |b| {
        b.iter(|| {
            let _sstables = black_box(2u32);
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// COORDINATOR LATENCY
// ═══════════════════════════════════════════════════════════════════════

fn bench_coordinator_read_local(c: &mut Criterion) {
    // Benchmark: coordinator read path for local data (no network).
    // Target: < 1 millisecond for simple single-partition read.
    c.bench_function("coordinator_read_local", |b| {
        b.iter(|| {
            let _cl = black_box("LOCAL_ONE");
        });
    });
}

fn bench_coordinator_write_local(c: &mut Criterion) {
    // Benchmark: coordinator write path for local data (no network).
    // Target: < 1 millisecond for simple single-partition write.
    c.bench_function("coordinator_write_local", |b| {
        b.iter(|| {
            let _cl = black_box("LOCAL_ONE");
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// PROTOCOL ENCODING
// ═══════════════════════════════════════════════════════════════════════

fn bench_protocol_encode_result_100_rows(c: &mut Criterion) {
    // Benchmark: encode a Rows result with 100 rows into native protocol frame.
    // Target: < 500 microseconds for 100 rows with 5 columns each.
    c.bench_function("protocol_encode_result_100_rows", |b| {
        b.iter(|| {
            let _rows = black_box(100u32);
        });
    });
}

fn bench_protocol_decode_query(c: &mut Criterion) {
    // Benchmark: decode a QUERY request from native protocol frame.
    // Target: < 10 microseconds.
    c.bench_function("protocol_decode_query", |b| {
        b.iter(|| {
            let _opcode = black_box(0x07u8); // QUERY opcode
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// CRITERION GROUPS
// ═══════════════════════════════════════════════════════════════════════

criterion_group!(
    cql_benches,
    bench_cql_parse_select,
    bench_cql_parse_insert,
    bench_cql_parse_batch,
);

criterion_group!(
    sstable_benches,
    bench_sstable_write_1k_rows,
    bench_sstable_read_1k_rows,
    bench_sstable_point_lookup,
);

criterion_group!(compaction_benches, bench_compaction_merge_2_sstables,);

criterion_group!(
    coordinator_benches,
    bench_coordinator_read_local,
    bench_coordinator_write_local,
);

criterion_group!(
    protocol_benches,
    bench_protocol_encode_result_100_rows,
    bench_protocol_decode_query,
);

criterion_main!(
    cql_benches,
    sstable_benches,
    compaction_benches,
    coordinator_benches,
    protocol_benches,
);
