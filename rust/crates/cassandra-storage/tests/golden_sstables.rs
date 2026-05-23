// Licensed under Apache License, Version 2.0.

//! Golden SSTable test corpus: deterministic write-and-verify regression tests.
//!
//! All data is fixed (no randomness). These tests serve as regression
//! protection for the SSTable read/write pipeline.

use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
use cassandra_storage::sstable::bti::{BtiReader, BtiWriter};
use cassandra_storage::sstable::format::{SSTableDescriptor, SSTableFormat};
use cassandra_storage::sstable::reader::SSTableReader;
use cassandra_storage::sstable::writer::SSTableWriter;
use tempfile::TempDir;

// ─── Deterministic test data builders ─────────────────────────────────────

/// 5 partitions, 3 rows each, all regular cells.
fn golden_regular_partitions() -> Vec<(Vec<u8>, PartitionData)> {
    let mut partitions = Vec::new();
    for i in 0..5u8 {
        let pk = format!("partition_{:03}", i).into_bytes();
        let mut pd = PartitionData::new();
        for j in 0..3u8 {
            let ck = format!("row_{:03}", j).into_bytes();
            pd.apply_row(Row {
                clustering_key: ck,
                cells: vec![
                    Cell {
                        column: "name".to_string(),
                        value: Some(format!("name_{i}_{j}").into_bytes()),
                        timestamp: 1000 + i as i64 * 10 + j as i64,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    },
                    Cell {
                        column: "age".to_string(),
                        value: Some(vec![20 + i + j]),
                        timestamp: 1000 + i as i64 * 10 + j as i64,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    },
                ],
                is_tombstone: false,
                local_deletion_time: None,
            });
        }
        partitions.push((pk, pd));
    }
    partitions
}

/// Complex SSTable with tombstones, TTLs, empty values, and large keys.
fn golden_complex_partitions() -> Vec<(Vec<u8>, PartitionData)> {
    let mut partitions = Vec::new();

    // 1. Regular data partition
    let mut pd1 = PartitionData::new();
    pd1.apply_row(Row {
        clustering_key: b"ck_regular".to_vec(),
        cells: vec![Cell {
            column: "data".to_string(),
            value: Some(b"regular_value".to_vec()),
            timestamp: 100,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partitions.push((b"aaa_regular".to_vec(), pd1));

    // 2. Cell tombstone partition
    let mut pd2 = PartitionData::new();
    pd2.apply_row(Row {
        clustering_key: b"ck_cell_tomb".to_vec(),
        cells: vec![Cell {
            column: "deleted_col".to_string(),
            value: None,
            timestamp: 200,
            ttl: 0,
            local_deletion_time: Some(200),
            is_tombstone: true,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partitions.push((b"bbb_cell_tombstone".to_vec(), pd2));

    // 3. Row tombstone partition
    let mut pd3 = PartitionData::new();
    pd3.apply_row(Row {
        clustering_key: b"ck_row_tomb".to_vec(),
        cells: vec![Cell {
            column: "col".to_string(),
            value: None,
            timestamp: 300,
            ttl: 0,
            local_deletion_time: Some(300),
            is_tombstone: true,
        }],
        is_tombstone: true,
        local_deletion_time: Some(300),
    });
    partitions.push((b"ccc_row_tombstone".to_vec(), pd3));

    // 4. TTL partition
    let mut pd4 = PartitionData::new();
    pd4.apply_row(Row {
        clustering_key: b"ck_ttl".to_vec(),
        cells: vec![Cell {
            column: "expiring".to_string(),
            value: Some(b"will_expire".to_vec()),
            timestamp: 400,
            ttl: 7200,
            local_deletion_time: Some(8200),
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partitions.push((b"ddd_ttl".to_vec(), pd4));

    // 5. Empty value partition
    let mut pd5 = PartitionData::new();
    pd5.apply_row(Row {
        clustering_key: b"ck_empty".to_vec(),
        cells: vec![Cell {
            column: "empty".to_string(),
            value: Some(Vec::new()),
            timestamp: 500,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partitions.push((b"eee_empty_value".to_vec(), pd5));

    // 6. Large key partition (512 bytes)
    let large_key = vec![0x42u8; 512];
    let mut pd6 = PartitionData::new();
    pd6.apply_row(Row {
        clustering_key: b"ck_large".to_vec(),
        cells: vec![Cell {
            column: "big".to_string(),
            value: Some(vec![0xFFu8; 256]),
            timestamp: 600,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    // Large keys sort after ASCII — this key is all 0x42='B' repeated
    // which sorts between the named partitions. We insert it in order.
    partitions.push((large_key, pd6));

    // Sort by partition key for SSTable writer requirement
    partitions.sort_by(|a, b| a.0.cmp(&b.0));
    partitions
}

// ─── Test 1: Regular 5x3 roundtrip ───────────────────────────────────────

#[test]
fn golden_regular_write_read_exact_match() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "regular", 1);
    let partitions = golden_regular_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();
    let read_back = reader.iter_partitions().unwrap();

    assert_eq!(read_back.len(), 5, "expected 5 partitions");

    for (i, (pk, pd)) in read_back.iter().enumerate() {
        let expected_pk = format!("partition_{:03}", i).into_bytes();
        assert_eq!(pk, &expected_pk, "partition key mismatch at index {i}");
        assert_eq!(pd.rows.len(), 3, "partition {i} should have 3 rows");

        for j in 0..3u8 {
            let ck = format!("row_{:03}", j).into_bytes();
            let row = pd.rows.get(&ck).expect("row should exist");
            assert_eq!(row.cells.len(), 2, "each row has 2 cells");

            // Verify name cell
            assert_eq!(row.cells[0].column, "name");
            let expected_name = format!("name_{}_{}", i, j).into_bytes();
            assert_eq!(
                row.cells[0].value.as_deref(),
                Some(expected_name.as_slice())
            );
            assert_eq!(row.cells[0].timestamp, 1000 + i as i64 * 10 + j as i64);
            assert!(!row.cells[0].is_tombstone);

            // Verify age cell
            assert_eq!(row.cells[1].column, "age");
            assert_eq!(
                row.cells[1].value.as_deref(),
                Some([20 + i as u8 + j].as_slice())
            );
        }
    }
}

// ─── Test 2: Complex SSTable with all data types ─────────────────────────

#[test]
fn golden_complex_tombstones_ttls_empty_large() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "complex", 2);
    let partitions = golden_complex_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();

    // Regular
    let p = reader.get_partition(b"aaa_regular").unwrap().unwrap();
    let row = p.rows.get(b"ck_regular".as_slice()).unwrap();
    assert!(!row.is_tombstone);
    assert_eq!(
        row.cells[0].value.as_deref(),
        Some(b"regular_value".as_slice())
    );

    // Cell tombstone
    let p = reader
        .get_partition(b"bbb_cell_tombstone")
        .unwrap()
        .unwrap();
    let row = p.rows.get(b"ck_cell_tomb".as_slice()).unwrap();
    assert!(row.cells[0].is_tombstone);
    assert!(row.cells[0].value.is_none());
    assert_eq!(row.cells[0].local_deletion_time, Some(200));

    // Row tombstone
    let p = reader.get_partition(b"ccc_row_tombstone").unwrap().unwrap();
    let row = p.rows.get(b"ck_row_tomb".as_slice()).unwrap();
    assert!(row.is_tombstone);
    assert_eq!(row.local_deletion_time, Some(300));

    // TTL
    let p = reader.get_partition(b"ddd_ttl").unwrap().unwrap();
    let row = p.rows.get(b"ck_ttl".as_slice()).unwrap();
    assert_eq!(row.cells[0].ttl, 7200);
    assert_eq!(row.cells[0].local_deletion_time, Some(8200));
    assert_eq!(
        row.cells[0].value.as_deref(),
        Some(b"will_expire".as_slice())
    );

    // Empty value
    let p = reader.get_partition(b"eee_empty_value").unwrap().unwrap();
    let row = p.rows.get(b"ck_empty".as_slice()).unwrap();
    assert_eq!(row.cells[0].value.as_deref(), Some(b"".as_slice()));

    // Large key (512 bytes of 0x42)
    let large_key = vec![0x42u8; 512];
    let p = reader.get_partition(&large_key).unwrap().unwrap();
    let row = p.rows.get(b"ck_large".as_slice()).unwrap();
    assert_eq!(row.cells[0].value.as_ref().unwrap().len(), 256);
}

// ─── Test 3: Statistics validation ───────────────────────────────────────

#[test]
fn golden_statistics_match_expected() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "stats", 3);
    let partitions = golden_regular_partitions();

    let write_stats = SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();
    let read_stats = reader.stats().unwrap();

    // 5 partitions, 3 rows each, 2 cells per row
    assert_eq!(write_stats.partition_count, 5);
    assert_eq!(write_stats.row_count, 15);
    assert_eq!(write_stats.cell_count, 30);
    assert_eq!(read_stats.partition_count, 5);
    assert_eq!(read_stats.row_count, 15);
    assert_eq!(read_stats.cell_count, 30);

    // Timestamp bounds: min = 1000+0*10+0 = 1000, max = 1000+4*10+2 = 1042
    assert_eq!(write_stats.min_timestamp, 1000);
    assert_eq!(write_stats.max_timestamp, 1042);
    assert_eq!(read_stats.min_timestamp, 1000);
    assert_eq!(read_stats.max_timestamp, 1042);

    assert!(write_stats.data_size > 0);
    assert!(write_stats.index_size > 0);
}

#[test]
fn golden_complex_statistics_match_expected() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "cstats", 4);
    let partitions = golden_complex_partitions();

    let write_stats = SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

    // 6 partitions (after sort), 1 row each, 1 cell per row = 6 cells
    assert_eq!(write_stats.partition_count, 6);
    assert_eq!(write_stats.row_count, 6);
    assert_eq!(write_stats.cell_count, 6);
    assert_eq!(write_stats.min_timestamp, 100);
    assert_eq!(write_stats.max_timestamp, 600);
}

// ─── Test 4: Index validation ────────────────────────────────────────────

#[test]
fn golden_index_all_partitions_accessible() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "index", 5);
    let partitions = golden_regular_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();

    // Every written partition should be retrievable by key
    for (pk, original_pd) in &partitions {
        let read_pd = reader
            .get_partition(pk)
            .unwrap()
            .expect("partition should be found via index");
        assert_eq!(
            read_pd.rows.len(),
            original_pd.rows.len(),
            "row count mismatch for pk {:?}",
            pk
        );
    }

    // iter_partitions should return same count and order
    let all = reader.iter_partitions().unwrap();
    assert_eq!(all.len(), partitions.len());
    for (i, (pk, _)) in all.iter().enumerate() {
        assert_eq!(pk, &partitions[i].0);
    }
}

#[test]
fn golden_complex_index_all_accessible() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "cidx", 6);
    let partitions = golden_complex_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();

    for (pk, _) in &partitions {
        let result = reader.get_partition(pk).unwrap();
        assert!(
            result.is_some(),
            "partition {:?} not found via index",
            String::from_utf8_lossy(pk)
        );
    }
}

// ─── Test 5: Bloom filter validation ─────────────────────────────────────

#[test]
fn golden_bloom_filter_contains_all_written_keys() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "bloom", 7);
    let partitions = golden_regular_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();

    // All written keys MUST return true
    for (pk, _) in &partitions {
        assert!(
            reader.might_contain_key(pk),
            "bloom filter should contain written key {:?}",
            pk
        );
    }

    // Random keys should mostly return false
    let mut false_positives = 0;
    let test_count = 100;
    for i in 0..test_count {
        let fake_key = format!("nonexistent_key_{:05}", i + 1000).into_bytes();
        if reader.might_contain_key(&fake_key) {
            false_positives += 1;
        }
    }

    // With 5 elements and fp_rate 0.01, 100 probes should yield < 10 FPs
    assert!(
        false_positives < 10,
        "too many false positives: {false_positives}/{test_count}"
    );
}

#[test]
fn golden_bloom_filter_complex_keys() {
    let dir = TempDir::new().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "golden", "cbloom", 8);
    let partitions = golden_complex_partitions();

    SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = SSTableReader::open(desc).unwrap();

    // All written keys must be found (including large key)
    for (pk, _) in &partitions {
        assert!(
            reader.might_contain_key(pk),
            "bloom filter missing key of len {}",
            pk.len()
        );
    }
}

// ─── BTI format golden tests ─────────────────────────────────────────────

#[test]
fn golden_bti_regular_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut desc = SSTableDescriptor::new(dir.path(), "golden", "bti_reg", 9);
    desc.format = SSTableFormat::Bti;
    let partitions = golden_regular_partitions();

    BtiWriter::new(desc.clone()).write(&partitions).unwrap();
    let reader = BtiReader::open(desc).unwrap();
    let read_back = reader.iter_partitions().unwrap();

    assert_eq!(read_back.len(), 5);
    for (i, (pk, pd)) in read_back.iter().enumerate() {
        assert_eq!(pk, &partitions[i].0);
        assert_eq!(pd.rows.len(), 3);
    }

    // Verify individual lookups
    for (pk, _) in &partitions {
        let p = reader.get_partition(pk).unwrap();
        assert!(p.is_some(), "BTI lookup failed for {:?}", pk);
    }
}

#[test]
fn golden_bti_statistics_match() {
    let dir = TempDir::new().unwrap();
    let mut desc = SSTableDescriptor::new(dir.path(), "golden", "bti_st", 10);
    desc.format = SSTableFormat::Bti;
    let partitions = golden_regular_partitions();

    let stats = BtiWriter::new(desc.clone()).write(&partitions).unwrap();
    assert_eq!(stats.partition_count, 5);
    assert_eq!(stats.row_count, 15);
    assert_eq!(stats.cell_count, 30);
    assert_eq!(stats.min_timestamp, 1000);
    assert_eq!(stats.max_timestamp, 1042);

    let reader = BtiReader::open(desc).unwrap();
    let read_stats = reader.stats().unwrap();
    assert_eq!(read_stats.partition_count, 5);
    assert_eq!(read_stats.row_count, 15);
}
