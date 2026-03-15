// Licensed under Apache License, Version 2.0.

//! Security benchmarks: bcrypt hashing, audit event throughput.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use cassandra_security::audit::{AuditEvent, AuditEventType, NoOpAuditLogger, AuditLogger};
use cassandra_security::fql::FqlRecord;

fn bench_bcrypt_hash(c: &mut Criterion) {
    c.bench_function("bcrypt_hash_cost_4", |b| {
        b.iter(|| {
            bcrypt::hash(black_box("password123"), 4).unwrap()
        });
    });
}

fn bench_bcrypt_verify(c: &mut Criterion) {
    let hashed = bcrypt::hash("password123", 4).unwrap();
    c.bench_function("bcrypt_verify_cost_4", |b| {
        b.iter(|| {
            bcrypt::verify(black_box("password123"), black_box(&hashed)).unwrap()
        });
    });
}

fn bench_audit_event_serialize(c: &mut Criterion) {
    let event = AuditEvent::now(AuditEventType::Query, "user", "127.0.0.1")
        .with_keyspace("test_ks")
        .with_table("users")
        .with_query("SELECT * FROM test_ks.users WHERE id = 1");

    c.bench_function("audit_event_serialize", |b| {
        b.iter(|| {
            serde_json::to_string(black_box(&event)).unwrap()
        });
    });
}

fn bench_noop_audit_log(c: &mut Criterion) {
    let logger = NoOpAuditLogger;
    let event = AuditEvent::now(AuditEventType::DmlRead, "user", "10.0.0.1");

    c.bench_function("noop_audit_log", |b| {
        b.iter(|| {
            logger.log(black_box(&event));
        });
    });
}

fn bench_fql_encode(c: &mut Criterion) {
    let record = FqlRecord {
        timestamp_micros: 1700000000_000000,
        consistency_level: 1,
        query: "SELECT * FROM ks.users WHERE id = ?".into(),
        bind_values: vec![b"user-123".to_vec()],
    };

    c.bench_function("fql_encode", |b| {
        b.iter(|| {
            black_box(&record).encode()
        });
    });
}

fn bench_fql_decode(c: &mut Criterion) {
    let record = FqlRecord {
        timestamp_micros: 1700000000_000000,
        consistency_level: 1,
        query: "SELECT * FROM ks.users WHERE id = ?".into(),
        bind_values: vec![b"user-123".to_vec()],
    };
    let encoded = record.encode();

    c.bench_function("fql_decode", |b| {
        b.iter(|| {
            FqlRecord::decode(black_box(&encoded)).unwrap()
        });
    });
}

criterion_group!(
    benches,
    bench_bcrypt_hash,
    bench_bcrypt_verify,
    bench_audit_event_serialize,
    bench_noop_audit_log,
    bench_fql_encode,
    bench_fql_decode,
);
criterion_main!(benches);
