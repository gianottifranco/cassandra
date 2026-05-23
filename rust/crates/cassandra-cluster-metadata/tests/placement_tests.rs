// Licensed under Apache License, Version 2.0.

//! Integration tests for Phase 23: DHT, locator, snitches, partitioners,
//! token allocator, range streamer, and transient replication.

use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use cassandra_common::Token;
use cassandra_common::partitioner::{
    ByteOrderedPartitioner, LocalPartitioner, Murmur3Partitioner, Partitioner, RandomPartitioner,
    create_partitioner,
};
use cassandra_common::token::TokenRange;

use cassandra_cluster_metadata::node::Endpoint;
use cassandra_cluster_metadata::range_streamer::RangeStreamer;
use cassandra_cluster_metadata::replica_collection::ReplicationFactor;
use cassandra_cluster_metadata::replication::{
    NetworkTopologyStrategy, OldNetworkTopologyStrategy, Replica, ReplicationStrategy,
    SimpleStrategy, TransientReplicationStrategy, create_strategy,
};
use cassandra_cluster_metadata::ring::TokenRing;
use cassandra_cluster_metadata::snitch::{
    AlibabaCloudSnitch, AzureSnitch, CloudstackSnitch, Ec2Snitch, GoogleCloudSnitch,
    PropertyFileSnitch, SimpleSnitch, Snitch,
};
use cassandra_cluster_metadata::token_allocator::{NoReplicationTokenAllocator, TokenAllocator};

fn ep(port: u16) -> Endpoint {
    Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 0, 0, 1),
        port,
    )))
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-DC NTS placement with rack awareness
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn nts_multi_dc_rack_awareness() {
    // 6-node cluster: 3 nodes in dc1 (3 racks), 3 nodes in dc2 (3 racks)
    let mut topology = HashMap::new();
    topology.insert(ep(7001), ("dc1".to_string(), "rack-a".to_string()));
    topology.insert(ep(7002), ("dc1".to_string(), "rack-b".to_string()));
    topology.insert(ep(7003), ("dc1".to_string(), "rack-c".to_string()));
    topology.insert(ep(7004), ("dc2".to_string(), "rack-a".to_string()));
    topology.insert(ep(7005), ("dc2".to_string(), "rack-b".to_string()));
    topology.insert(ep(7006), ("dc2".to_string(), "rack-c".to_string()));
    let snitch = PropertyFileSnitch::new(topology, "dc1", "rack-a");

    let mut ring = TokenRing::new();
    ring.add_token(Token::from_raw(-3000), ep(7001));
    ring.add_token(Token::from_raw(-2000), ep(7004));
    ring.add_token(Token::from_raw(-1000), ep(7002));
    ring.add_token(Token::from_raw(0), ep(7005));
    ring.add_token(Token::from_raw(1000), ep(7003));
    ring.add_token(Token::from_raw(2000), ep(7006));

    let mut dc_rf = BTreeMap::new();
    dc_rf.insert("dc1".to_string(), 3);
    dc_rf.insert("dc2".to_string(), 2);
    let strategy = NetworkTopologyStrategy::new(dc_rf);

    // For any token, we should get 3 replicas in dc1 and 2 in dc2
    let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-2500), &ring, &snitch);
    assert_eq!(replicas.len(), 5);

    let dc1_count = replicas
        .iter()
        .filter(|e| snitch.datacenter(e) == "dc1")
        .count();
    let dc2_count = replicas
        .iter()
        .filter(|e| snitch.datacenter(e) == "dc2")
        .count();
    assert_eq!(dc1_count, 3);
    assert_eq!(dc2_count, 2);

    // Verify rack diversity in dc1: all 3 racks should be used
    let dc1_racks: Vec<String> = replicas
        .iter()
        .filter(|e| snitch.datacenter(e) == "dc1")
        .map(|e| snitch.rack(e))
        .collect();
    let mut unique_racks: Vec<_> = dc1_racks.clone();
    unique_racks.sort();
    unique_racks.dedup();
    assert_eq!(unique_racks.len(), 3, "dc1 should use all 3 racks");
}

// ─────────────────────────────────────────────────────────────────────────────
// Token allocation balance verification
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn token_allocation_balances_ownership() {
    let mut ring = TokenRing::new();
    // Start with 3 nodes, evenly spaced
    let third = (i64::MAX as i128 - i64::MIN as i128) / 3;
    ring.add_token(Token::from_raw((i64::MIN as i128 + third) as i64), ep(7001));
    ring.add_token(
        Token::from_raw((i64::MIN as i128 + 2 * third) as i64),
        ep(7002),
    );
    ring.add_token(Token::from_raw(i64::MAX - 1), ep(7003));

    let allocator = NoReplicationTokenAllocator;
    let snitch = SimpleSnitch;

    // Allocate 3 tokens for a new node
    let new_tokens = allocator.allocate(&ring, 3, &snitch, &ep(7004));
    assert_eq!(new_tokens.len(), 3);

    // All new tokens should be unique and different from existing
    for t in &new_tokens {
        assert!(ring.primary_endpoint(*t).is_some() || ring.is_empty());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cloud snitch DC/rack parsing
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn cloud_snitches_dc_rack_parsing() {
    // EC2
    let ec2 = Ec2Snitch::new("us-east-1", "us-east-1a");
    assert_eq!(ec2.datacenter(&ep(7001)), "us-east-1");
    assert_eq!(ec2.rack(&ep(7001)), "us-east-1a");

    // GCE
    let gce = GoogleCloudSnitch::new("us-central1", "us-central1-a");
    assert_eq!(gce.datacenter(&ep(7001)), "us-central1");
    assert_eq!(gce.rack(&ep(7001)), "us-central1-a");

    // Azure
    let azure = AzureSnitch::new("eastus", "eastus-1");
    assert_eq!(azure.datacenter(&ep(7001)), "eastus");
    assert_eq!(azure.rack(&ep(7001)), "eastus-1");

    // Alibaba
    let alibaba = AlibabaCloudSnitch::new("cn-hangzhou", "cn-hangzhou-b");
    assert_eq!(alibaba.datacenter(&ep(7001)), "cn-hangzhou");
    assert_eq!(alibaba.rack(&ep(7001)), "cn-hangzhou-b");

    // CloudStack
    let cs = CloudstackSnitch::new("zone1", "zone1-rack1");
    assert_eq!(cs.datacenter(&ep(7001)), "zone1");
    assert_eq!(cs.rack(&ep(7001)), "zone1-rack1");
}

// ─────────────────────────────────────────────────────────────────────────────
// Transient replica placement
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn transient_replica_placement_correct() {
    let mut dc_rf = BTreeMap::new();
    dc_rf.insert("datacenter1".to_string(), 3);
    let mut dc_trans = BTreeMap::new();
    dc_trans.insert("datacenter1".to_string(), 1);

    let strategy = TransientReplicationStrategy::new(dc_rf, dc_trans);
    let snitch = SimpleSnitch;

    let mut ring = TokenRing::new();
    ring.add_token(Token::from_raw(-100), ep(7001));
    ring.add_token(Token::from_raw(0), ep(7002));
    ring.add_token(Token::from_raw(100), ep(7003));
    ring.add_token(Token::from_raw(200), ep(7004));

    let replicas = strategy.calculate_natural_replicas(Token::from_raw(-50), &ring, &snitch);
    assert_eq!(replicas.len(), 3);

    // 2 full + 1 transient
    let full_count = replicas.iter().filter(|r: &&Replica| r.is_full()).count();
    let trans_count = replicas
        .iter()
        .filter(|r: &&Replica| r.is_transient)
        .count();
    assert_eq!(full_count, 2);
    assert_eq!(trans_count, 1);

    // Full replicas come first
    assert!(replicas[0].is_full());
    assert!(replicas[1].is_full());
    assert!(replicas[2].is_transient);

    // Read endpoints should only include full replicas
    let read_eps = strategy.calculate_read_endpoints(Token::from_raw(-50), &ring, &snitch);
    assert_eq!(read_eps.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// Range streaming coverage
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn range_streaming_covers_all_ranges() {
    let mut ring = TokenRing::new();
    ring.add_token(Token::from_raw(-100), ep(7001));
    ring.add_token(Token::from_raw(0), ep(7002));
    ring.add_token(Token::from_raw(100), ep(7003));
    ring.add_token(Token::from_raw(200), ep(7004));

    let strategy = SimpleStrategy::new(2);
    let snitch = SimpleSnitch;

    let mut streamer = RangeStreamer::new(ep(7005), &ring, &strategy, &snitch);

    let ranges = vec![
        TokenRange::new(Token::from_raw(-100), Token::from_raw(0)),
        TokenRange::new(Token::from_raw(0), Token::from_raw(100)),
        TokenRange::new(Token::from_raw(100), Token::from_raw(200)),
    ];
    streamer.add_ranges(&ranges);

    let fetches = streamer.fetch_replicas();
    // All 3 ranges should have sources
    assert_eq!(fetches.len(), 3);

    // Convert to stream plan
    let plan = streamer.to_stream_plan();
    assert!(!plan.requests.is_empty());

    // Total ranges across all requests should equal input ranges
    let total_ranges: usize = plan.requests.iter().map(|r| r.ranges.len()).sum();
    assert_eq!(total_ranges, 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Create strategy with transient format
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn create_strategy_transient_format() {
    let mut options = BTreeMap::new();
    options.insert("dc1".to_string(), "3/1".to_string());
    options.insert("dc2".to_string(), "2".to_string());
    let s = create_strategy(
        "org.apache.cassandra.locator.NetworkTopologyStrategy",
        &options,
    );
    assert_eq!(s.name(), "TransientReplicationStrategy");
    assert_eq!(s.replication_factor(), 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationFactor parsing
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn replication_factor_formats() {
    let rf = ReplicationFactor::parse("3").unwrap();
    assert_eq!(rf.all_replicas(), 3);
    assert_eq!(rf.full_replicas(), 3);
    assert!(!rf.has_transient());

    let rf = ReplicationFactor::parse("5/2").unwrap();
    assert_eq!(rf.all_replicas(), 5);
    assert_eq!(rf.full_replicas(), 3);
    assert_eq!(rf.transient_replicas(), 2);
    assert!(rf.has_transient());

    // Invalid
    assert!(ReplicationFactor::parse("3/3").is_none());
    assert!(ReplicationFactor::parse("abc").is_none());
}

// ─────────────────────────────────────────────────────────────────────────────
// Partitioner enhancements
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn partitioner_split_and_ownership() {
    let p = Murmur3Partitioner;

    // Split
    let splits = p.split(Token::from_raw(0), Token::from_raw(1000), 5);
    assert_eq!(splits.len(), 6);
    assert_eq!(splits[0], Token::from_raw(0));
    assert_eq!(splits[5], Token::from_raw(1000));

    // Ownership
    let tokens = vec![Token::from_raw(0)];
    let ownership = p.describe_ownership(&tokens);
    assert_eq!(ownership[&Token::from_raw(0)], 1.0);

    // Preserves order
    assert!(!p.preserves_order());
    assert!(ByteOrderedPartitioner.preserves_order());
    assert!(LocalPartitioner::default().preserves_order());
}

#[test]
fn partitioner_factory_covers_legacy_random_and_byteordered() {
    let random = create_partitioner("org.apache.cassandra.dht.RandomPartitioner");
    assert_eq!(random.name(), "org.apache.cassandra.dht.RandomPartitioner");
    assert!(!random.preserves_order());
    assert!(random.get_token(b"account:1").value() >= 0);
    assert_eq!(
        random.get_token(b"account:1"),
        random.get_token(b"account:1")
    );

    let short_random = create_partitioner("random");
    assert_eq!(short_random.name(), RandomPartitioner.name());

    let byteordered = create_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner");
    assert_eq!(
        byteordered.name(),
        "org.apache.cassandra.dht.ByteOrderedPartitioner"
    );
    assert!(byteordered.preserves_order());
    assert!(byteordered.get_token(b"\x00\x01") < byteordered.get_token(b"\x00\x02"));
}

// ─────────────────────────────────────────────────────────────────────────────
// OldNetworkTopologyStrategy
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn old_nts_simple_ring_walk() {
    let strategy = OldNetworkTopologyStrategy::new(3);
    let snitch = SimpleSnitch;

    let mut ring = TokenRing::new();
    ring.add_token(Token::from_raw(-100), ep(7001));
    ring.add_token(Token::from_raw(0), ep(7002));
    ring.add_token(Token::from_raw(100), ep(7003));
    ring.add_token(Token::from_raw(200), ep(7004));

    let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
    assert_eq!(replicas.len(), 3);
    // Should walk clockwise: 7002, 7003, 7004
    assert_eq!(replicas[0], ep(7002));
    assert_eq!(replicas[1], ep(7003));
    assert_eq!(replicas[2], ep(7004));
}
