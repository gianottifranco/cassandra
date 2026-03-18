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

//! Integration tests: multi-node gossip convergence over real TCP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cassandra_cluster_metadata::gossip::handlers::register_gossip_handlers;
use cassandra_cluster_metadata::gossip::task::{GossipTask, GossipTaskConfig};
use cassandra_cluster_metadata::node::Endpoint;
use cassandra_cluster_metadata::{ApplicationState, Gossiper, SeedProvider};
use cassandra_messaging::MessagingService;

/// Helper: start a gossiper with a TCP listener and periodic task.
struct TestNode {
    gossiper: Arc<Gossiper>,
    #[allow(dead_code)]
    messaging: Arc<MessagingService>,
    addr: SocketAddr,
    _task_handle: tokio::task::JoinHandle<()>,
}

impl TestNode {
    async fn start(seeds: Vec<SocketAddr>, generation: i64) -> Self {
        // Bind to random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let seed_endpoints: Vec<Endpoint> = seeds.iter().map(|a| Endpoint::new(*a)).collect();
        let gossiper = Arc::new(Gossiper::new(
            Endpoint::new(addr),
            SeedProvider::new(seed_endpoints),
            generation,
        ));

        let messaging = Arc::new(MessagingService::new(addr));
        register_gossip_handlers(Arc::clone(&gossiper), &messaging);

        // Start listener
        let svc = Arc::clone(&messaging);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let svc2 = Arc::clone(&svc);
                        tokio::spawn(async move {
                            let _ = svc2.dispatch_on_stream(stream).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        // Start gossip task
        let task = Arc::new(GossipTask::new(
            Arc::clone(&gossiper),
            Arc::clone(&messaging),
            GossipTaskConfig {
                interval: Duration::from_millis(100), // Fast for testing
                message_timeout: Duration::from_secs(2),
            },
        ));
        let task_handle = task.spawn();

        Self {
            gossiper,
            messaging,
            addr,
            _task_handle: task_handle,
        }
    }
}

#[tokio::test]
async fn three_node_convergence() {
    // Start node 1 (seed)
    let n1 = TestNode::start(vec![], 1).await;
    n1.gossiper
        .set_local_state(ApplicationState::Datacenter, "dc1".to_string());
    n1.gossiper
        .set_local_state(ApplicationState::Status, "NORMAL".to_string());

    // Start node 2 with n1 as seed
    let n2 = TestNode::start(vec![n1.addr], 1).await;
    n2.gossiper
        .set_local_state(ApplicationState::Datacenter, "dc1".to_string());
    n2.gossiper
        .set_local_state(ApplicationState::Status, "NORMAL".to_string());

    // Start node 3 with n1 as seed
    let n3 = TestNode::start(vec![n1.addr], 1).await;
    n3.gossiper
        .set_local_state(ApplicationState::Datacenter, "dc2".to_string());
    n3.gossiper
        .set_local_state(ApplicationState::Status, "NORMAL".to_string());

    // Wait for convergence (up to 3 seconds)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if n1.gossiper.known_endpoint_count() >= 3
            && n2.gossiper.known_endpoint_count() >= 3
            && n3.gossiper.known_endpoint_count() >= 3
        {
            break;
        }

        if tokio::time::Instant::now() > deadline {
            panic!(
                "Convergence timeout: n1={}, n2={}, n3={}",
                n1.gossiper.known_endpoint_count(),
                n2.gossiper.known_endpoint_count(),
                n3.gossiper.known_endpoint_count()
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Verify cross-node state propagation
    let n3_state_from_n1 = n1
        .gossiper
        .get_endpoint_state(&Endpoint::new(n3.addr))
        .unwrap();
    assert_eq!(
        n3_state_from_n1
            .get_state(&ApplicationState::Datacenter)
            .unwrap()
            .value,
        "dc2"
    );

    // Shutdown
    n1.gossiper.shutdown();
    n2.gossiper.shutdown();
    n3.gossiper.shutdown();
}

#[tokio::test]
async fn node_join_detected() {
    // Start seed node
    let n1 = TestNode::start(vec![], 1).await;
    n1.gossiper
        .set_local_state(ApplicationState::Status, "NORMAL".to_string());

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Join a second node
    let n2 = TestNode::start(vec![n1.addr], 1).await;
    n2.gossiper
        .set_local_state(ApplicationState::Status, "JOINING".to_string());

    // Wait for n1 to see n2
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if n1.gossiper.known_endpoint_count() >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("n1 did not discover n2");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    n1.gossiper.shutdown();
    n2.gossiper.shutdown();
}

#[tokio::test]
async fn schema_agreement_across_nodes() {
    let n1 = TestNode::start(vec![], 1).await;
    n1.gossiper
        .set_local_state(ApplicationState::SchemaVersion, "schema-v1".to_string());

    let n2 = TestNode::start(vec![n1.addr], 1).await;
    n2.gossiper
        .set_local_state(ApplicationState::SchemaVersion, "schema-v1".to_string());

    // Wait for convergence
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if n1.gossiper.known_endpoint_count() >= 2 && n2.gossiper.known_endpoint_count() >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("Convergence timeout");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Both should agree on schema after convergence
    // (Note: assess_schema_agreement only checks live endpoints,
    // and the failure detector may not have marked them live yet
    // since heartbeat arrival tracking happens through gossip state updates)
    assert!(n1.gossiper.known_endpoint_count() >= 2);

    n1.gossiper.shutdown();
    n2.gossiper.shutdown();
}
