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

//! Integration facade: wires `Gossiper`, `MessagingService`, handlers,
//! periodic task, subscribers, and metrics into a single `GossipService`.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.Gossiper` (lifecycle management parts)
//! - `org.apache.cassandra.service.StorageService` (gossip bootstrap)

use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::MessagingService;
use tracing::info;

use crate::gossip::handlers::register_gossip_handlers;
use crate::gossip::metrics::{GossipMetrics, GossipMetricsSnapshot};
use crate::gossip::shutdown::announce_shutdown;
use crate::gossip::subscribers::{EndpointStateChangeSubscriber, SubscriberRegistry};
use crate::gossip::task::{GossipTask, GossipTaskConfig};
use crate::gossip::{Gossiper, SeedProvider};
use crate::node::Endpoint;

/// Configuration for `GossipService`.
pub struct GossipServiceConfig {
    /// This node's endpoint.
    pub local_endpoint: Endpoint,
    /// Seed nodes for initial contact.
    pub seeds: SeedProvider,
    /// Generation number (typically epoch seconds at startup).
    pub generation: i64,
    /// Cluster name for SYN validation.
    pub cluster_name: String,
    /// Gossip round interval.
    pub gossip_interval: Duration,
    /// Per-message timeout.
    pub message_timeout: Duration,
}

impl GossipServiceConfig {
    /// Create a config with sensible defaults.
    pub fn new(local_endpoint: Endpoint, seeds: SeedProvider, generation: i64) -> Self {
        Self {
            local_endpoint,
            seeds,
            generation,
            cluster_name: "cassandra-rust".to_string(),
            gossip_interval: Duration::from_secs(1),
            message_timeout: Duration::from_secs(1),
        }
    }
}

/// The gossip service: wires together all gossip subsystems.
///
/// Owns the `Gossiper`, registers handlers on `MessagingService`,
/// spawns the periodic task, and provides access to subscribers and metrics.
pub struct GossipService {
    gossiper: Arc<Gossiper>,
    messaging: Arc<MessagingService>,
    subscribers: Arc<SubscriberRegistry>,
    metrics: Arc<GossipMetrics>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GossipService {
    /// Create and start a new gossip service.
    pub fn new(config: GossipServiceConfig, messaging: Arc<MessagingService>) -> Self {
        let gossiper = Arc::new(Gossiper::with_cluster_name(
            config.local_endpoint,
            config.seeds,
            config.generation,
            config.cluster_name,
        ));

        // Register verb handlers
        register_gossip_handlers(Arc::clone(&gossiper), &messaging);

        let subscribers = Arc::new(SubscriberRegistry::new());
        let metrics = Arc::new(GossipMetrics::new());

        Self {
            gossiper,
            messaging,
            subscribers,
            metrics,
            task_handle: None,
        }
    }

    /// Start the periodic gossip task.
    pub fn start(&mut self) {
        let task = Arc::new(GossipTask::new(
            Arc::clone(&self.gossiper),
            Arc::clone(&self.messaging),
            GossipTaskConfig {
                interval: Duration::from_secs(1),
                message_timeout: Duration::from_secs(1),
            },
        ));

        let handle = task.spawn();
        self.task_handle = Some(handle);

        info!(
            endpoint = %self.gossiper.local_endpoint(),
            "Gossip service started"
        );
    }

    /// Gracefully shut down the gossip service.
    pub async fn shutdown(&mut self) {
        announce_shutdown(&self.gossiper, &self.messaging).await;

        if let Some(handle) = self.task_handle.take() {
            // The task checks is_running() and will exit
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        info!(
            endpoint = %self.gossiper.local_endpoint(),
            "Gossip service stopped"
        );
    }

    /// Get a reference to the underlying gossiper.
    pub fn gossiper(&self) -> &Arc<Gossiper> {
        &self.gossiper
    }

    /// Get the subscriber registry for registering event listeners.
    pub fn subscribers(&self) -> &Arc<SubscriberRegistry> {
        &self.subscribers
    }

    /// Register a subscriber for gossip events.
    pub fn register_subscriber(&self, sub: Arc<dyn EndpointStateChangeSubscriber>) {
        self.subscribers.register(sub);
    }

    /// Get the gossip metrics.
    pub fn metrics(&self) -> &Arc<GossipMetrics> {
        &self.metrics
    }

    /// Take a snapshot of the current gossip metrics.
    pub fn metrics_snapshot(&self) -> GossipMetricsSnapshot {
        self.metrics.update_from_gossiper(&self.gossiper);
        self.metrics.snapshot()
    }

    /// Get the messaging service.
    pub fn messaging(&self) -> &Arc<MessagingService> {
        &self.messaging
    }

    /// Check if the gossip service is running.
    pub fn is_running(&self) -> bool {
        self.gossiper.is_running()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::ApplicationState;
    use std::net::SocketAddr;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn gossip_service_creation() {
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let config = GossipServiceConfig::new(ep(7001), SeedProvider::new(vec![ep(7002)]), 1);

        let service = GossipService::new(config, messaging);

        assert!(service.is_running());
        assert_eq!(service.gossiper().local_endpoint(), ep(7001));
        assert_eq!(service.subscribers().subscriber_count(), 0);
    }

    #[tokio::test]
    async fn gossip_service_shutdown() {
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let config = GossipServiceConfig::new(ep(7001), SeedProvider::new(vec![]), 1);

        let mut service = GossipService::new(config, messaging);
        service.start();

        assert!(service.is_running());

        service.shutdown().await;

        assert!(!service.is_running());
    }

    #[test]
    fn metrics_snapshot_reflects_state() {
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let config = GossipServiceConfig::new(ep(7001), SeedProvider::new(vec![]), 1);

        let service = GossipService::new(config, messaging);
        service
            .gossiper()
            .set_local_state(ApplicationState::SchemaVersion, "v1".to_string());

        let snap = service.metrics_snapshot();
        assert!(snap.schema_agreement);
        assert_eq!(snap.live_count, 0);
    }

    #[test]
    fn register_subscriber() {
        use crate::gossip::subscribers::EndpointStateChangeSubscriber;
        use crate::gossip::{EndpointState, VersionedValue};

        struct NoopSub;
        impl EndpointStateChangeSubscriber for NoopSub {
            fn on_join(&self, _: Endpoint, _: &EndpointState) {}
            fn on_alive(&self, _: Endpoint, _: &EndpointState) {}
            fn on_dead(&self, _: Endpoint, _: &EndpointState) {}
            fn on_removed(&self, _: Endpoint, _: &EndpointState) {}
            fn on_restart(&self, _: Endpoint, _: &EndpointState) {}
            fn on_state_changed(&self, _: Endpoint, _: ApplicationState, _: &VersionedValue) {}
        }

        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let config = GossipServiceConfig::new(ep(7001), SeedProvider::new(vec![]), 1);
        let service = GossipService::new(config, messaging);

        service.register_subscriber(Arc::new(NoopSub));
        assert_eq!(service.subscribers().subscriber_count(), 1);
    }
}
