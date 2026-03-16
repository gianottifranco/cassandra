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

//! Read executors: strategy pattern for how reads are dispatched to replicas.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.reads.AbstractReadExecutor`
//! - `org.apache.cassandra.service.reads.NeverSpeculatingReadExecutor`
//! - `org.apache.cassandra.service.reads.SpeculatingReadExecutor`
//! - `org.apache.cassandra.service.reads.AlwaysSpeculatingReadExecutor`

use std::time::Duration;

use cassandra_cluster_metadata::Endpoint;

use super::speculative_retry::SpeculativeRetryPolicy;

/// How a read should be executed: which replicas get data vs digest requests,
/// whether speculative requests are sent, and after what delay.
///
/// ## Java Oracle
///
/// `AbstractReadExecutor` subclasses determine the execution plan.
#[derive(Debug, Clone)]
pub struct ReadExecutionPlan {
    /// The replica that receives the full data request.
    pub data_replica: Endpoint,
    /// Replicas that receive digest-only requests.
    pub digest_replicas: Vec<Endpoint>,
    /// Optional speculative replica (contacted after delay or immediately).
    pub speculative_replica: Option<Endpoint>,
    /// Delay before contacting the speculative replica. `None` = no speculation.
    /// `Some(Duration::ZERO)` = immediate (Always).
    pub speculative_delay: Option<Duration>,
    /// Total number of required responses to satisfy the consistency level.
    pub required_responses: usize,
}

/// The type of read executor to use, determined by the speculative retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadExecutorType {
    /// Standard executor: 1 data + N digest, no speculation.
    NeverSpeculating,
    /// Sends an extra request after a delay based on latency percentile or fixed ms.
    Speculating,
    /// Sends an extra request immediately with the initial batch.
    AlwaysSpeculating,
}

impl ReadExecutorType {
    /// Determine executor type from speculative retry policy.
    pub fn from_policy(policy: &SpeculativeRetryPolicy) -> Self {
        match policy {
            SpeculativeRetryPolicy::None => Self::NeverSpeculating,
            SpeculativeRetryPolicy::Always => Self::AlwaysSpeculating,
            SpeculativeRetryPolicy::Percentile(_) | SpeculativeRetryPolicy::FixedDelay(_) => {
                Self::Speculating
            }
        }
    }
}

/// Compute the read execution plan given replicas, CL requirements, and policy.
///
/// ## Java Oracle
///
/// `AbstractReadExecutor.getReadExecutor()` in `StorageProxy`
pub fn compute_execution_plan(
    sorted_replicas: &[Endpoint],
    required: usize,
    policy: &SpeculativeRetryPolicy,
    percentile_latency_ms: impl Fn(f64) -> u64,
) -> ReadExecutionPlan {
    assert!(!sorted_replicas.is_empty(), "Need at least one replica");
    assert!(required <= sorted_replicas.len(), "Not enough replicas for CL");

    let data_replica = sorted_replicas[0];
    let digest_end = required.min(sorted_replicas.len());
    let digest_replicas: Vec<Endpoint> = sorted_replicas[1..digest_end].to_vec();

    let executor_type = ReadExecutorType::from_policy(policy);

    // Find a speculative replica (one beyond the required set)
    let speculative_replica = if executor_type != ReadExecutorType::NeverSpeculating
        && sorted_replicas.len() > required
    {
        Some(sorted_replicas[required])
    } else {
        None
    };

    let speculative_delay = if speculative_replica.is_some() {
        policy.speculative_delay(&percentile_latency_ms)
    } else {
        None
    };

    ReadExecutionPlan {
        data_replica,
        digest_replicas,
        speculative_replica,
        speculative_delay,
        required_responses: required,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn never_speculating_executor() {
        let replicas = vec![ep(7001), ep(7002), ep(7003)];
        let plan = compute_execution_plan(
            &replicas,
            2,
            &SpeculativeRetryPolicy::None,
            |_| 100,
        );
        assert_eq!(plan.data_replica, ep(7001));
        assert_eq!(plan.digest_replicas.len(), 1);
        assert!(plan.speculative_replica.is_none());
        assert!(plan.speculative_delay.is_none());
    }

    #[test]
    fn always_speculating_executor() {
        let replicas = vec![ep(7001), ep(7002), ep(7003)];
        let plan = compute_execution_plan(
            &replicas,
            2,
            &SpeculativeRetryPolicy::Always,
            |_| 100,
        );
        assert_eq!(plan.data_replica, ep(7001));
        assert_eq!(plan.digest_replicas.len(), 1);
        assert_eq!(plan.speculative_replica, Some(ep(7003)));
        assert_eq!(plan.speculative_delay, Some(Duration::ZERO));
    }

    #[test]
    fn speculating_executor_with_percentile() {
        let replicas = vec![ep(7001), ep(7002), ep(7003)];
        let plan = compute_execution_plan(
            &replicas,
            2,
            &SpeculativeRetryPolicy::Percentile(99.0),
            |_| 42,
        );
        assert_eq!(plan.speculative_replica, Some(ep(7003)));
        assert_eq!(plan.speculative_delay, Some(Duration::from_millis(42)));
    }

    #[test]
    fn no_speculation_when_not_enough_replicas() {
        let replicas = vec![ep(7001), ep(7002)];
        let plan = compute_execution_plan(
            &replicas,
            2,
            &SpeculativeRetryPolicy::Always,
            |_| 100,
        );
        // No extra replica available for speculation
        assert!(plan.speculative_replica.is_none());
    }

    #[test]
    fn cl_one_uses_single_replica() {
        let replicas = vec![ep(7001), ep(7002), ep(7003)];
        let plan = compute_execution_plan(
            &replicas,
            1,
            &SpeculativeRetryPolicy::None,
            |_| 100,
        );
        assert_eq!(plan.data_replica, ep(7001));
        assert!(plan.digest_replicas.is_empty());
    }

    #[test]
    fn executor_type_from_policy() {
        assert_eq!(
            ReadExecutorType::from_policy(&SpeculativeRetryPolicy::None),
            ReadExecutorType::NeverSpeculating,
        );
        assert_eq!(
            ReadExecutorType::from_policy(&SpeculativeRetryPolicy::Always),
            ReadExecutorType::AlwaysSpeculating,
        );
        assert_eq!(
            ReadExecutorType::from_policy(&SpeculativeRetryPolicy::Percentile(99.0)),
            ReadExecutorType::Speculating,
        );
        assert_eq!(
            ReadExecutorType::from_policy(&SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(50))),
            ReadExecutorType::Speculating,
        );
    }
}
