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

//! Pluggable contention strategies for Paxos CAS retries.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.paxos.ContentionStrategy`

use std::time::Duration;

/// Trait for computing backoff delays under Paxos contention.
pub trait ContentionStrategy: Send + Sync {
    /// Compute the backoff delay for the given attempt number (1-indexed).
    fn backoff(&self, attempt: u32) -> Duration;

    /// Maximum number of retries before giving up.
    fn max_retries(&self) -> u32;
}

/// Exponential backoff with optional jitter.
///
/// Delay = base * 2^(attempt-1), optionally randomized by [0.5, 1.5).
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    pub base: Duration,
    pub max_retries: u32,
    pub use_jitter: bool,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_micros(100),
            max_retries: 4,
            use_jitter: true,
        }
    }
}

impl ContentionStrategy for ExponentialBackoff {
    fn backoff(&self, attempt: u32) -> Duration {
        let exp = self.base.as_micros() as u64 * (1u64 << (attempt.min(10) - 1).min(63));
        if self.use_jitter {
            let jitter_factor = 0.5 + (fastrand(exp) as f64 / u64::MAX as f64);
            Duration::from_micros((exp as f64 * jitter_factor) as u64)
        } else {
            Duration::from_micros(exp)
        }
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

/// Constant backoff -- same delay every retry.
#[derive(Debug, Clone)]
pub struct ConstantBackoff {
    pub delay: Duration,
    pub max_retries: u32,
}

impl Default for ConstantBackoff {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(1),
            max_retries: 4,
        }
    }
}

impl ContentionStrategy for ConstantBackoff {
    fn backoff(&self, _attempt: u32) -> Duration {
        self.delay
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

/// Simple XOR-based pseudo-random for jitter.
fn fastrand(seed: u64) -> u64 {
    let mut x = seed;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_no_jitter() {
        let strategy = ExponentialBackoff {
            base: Duration::from_micros(100),
            max_retries: 4,
            use_jitter: false,
        };
        assert_eq!(strategy.backoff(1), Duration::from_micros(100));
        assert_eq!(strategy.backoff(2), Duration::from_micros(200));
        assert_eq!(strategy.backoff(3), Duration::from_micros(400));
    }

    #[test]
    fn constant_backoff_same_every_time() {
        let strategy = ConstantBackoff {
            delay: Duration::from_millis(5),
            max_retries: 3,
        };
        assert_eq!(strategy.backoff(1), Duration::from_millis(5));
        assert_eq!(strategy.backoff(2), Duration::from_millis(5));
        assert_eq!(strategy.backoff(99), Duration::from_millis(5));
        assert_eq!(strategy.max_retries(), 3);
    }

    #[test]
    fn exponential_backoff_with_jitter_bounded() {
        let strategy = ExponentialBackoff {
            base: Duration::from_micros(100),
            max_retries: 4,
            use_jitter: true,
        };
        // With jitter, should still be in reasonable range
        let d = strategy.backoff(1);
        assert!(d.as_micros() >= 50 && d.as_micros() <= 200);
    }
}
