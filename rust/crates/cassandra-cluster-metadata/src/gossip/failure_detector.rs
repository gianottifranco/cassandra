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

//! Phi-accrual failure detector.
//!
//! Uses inter-arrival times of heartbeats to compute a suspicion level (phi).
//! When phi exceeds the configured threshold, the endpoint is considered dead.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.FailureDetector`
//! - `org.apache.cassandra.gms.ArrivalWindow`

use std::collections::HashMap;
use std::time::Instant;

use crate::node::Endpoint;

/// Maximum number of samples in the arrival window.
const MAX_WINDOW_SIZE: usize = 1000;

/// Minimum interval (milliseconds) — avoids division by zero or extreme phi.
const MIN_INTERVAL_MS: f64 = 10.0;

/// Bounded window of inter-arrival times for phi calculation.
#[derive(Debug, Clone)]
struct ArrivalWindow {
    /// Ring buffer of inter-arrival times in milliseconds.
    intervals: Vec<f64>,
    /// Last arrival time.
    last_arrival: Option<Instant>,
}

impl ArrivalWindow {
    fn new() -> Self {
        Self {
            intervals: Vec::with_capacity(MAX_WINDOW_SIZE),
            last_arrival: None,
        }
    }

    /// Record a heartbeat arrival.
    fn add(&mut self, now: Instant) {
        if let Some(last) = self.last_arrival {
            let interval = now.duration_since(last).as_secs_f64() * 1000.0;
            let interval = interval.max(MIN_INTERVAL_MS);

            if self.intervals.len() >= MAX_WINDOW_SIZE {
                self.intervals.remove(0);
            }
            self.intervals.push(interval);
        }
        self.last_arrival = Some(now);
    }

    /// Compute phi: the suspicion level.
    ///
    /// Phi is calculated using the exponential distribution on inter-arrival
    /// times: `phi = -log10(P(t_now - t_last > observed_interval))`.
    ///
    /// Higher phi = more suspicious (longer silence).
    fn phi(&self, now: Instant) -> f64 {
        if self.intervals.is_empty() {
            return 0.0;
        }

        let last = match self.last_arrival {
            Some(t) => t,
            None => return 0.0,
        };

        let t = now.duration_since(last).as_secs_f64() * 1000.0;
        let mean = self.mean();

        if mean <= 0.0 {
            return 0.0;
        }

        // Exponential CDF: P(X > t) = e^(-t/mean)
        // phi = -log10(P(X > t)) = t / (mean * ln(10))
        let phi = t / (mean * std::f64::consts::LN_10);
        phi.max(0.0)
    }

    /// Mean inter-arrival time.
    fn mean(&self) -> f64 {
        if self.intervals.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.intervals.iter().sum();
        sum / self.intervals.len() as f64
    }
}

/// Phi-accrual failure detector for the cluster.
///
/// Each endpoint has an `ArrivalWindow` that tracks heartbeat intervals.
/// Call `report(endpoint)` when a heartbeat arrives, and `phi(endpoint)`
/// to get the suspicion level.
#[derive(Debug)]
pub struct FailureDetector {
    /// Per-endpoint arrival windows.
    windows: HashMap<Endpoint, ArrivalWindow>,
    /// Phi threshold above which an endpoint is considered dead.
    phi_threshold: f64,
}

impl FailureDetector {
    /// Create a new failure detector with the given phi threshold.
    ///
    /// Java default is 8.0. Higher values are more lenient (tolerate longer
    /// pauses). Lower values detect failures faster but may false-positive.
    pub fn new(phi_threshold: f64) -> Self {
        Self {
            windows: HashMap::new(),
            phi_threshold,
        }
    }

    /// Report a heartbeat from an endpoint.
    pub fn report(&mut self, endpoint: Endpoint) {
        let window = self
            .windows
            .entry(endpoint)
            .or_insert_with(ArrivalWindow::new);
        window.add(Instant::now());
    }

    /// Report a heartbeat at a specific time (for testing).
    pub fn report_at(&mut self, endpoint: Endpoint, at: Instant) {
        let window = self
            .windows
            .entry(endpoint)
            .or_insert_with(ArrivalWindow::new);
        window.add(at);
    }

    /// Get the current phi value for an endpoint.
    ///
    /// Returns 0.0 if no heartbeats have been recorded.
    pub fn phi(&self, endpoint: &Endpoint) -> f64 {
        self.windows
            .get(endpoint)
            .map(|w| w.phi(Instant::now()))
            .unwrap_or(0.0)
    }

    /// Get phi at a specific time (for testing).
    pub fn phi_at(&self, endpoint: &Endpoint, at: Instant) -> f64 {
        self.windows.get(endpoint).map(|w| w.phi(at)).unwrap_or(0.0)
    }

    /// Returns `true` if the endpoint is considered alive.
    pub fn is_alive(&self, endpoint: &Endpoint) -> bool {
        let phi = self.phi(endpoint);
        // Phi of 0 means no data — treat as alive (benefit of the doubt)
        phi == 0.0 || phi < self.phi_threshold
    }

    /// Clear all state for the given endpoint.
    pub fn remove(&mut self, endpoint: &Endpoint) {
        self.windows.remove(endpoint);
    }

    /// Get the configured phi threshold.
    pub fn threshold(&self) -> f64 {
        self.phi_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn unknown_endpoint_has_zero_phi() {
        let fd = FailureDetector::new(8.0);
        assert_eq!(fd.phi(&ep(7001)), 0.0);
        assert!(fd.is_alive(&ep(7001))); // benefit of the doubt
    }

    #[test]
    fn regular_heartbeats_keep_alive() {
        let mut fd = FailureDetector::new(8.0);
        let now = Instant::now();

        // Simulate regular 1-second heartbeats
        for i in 0..10 {
            fd.report_at(ep(7001), now + Duration::from_secs(i));
        }

        // Check right after the last heartbeat
        let phi = fd.phi_at(&ep(7001), now + Duration::from_secs(10));
        // Should be moderate — 1 second since last beat with 1s mean
        assert!(phi < 8.0, "phi={phi} should be < 8.0");
    }

    #[test]
    fn missed_heartbeats_increase_phi() {
        let mut fd = FailureDetector::new(8.0);
        let now = Instant::now();

        // Regular heartbeats every 1 second
        for i in 0..10 {
            fd.report_at(ep(7001), now + Duration::from_secs(i));
        }

        // Check 30 seconds after last heartbeat (mean is ~1s)
        let phi = fd.phi_at(&ep(7001), now + Duration::from_secs(39));
        assert!(phi > 8.0, "phi={phi} should be > 8.0 after 30s silence");
    }

    #[test]
    fn arrival_window_mean() {
        let mut w = ArrivalWindow::new();
        let now = Instant::now();

        w.add(now);
        w.add(now + Duration::from_millis(1000));
        w.add(now + Duration::from_millis(2000));

        // Two intervals of ~1000ms each
        let mean = w.mean();
        assert!((mean - 1000.0).abs() < 50.0, "mean={mean}");
    }

    #[test]
    fn failure_detector_report_and_remove() {
        let mut fd = FailureDetector::new(8.0);
        fd.report(ep(7001));
        fd.report(ep(7001));
        assert!(fd.is_alive(&ep(7001)));

        fd.remove(&ep(7001));
        // After removal, phi is 0 → treated as alive
        assert!(fd.is_alive(&ep(7001)));
    }
}
