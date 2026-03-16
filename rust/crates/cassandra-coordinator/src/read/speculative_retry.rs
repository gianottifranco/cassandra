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

//! Speculative retry policy for reads.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.schema.SpeculativeRetryPolicy`
//! - `org.apache.cassandra.service.reads.SpeculativeRetryPolicy`

use std::fmt;
use std::time::Duration;

/// Speculative retry policy determines when to send an extra read request.
///
/// ## Java Oracle
///
/// `CREATE TABLE ... WITH speculative_retry = '...'`
///
/// Values:
/// - `'NONE'` — never speculate
/// - `'ALWAYS'` — always send a speculative request immediately
/// - `'99PERCENTILE'` — speculate after p99 read latency
/// - `'50ms'` — speculate after 50ms
#[derive(Debug, Clone, PartialEq)]
pub enum SpeculativeRetryPolicy {
    /// Never send speculative requests.
    None,
    /// Always send a speculative request immediately with the initial request.
    Always,
    /// Speculate after the given percentile of read latency.
    Percentile(f64),
    /// Speculate after a fixed duration.
    FixedDelay(Duration),
}

impl SpeculativeRetryPolicy {
    /// Parse from CQL string (case-insensitive).
    pub fn from_str_cql(s: &str) -> Option<Self> {
        let s = s.trim().to_uppercase();
        match s.as_str() {
            "NONE" => Some(Self::None),
            "ALWAYS" => Some(Self::Always),
            _ => {
                if s.ends_with("PERCENTILE") {
                    let num = s.trim_end_matches("PERCENTILE").trim();
                    num.parse::<f64>().ok().map(Self::Percentile)
                } else if s.ends_with("MS") {
                    let num = s.trim_end_matches("MS").trim();
                    num.parse::<u64>().ok().map(|ms| Self::FixedDelay(Duration::from_millis(ms)))
                } else {
                    std::option::Option::None
                }
            }
        }
    }

    /// Whether this policy may trigger speculative requests.
    pub fn may_speculate(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Get the delay before sending a speculative request, given current latency stats.
    ///
    /// Returns `None` for `None` policy, `Some(Duration::ZERO)` for `Always`,
    /// and the computed delay for `Percentile` and `FixedDelay`.
    pub fn speculative_delay(&self, percentile_latency_ms: impl Fn(f64) -> u64) -> Option<Duration> {
        match self {
            Self::None => std::option::Option::None,
            Self::Always => Some(Duration::ZERO),
            Self::Percentile(p) => {
                let latency_ms = percentile_latency_ms(*p);
                Some(Duration::from_millis(latency_ms))
            }
            Self::FixedDelay(d) => Some(*d),
        }
    }

    /// Default policy: 99th percentile.
    pub fn default_policy() -> Self {
        Self::Percentile(99.0)
    }
}

impl Default for SpeculativeRetryPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

impl fmt::Display for SpeculativeRetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "NONE"),
            Self::Always => write!(f, "ALWAYS"),
            Self::Percentile(p) => write!(f, "{p}PERCENTILE"),
            Self::FixedDelay(d) => write!(f, "{}ms", d.as_millis()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_none() {
        assert_eq!(
            SpeculativeRetryPolicy::from_str_cql("NONE"),
            Some(SpeculativeRetryPolicy::None)
        );
        assert_eq!(
            SpeculativeRetryPolicy::from_str_cql("none"),
            Some(SpeculativeRetryPolicy::None)
        );
    }

    #[test]
    fn parse_always() {
        assert_eq!(
            SpeculativeRetryPolicy::from_str_cql("ALWAYS"),
            Some(SpeculativeRetryPolicy::Always)
        );
    }

    #[test]
    fn parse_percentile() {
        let p = SpeculativeRetryPolicy::from_str_cql("99PERCENTILE").unwrap();
        assert_eq!(p, SpeculativeRetryPolicy::Percentile(99.0));
    }

    #[test]
    fn parse_fixed_delay() {
        let p = SpeculativeRetryPolicy::from_str_cql("50ms").unwrap();
        assert_eq!(p, SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(50)));
    }

    #[test]
    fn may_speculate() {
        assert!(!SpeculativeRetryPolicy::None.may_speculate());
        assert!(SpeculativeRetryPolicy::Always.may_speculate());
        assert!(SpeculativeRetryPolicy::Percentile(99.0).may_speculate());
        assert!(SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(50)).may_speculate());
    }

    #[test]
    fn speculative_delay_none() {
        let policy = SpeculativeRetryPolicy::None;
        assert!(policy.speculative_delay(|_| 100).is_none());
    }

    #[test]
    fn speculative_delay_always_zero() {
        let policy = SpeculativeRetryPolicy::Always;
        assert_eq!(policy.speculative_delay(|_| 100), Some(Duration::ZERO));
    }

    #[test]
    fn speculative_delay_percentile() {
        let policy = SpeculativeRetryPolicy::Percentile(99.0);
        let delay = policy.speculative_delay(|p| {
            assert!((p - 99.0).abs() < f64::EPSILON);
            42
        });
        assert_eq!(delay, Some(Duration::from_millis(42)));
    }

    #[test]
    fn speculative_delay_fixed() {
        let policy = SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(50));
        assert_eq!(policy.speculative_delay(|_| 0), Some(Duration::from_millis(50)));
    }

    #[test]
    fn display() {
        assert_eq!(SpeculativeRetryPolicy::None.to_string(), "NONE");
        assert_eq!(SpeculativeRetryPolicy::Always.to_string(), "ALWAYS");
        assert_eq!(SpeculativeRetryPolicy::Percentile(99.0).to_string(), "99PERCENTILE");
        assert_eq!(
            SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(50)).to_string(),
            "50ms"
        );
    }
}
