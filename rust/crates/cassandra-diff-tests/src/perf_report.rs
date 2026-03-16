// Licensed under Apache License, Version 2.0.

//! Performance report generator.
//!
//! Provides structured latency/throughput/memory metrics with comparison
//! against the performance budgets defined in ADR-015.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A single performance measurement with percentile breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMeasurement {
    pub name: String,
    pub sample_count: u64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
}

/// A throughput measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputMeasurement {
    pub name: String,
    pub ops_per_sec: f64,
    pub duration: Duration,
    pub total_ops: u64,
}

/// A memory measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMeasurement {
    pub name: String,
    pub initial_bytes: usize,
    pub final_bytes: usize,
    pub peak_bytes: usize,
    pub growth_ratio: f64,
}

/// Performance budget from ADR-015.
#[derive(Debug, Clone)]
pub struct LatencyBudget {
    pub name: &'static str,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

/// Check a latency measurement against its budget.
#[derive(Debug, Clone, Serialize)]
pub struct BudgetCheckResult {
    pub name: String,
    pub passed: bool,
    pub p50_pass: bool,
    pub p99_pass: bool,
    pub p999_pass: bool,
    pub details: String,
}

/// ADR-015 performance budgets.
pub mod budgets {
    use super::LatencyBudget;

    pub const MEMTABLE_WRITE: LatencyBudget = LatencyBudget {
        name: "single_row_write",
        p50_us: 20,
        p99_us: 100,
        p999_us: 500,
    };

    pub const MEMTABLE_READ: LatencyBudget = LatencyBudget {
        name: "memtable_read",
        p50_us: 50,
        p99_us: 200,
        p999_us: 1000,
    };

    pub const SSTABLE_READ_WARM: LatencyBudget = LatencyBudget {
        name: "sstable_read_warm",
        p50_us: 200,
        p99_us: 1000,
        p999_us: 5000,
    };

    pub const FRAME_PARSE: LatencyBudget = LatencyBudget {
        name: "frame_parse",
        p50_us: 1,
        p99_us: 5,
        p999_us: 20,
    };

    pub const FRAME_ENCODE: LatencyBudget = LatencyBudget {
        name: "frame_encode",
        p50_us: 2,
        p99_us: 10,
        p999_us: 50,
    };

    pub const THROUGHPUT_SEQ_WRITES_OPS: f64 = 100_000.0;
    pub const THROUGHPUT_SEQ_READS_OPS: f64 = 200_000.0;
    pub const THROUGHPUT_MIXED_OPS: f64 = 80_000.0;
    pub const MEMORY_GROWTH_RATIO: f64 = 1.05;
}

/// Compute percentiles from a sorted slice of microsecond samples.
pub fn compute_latency(name: &str, samples: &mut Vec<u64>) -> LatencyMeasurement {
    samples.sort();
    let len = samples.len();
    if len == 0 {
        return LatencyMeasurement {
            name: name.to_string(),
            sample_count: 0,
            p50_us: 0,
            p99_us: 0,
            p999_us: 0,
            min_us: 0,
            max_us: 0,
            mean_us: 0.0,
        };
    }

    let sum: u64 = samples.iter().sum();

    LatencyMeasurement {
        name: name.to_string(),
        sample_count: len as u64,
        p50_us: samples[len / 2],
        p99_us: samples[(len as f64 * 0.99) as usize],
        p999_us: samples[((len as f64 * 0.999) as usize).min(len - 1)],
        min_us: samples[0],
        max_us: samples[len - 1],
        mean_us: sum as f64 / len as f64,
    }
}

/// Check a latency measurement against a budget.
pub fn check_budget(measurement: &LatencyMeasurement, budget: &LatencyBudget) -> BudgetCheckResult {
    let p50_pass = measurement.p50_us <= budget.p50_us;
    let p99_pass = measurement.p99_us <= budget.p99_us;
    let p999_pass = measurement.p999_us <= budget.p999_us;
    let passed = p50_pass && p99_pass && p999_pass;

    let details = format!(
        "p50: {}μs/{budget_p50}μs {p50_icon} | p99: {}μs/{budget_p99}μs {p99_icon} | p999: {}μs/{budget_p999}μs {p999_icon}",
        measurement.p50_us,
        measurement.p99_us,
        measurement.p999_us,
        budget_p50 = budget.p50_us,
        budget_p99 = budget.p99_us,
        budget_p999 = budget.p999_us,
        p50_icon = if p50_pass { "✅" } else { "❌" },
        p99_icon = if p99_pass { "✅" } else { "❌" },
        p999_icon = if p999_pass { "✅" } else { "❌" },
    );

    BudgetCheckResult {
        name: measurement.name.clone(),
        passed,
        p50_pass,
        p99_pass,
        p999_pass,
        details,
    }
}

/// Full performance report.
#[derive(Debug, Clone, Serialize)]
pub struct PerfReport {
    pub timestamp: String,
    pub latency_results: Vec<BudgetCheckResult>,
    pub throughput_measurements: Vec<ThroughputMeasurement>,
    pub memory_measurements: Vec<MemoryMeasurement>,
    pub overall_pass: bool,
}

impl PerfReport {
    pub fn new() -> Self {
        Self {
            timestamp: chrono_now(),
            latency_results: Vec::new(),
            throughput_measurements: Vec::new(),
            memory_measurements: Vec::new(),
            overall_pass: true,
        }
    }

    pub fn add_latency_check(&mut self, result: BudgetCheckResult) {
        if !result.passed {
            self.overall_pass = false;
        }
        self.latency_results.push(result);
    }

    pub fn add_throughput(&mut self, m: ThroughputMeasurement) {
        self.throughput_measurements.push(m);
    }

    pub fn add_memory(&mut self, m: MemoryMeasurement) {
        if m.growth_ratio > budgets::MEMORY_GROWTH_RATIO {
            self.overall_pass = false;
        }
        self.memory_measurements.push(m);
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn print_summary(&self) {
        println!("\n═══════════════════════════════════════════════════════════");
        println!("  Performance Budget Report — {}", self.timestamp);
        println!("═══════════════════════════════════════════════════════════\n");

        for r in &self.latency_results {
            println!("  {} {}", if r.passed { "✅" } else { "❌" }, r.details);
        }

        for t in &self.throughput_measurements {
            println!(
                "  📊 {}: {:.0} ops/s ({} ops in {:?})",
                t.name, t.ops_per_sec, t.total_ops, t.duration
            );
        }

        for m in &self.memory_measurements {
            println!(
                "  🧠 {}: growth={:.2}x (initial={}B, peak={}B)",
                m.name, m.growth_ratio, m.initial_bytes, m.peak_bytes
            );
        }

        println!(
            "\n  Overall: {}",
            if self.overall_pass {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
        println!("═══════════════════════════════════════════════════════════\n");
    }
}

fn chrono_now() -> String {
    // Simple timestamp without chrono dependency
    format!(
        "{:?}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_latency_basic() {
        let mut samples = vec![100, 50, 200, 10, 500, 80, 120, 90, 300, 150];
        let m = compute_latency("test_op", &mut samples);
        assert_eq!(m.sample_count, 10);
        assert_eq!(m.min_us, 10);
        assert_eq!(m.max_us, 500);
        assert!(m.p50_us <= m.p99_us);
        assert!(m.p99_us <= m.p999_us);
    }

    #[test]
    fn budget_check_pass() {
        let m = LatencyMeasurement {
            name: "test".to_string(),
            sample_count: 100,
            p50_us: 10,
            p99_us: 50,
            p999_us: 200,
            min_us: 5,
            max_us: 200,
            mean_us: 30.0,
        };
        let result = check_budget(&m, &budgets::MEMTABLE_WRITE);
        assert!(result.passed);
    }

    #[test]
    fn budget_check_fail() {
        let m = LatencyMeasurement {
            name: "test".to_string(),
            sample_count: 100,
            p50_us: 100, // exceeds budget of 20μs
            p99_us: 500,
            p999_us: 2000,
            min_us: 50,
            max_us: 2000,
            mean_us: 200.0,
        };
        let result = check_budget(&m, &budgets::MEMTABLE_WRITE);
        assert!(!result.passed);
    }
}
