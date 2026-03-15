// Licensed under Apache License, Version 2.0.

//! Aggregated metrics facade for streaming, repair, and topology.

use serde::Serialize;

/// Aggregated metrics snapshot combining streaming and repair counters.
#[derive(Debug, Clone, Serialize)]
pub struct AdminMetrics {
    pub streaming: StreamingSnapshot,
    pub repair: RepairSnapshot,
    pub topology_state: String,
}

/// Streaming counters snapshot (sourced from cassandra-streaming).
#[derive(Debug, Clone, Serialize, Default)]
pub struct StreamingSnapshot {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub sessions_active: u64,
    pub sessions_completed: u64,
    pub sessions_failed: u64,
}

/// Repair counters snapshot (sourced from cassandra-repair).
#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairSnapshot {
    pub trees_built: u64,
    pub trees_exchanged: u64,
    pub ranges_repaired: u64,
    pub sessions_active: u64,
    pub sessions_completed: u64,
    pub sessions_failed: u64,
}

impl AdminMetrics {
    /// Build metrics from the individual component snapshots.
    pub fn from_components(
        streaming: StreamingSnapshot,
        repair: RepairSnapshot,
        topology_state: String,
    ) -> Self {
        Self {
            streaming,
            repair,
            topology_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_metrics_construction() {
        let m = AdminMetrics::from_components(
            StreamingSnapshot {
                bytes_sent: 1024,
                sessions_active: 1,
                ..Default::default()
            },
            RepairSnapshot::default(),
            "IDLE".to_string(),
        );

        assert_eq!(m.streaming.bytes_sent, 1024);
        assert_eq!(m.topology_state, "IDLE");
    }
}
