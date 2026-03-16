// Licensed under Apache License, Version 2.0.

//! Graceful shutdown coordination.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.CassandraDaemon.stop()`
//! - `org.apache.cassandra.transport.Server.stop()`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Coordinates graceful shutdown across the server.
///
/// Uses a `CancellationToken` to signal all listeners and connection
/// handlers, then waits for in-flight requests to drain.
pub struct ShutdownCoordinator {
    /// Token that signals all tasks to begin shutting down.
    token: CancellationToken,
    /// Number of in-flight requests being processed.
    in_flight: Arc<AtomicU64>,
    /// Maximum time to wait for in-flight requests to drain.
    drain_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(drain_timeout: Duration) -> Self {
        Self {
            token: CancellationToken::new(),
            in_flight: Arc::new(AtomicU64::new(0)),
            drain_timeout,
        }
    }

    /// Get a clone of the cancellation token for use in `tokio::select!`.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Check if shutdown has been signalled.
    pub fn is_shutting_down(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Signal all tasks to shut down.
    pub fn signal_shutdown(&self) {
        info!("Shutdown signal received");
        self.token.cancel();
    }

    /// Increment in-flight request counter. Returns a guard that
    /// decrements on drop.
    pub fn track_request(&self) -> InFlightGuard {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlightGuard {
            counter: Arc::clone(&self.in_flight),
        }
    }

    /// Current number of in-flight requests.
    pub fn in_flight_count(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Wait for all in-flight requests to complete, up to the drain timeout.
    /// Returns `true` if all drained, `false` if timed out.
    pub async fn drain(&self) -> bool {
        let deadline = tokio::time::Instant::now() + self.drain_timeout;
        let mut interval = tokio::time::interval(Duration::from_millis(50));

        loop {
            let count = self.in_flight.load(Ordering::Acquire);
            if count == 0 {
                info!("All in-flight requests drained");
                return true;
            }

            if tokio::time::Instant::now() >= deadline {
                warn!(remaining = count, "Drain timeout reached with in-flight requests");
                return false;
            }

            interval.tick().await;
        }
    }
}

/// RAII guard that decrements the in-flight counter on drop.
pub struct InFlightGuard {
    counter: Arc<AtomicU64>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Install OS signal handlers (SIGTERM, SIGINT) that trigger shutdown.
pub async fn signal_handler(coordinator: Arc<ShutdownCoordinator>) {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {
                info!("Received SIGINT (Ctrl+C)");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C");
    }

    coordinator.signal_shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_and_check() {
        let sc = ShutdownCoordinator::new(Duration::from_secs(5));
        assert!(!sc.is_shutting_down());
        sc.signal_shutdown();
        assert!(sc.is_shutting_down());
        assert!(sc.token().is_cancelled());
    }

    #[test]
    fn in_flight_tracking() {
        let sc = ShutdownCoordinator::new(Duration::from_secs(5));
        assert_eq!(sc.in_flight_count(), 0);
        let g1 = sc.track_request();
        let g2 = sc.track_request();
        assert_eq!(sc.in_flight_count(), 2);
        drop(g1);
        assert_eq!(sc.in_flight_count(), 1);
        drop(g2);
        assert_eq!(sc.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn drain_with_no_inflight() {
        let sc = ShutdownCoordinator::new(Duration::from_secs(1));
        assert!(sc.drain().await);
    }

    #[tokio::test]
    async fn drain_with_inflight_completion() {
        let sc = Arc::new(ShutdownCoordinator::new(Duration::from_secs(5)));
        let guard = sc.track_request();

        let sc2 = Arc::clone(&sc);
        let handle = tokio::spawn(async move { sc2.drain().await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(guard);

        assert!(handle.await.unwrap());
    }

    #[tokio::test]
    async fn drain_timeout() {
        let sc = ShutdownCoordinator::new(Duration::from_millis(100));
        let _guard = sc.track_request(); // never dropped
        assert!(!sc.drain().await);
    }
}
