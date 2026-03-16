// Licensed under Apache License, Version 2.0.

//! Backpressure management for native transport.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.CQLMessageHandler` (bytes_in_flight, throwOnOverload)
//! - `org.apache.cassandra.net.RateLimiter`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::warn;

/// Per-connection backpressure tracking.
pub struct BackpressureManager {
    /// Per-connection bytes in flight.
    bytes_in_flight: AtomicU64,
    /// Per-connection limit.
    max_bytes_in_flight: u64,
    /// Queue depth (outstanding requests).
    queue_depth: AtomicU64,
    /// Maximum queue depth before applying backpressure.
    max_queue_depth: u64,
    /// Notify waiters when capacity is released.
    capacity_available: Arc<Notify>,
}

impl BackpressureManager {
    pub fn new(max_bytes_in_flight: u64, max_queue_depth: u64) -> Self {
        Self {
            bytes_in_flight: AtomicU64::new(0),
            max_bytes_in_flight,
            queue_depth: AtomicU64::new(0),
            max_queue_depth,
            capacity_available: Arc::new(Notify::new()),
        }
    }

    /// Try to acquire capacity for a request of `size` bytes.
    /// Returns `true` if acquired, `false` if backpressure should be applied.
    pub fn try_acquire(&self, size: u64) -> bool {
        // Check queue depth
        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel);
        if depth >= self.max_queue_depth {
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            warn!(depth, max = self.max_queue_depth, "Queue depth limit hit");
            return false;
        }

        // Check bytes in flight
        loop {
            let current = self.bytes_in_flight.load(Ordering::Acquire);
            let new = current + size;
            if new > self.max_bytes_in_flight {
                // Roll back queue depth
                self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                return false;
            }
            if self
                .bytes_in_flight
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release capacity after a request completes.
    pub fn release(&self, size: u64) {
        self.bytes_in_flight.fetch_sub(size, Ordering::AcqRel);
        self.queue_depth.fetch_sub(1, Ordering::AcqRel);
        self.capacity_available.notify_one();
    }

    /// Wait until capacity becomes available (for pause/resume pattern).
    pub async fn wait_for_capacity(&self) {
        self.capacity_available.notified().await;
    }

    /// Current bytes in flight for this connection.
    pub fn bytes_in_flight(&self) -> u64 {
        self.bytes_in_flight.load(Ordering::Relaxed)
    }

    /// Current queue depth.
    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }
}

/// Token-bucket rate limiter.
pub struct RateLimiter {
    /// Maximum tokens (requests) per second.
    max_per_second: u64,
    /// Current available tokens.
    tokens: AtomicU64,
    /// Notify when tokens are refilled.
    refill_notify: Arc<Notify>,
}

impl RateLimiter {
    pub fn new(max_per_second: u32) -> Self {
        Self {
            max_per_second: max_per_second as u64,
            tokens: AtomicU64::new(max_per_second as u64),
            refill_notify: Arc::new(Notify::new()),
        }
    }

    /// Try to consume one token. Returns `true` if allowed.
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Wait until a token becomes available.
    pub async fn acquire(&self) {
        loop {
            if self.try_acquire() {
                return;
            }
            self.refill_notify.notified().await;
        }
    }

    /// Refill tokens. Should be called by a periodic background task.
    pub fn refill(&self) {
        self.tokens.store(self.max_per_second, Ordering::Release);
        self.refill_notify.notify_waiters();
    }

    /// Spawn a background refill task that runs every second.
    pub fn spawn_refill_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let limiter = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                limiter.refill();
            }
        })
    }

    pub fn available_tokens(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backpressure_acquire_release() {
        let bp = BackpressureManager::new(100, 10);
        assert!(bp.try_acquire(50));
        assert_eq!(bp.bytes_in_flight(), 50);
        assert_eq!(bp.queue_depth(), 1);
        assert!(bp.try_acquire(50));
        assert_eq!(bp.bytes_in_flight(), 100);
        // Over limit
        assert!(!bp.try_acquire(1));
        bp.release(50);
        assert_eq!(bp.bytes_in_flight(), 50);
        assert_eq!(bp.queue_depth(), 1);
        assert!(bp.try_acquire(50));
    }

    #[test]
    fn queue_depth_limit() {
        let bp = BackpressureManager::new(u64::MAX, 2);
        assert!(bp.try_acquire(1));
        assert!(bp.try_acquire(1));
        assert!(!bp.try_acquire(1)); // queue depth = 2, max = 2
        bp.release(1);
        assert!(bp.try_acquire(1));
    }

    #[test]
    fn rate_limiter_basic() {
        let rl = RateLimiter::new(3);
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        assert!(!rl.try_acquire()); // exhausted
        rl.refill();
        assert!(rl.try_acquire());
        assert_eq!(rl.available_tokens(), 2);
    }

    #[tokio::test]
    async fn backpressure_notify() {
        let bp = Arc::new(BackpressureManager::new(10, 10));
        assert!(bp.try_acquire(10));

        let bp2 = Arc::clone(&bp);
        let handle = tokio::spawn(async move {
            bp2.wait_for_capacity().await;
            assert!(bp2.try_acquire(5));
        });

        tokio::task::yield_now().await;
        bp.release(10);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn rate_limiter_acquire_blocking() {
        let rl = Arc::new(RateLimiter::new(1));
        rl.try_acquire(); // exhaust

        let rl2 = Arc::clone(&rl);
        let handle = tokio::spawn(async move {
            rl2.acquire().await;
        });

        tokio::task::yield_now().await;
        rl.refill();
        handle.await.unwrap();
    }
}
