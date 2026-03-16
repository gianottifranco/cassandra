// Licensed under Apache License, Version 2.0.

//! Connection and memory resource limits.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.ClientResourceLimits`
//! - `org.apache.cassandra.net.ResourceLimits`

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Semaphore;
use tracing::warn;

/// Global + per-IP resource limits for native transport connections.
pub struct ResourceLimits {
    /// Semaphore controlling global connection count. `None` = unlimited.
    global_conn_semaphore: Option<Arc<Semaphore>>,
    /// Per-IP connection counts.
    per_ip_counts: Arc<DashMap<IpAddr, u32>>,
    /// Max connections per IP. `0` = unlimited.
    max_per_ip: u32,
    /// Global bytes-in-flight tracker.
    global_bytes_in_flight: Arc<AtomicU64>,
    /// Max global bytes in flight.
    max_bytes_in_flight: u64,
}

impl ResourceLimits {
    /// Create resource limits.
    ///
    /// - `max_connections`: -1 or 0 = unlimited
    /// - `max_per_ip`: -1 or 0 = unlimited
    /// - `max_bytes_in_flight`: max request data in-flight globally
    pub fn new(max_connections: i32, max_per_ip: i32, max_bytes_in_flight: u64) -> Self {
        let global_conn_semaphore = if max_connections > 0 {
            Some(Arc::new(Semaphore::new(max_connections as usize)))
        } else {
            None
        };
        let max_per_ip = if max_per_ip > 0 {
            max_per_ip as u32
        } else {
            0
        };
        Self {
            global_conn_semaphore,
            per_ip_counts: Arc::new(DashMap::new()),
            max_per_ip,
            global_bytes_in_flight: Arc::new(AtomicU64::new(0)),
            max_bytes_in_flight,
        }
    }

    /// Try to acquire a connection permit. Returns `None` if limits are
    /// exceeded (either global or per-IP).
    pub fn try_acquire_connection(&self, ip: IpAddr) -> Option<ConnectionPermit> {
        // Check per-IP limit first
        if self.max_per_ip > 0 {
            let mut entry = self.per_ip_counts.entry(ip).or_insert(0);
            if *entry >= self.max_per_ip {
                warn!(%ip, limit = self.max_per_ip, "Per-IP connection limit reached");
                return None;
            }
            *entry += 1;
        }

        // Check global limit
        if let Some(ref sem) = self.global_conn_semaphore {
            match sem.clone().try_acquire_owned() {
                Ok(permit) => Some(ConnectionPermit {
                    _global_permit: Some(permit),
                    ip,
                    per_ip_counts: Arc::clone(&self.per_ip_counts),
                    has_per_ip: self.max_per_ip > 0,
                }),
                Err(_) => {
                    // Roll back per-IP increment
                    if self.max_per_ip > 0 {
                        if let Some(mut entry) = self.per_ip_counts.get_mut(&ip) {
                            *entry = entry.saturating_sub(1);
                        }
                    }
                    warn!("Global connection limit reached");
                    None
                }
            }
        } else {
            Some(ConnectionPermit {
                _global_permit: None,
                ip,
                per_ip_counts: Arc::clone(&self.per_ip_counts),
                has_per_ip: self.max_per_ip > 0,
            })
        }
    }

    /// Try to acquire bytes for in-flight request data. Returns `None` if
    /// the global limit would be exceeded.
    pub fn try_acquire_bytes(&self, size: u64) -> Option<BytesPermit> {
        loop {
            let current = self.global_bytes_in_flight.load(Ordering::Acquire);
            let new = current + size;
            if new > self.max_bytes_in_flight {
                return None;
            }
            if self
                .global_bytes_in_flight
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(BytesPermit {
                    size,
                    tracker: Arc::clone(&self.global_bytes_in_flight),
                });
            }
        }
    }

    /// Current global bytes in flight.
    pub fn bytes_in_flight(&self) -> u64 {
        self.global_bytes_in_flight.load(Ordering::Relaxed)
    }

    /// Current connection count for a given IP.
    pub fn connections_for_ip(&self, ip: &IpAddr) -> u32 {
        self.per_ip_counts.get(ip).map(|v| *v).unwrap_or(0)
    }
}

/// RAII guard that releases a connection slot on drop.
pub struct ConnectionPermit {
    _global_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ip: IpAddr,
    per_ip_counts: Arc<DashMap<IpAddr, u32>>,
    has_per_ip: bool,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        if self.has_per_ip {
            if let Some(mut entry) = self.per_ip_counts.get_mut(&self.ip) {
                *entry = entry.saturating_sub(1);
                if *entry == 0 {
                    drop(entry);
                    self.per_ip_counts.remove(&self.ip);
                }
            }
        }
    }
}

/// RAII guard that releases bytes-in-flight on drop.
pub struct BytesPermit {
    size: u64,
    tracker: Arc<AtomicU64>,
}

impl Drop for BytesPermit {
    fn drop(&mut self) {
        self.tracker.fetch_sub(self.size, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_connections() {
        let rl = ResourceLimits::new(-1, -1, 1024);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let p1 = rl.try_acquire_connection(ip);
        let p2 = rl.try_acquire_connection(ip);
        assert!(p1.is_some());
        assert!(p2.is_some());
    }

    #[test]
    fn global_limit_enforced() {
        let rl = ResourceLimits::new(2, -1, 1024);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let _p1 = rl.try_acquire_connection(ip).unwrap();
        let _p2 = rl.try_acquire_connection(ip).unwrap();
        assert!(rl.try_acquire_connection(ip).is_none());
    }

    #[test]
    fn per_ip_limit_enforced() {
        let rl = ResourceLimits::new(-1, 1, 1024);
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();
        let _p1 = rl.try_acquire_connection(ip1).unwrap();
        assert!(rl.try_acquire_connection(ip1).is_none());
        // Different IP should still work
        let _p2 = rl.try_acquire_connection(ip2).unwrap();
    }

    #[test]
    fn raii_connection_drop() {
        let rl = ResourceLimits::new(1, 1, 1024);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        {
            let _p = rl.try_acquire_connection(ip).unwrap();
            assert!(rl.try_acquire_connection(ip).is_none());
        }
        // After drop, should be able to acquire again
        assert!(rl.try_acquire_connection(ip).is_some());
    }

    #[test]
    fn bytes_acquire_release() {
        let rl = ResourceLimits::new(-1, -1, 100);
        let p1 = rl.try_acquire_bytes(60).unwrap();
        assert_eq!(rl.bytes_in_flight(), 60);
        assert!(rl.try_acquire_bytes(60).is_none());
        let _p2 = rl.try_acquire_bytes(40).unwrap();
        assert_eq!(rl.bytes_in_flight(), 100);
        drop(p1);
        assert_eq!(rl.bytes_in_flight(), 40);
    }

    #[test]
    fn per_ip_isolation() {
        let rl = ResourceLimits::new(-1, 2, 1024);
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();
        let _p1 = rl.try_acquire_connection(ip1).unwrap();
        let _p2 = rl.try_acquire_connection(ip1).unwrap();
        assert_eq!(rl.connections_for_ip(&ip1), 2);
        assert_eq!(rl.connections_for_ip(&ip2), 0);
        let _p3 = rl.try_acquire_connection(ip2).unwrap();
        assert_eq!(rl.connections_for_ip(&ip2), 1);
    }
}
