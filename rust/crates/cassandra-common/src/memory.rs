// Licensed under Apache License, Version 2.0.

//! Memory accounting and allocator helpers shared by Cassandra components.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.memory.MemtableAllocator`
//! - `org.apache.cassandra.utils.memory.NativeAllocator`
//! - `org.apache.cassandra.utils.memory.SlabAllocator`
//! - `org.apache.cassandra.utils.memory.MemtablePool`

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AllocationError {
    #[error("memtable memory limit exceeded: requested {requested}, used {used}, limit {limit}")]
    LimitExceeded {
        requested: usize,
        used: usize,
        limit: usize,
    },
}

#[derive(Debug)]
pub struct MemtableAllocation {
    bytes: Vec<u8>,
    charged_bytes: usize,
    allocated: Arc<AtomicUsize>,
}

impl MemtableAllocation {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl Drop for MemtableAllocation {
    fn drop(&mut self) {
        self.allocated
            .fetch_sub(self.charged_bytes, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct NativeAllocator {
    limit_bytes: usize,
    allocated: Arc<AtomicUsize>,
}

impl NativeAllocator {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            allocated: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn allocate(&self, size: usize) -> Result<MemtableAllocation, AllocationError> {
        reserve(&self.allocated, self.limit_bytes, size)?;
        Ok(MemtableAllocation {
            bytes: vec![0; size],
            charged_bytes: size,
            allocated: Arc::clone(&self.allocated),
        })
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }
}

#[derive(Debug, Clone)]
pub struct SlabAllocator {
    slab_size: usize,
    limit_bytes: usize,
    allocated: Arc<AtomicUsize>,
    reserved: Arc<AtomicUsize>,
    remaining_in_current_slab: Arc<Mutex<usize>>,
}

impl SlabAllocator {
    pub fn new(slab_size: usize, limit_bytes: usize) -> Self {
        Self {
            slab_size: slab_size.max(1),
            limit_bytes,
            allocated: Arc::new(AtomicUsize::new(0)),
            reserved: Arc::new(AtomicUsize::new(0)),
            remaining_in_current_slab: Arc::new(Mutex::new(0)),
        }
    }

    pub fn allocate(&self, size: usize) -> Result<MemtableAllocation, AllocationError> {
        reserve(&self.allocated, self.limit_bytes, size)?;
        let mut remaining = self
            .remaining_in_current_slab
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if *remaining < size {
            let slab_bytes = self.slab_size.max(size);
            self.reserved.fetch_add(slab_bytes, Ordering::Relaxed);
            *remaining = slab_bytes;
        }
        *remaining -= size;
        Ok(MemtableAllocation {
            bytes: vec![0; size],
            charged_bytes: size,
            allocated: Arc::clone(&self.allocated),
        })
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    pub fn reserved_bytes(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    pub fn slab_size(&self) -> usize {
        self.slab_size
    }
}

#[derive(Debug, Clone)]
pub struct MemtablePool {
    limit_bytes: usize,
    used: Arc<AtomicUsize>,
}

impl MemtablePool {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            used: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemtablePoolReservation, AllocationError> {
        reserve(&self.used, self.limit_bytes, bytes)?;
        Ok(MemtablePoolReservation {
            bytes,
            used: Arc::clone(&self.used),
        })
    }

    pub fn used_bytes(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn available_bytes(&self) -> usize {
        self.limit_bytes.saturating_sub(self.used_bytes())
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }
}

pub struct MemtablePoolReservation {
    bytes: usize,
    used: Arc<AtomicUsize>,
}

impl MemtablePoolReservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for MemtablePoolReservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

fn reserve(counter: &AtomicUsize, limit_bytes: usize, bytes: usize) -> Result<(), AllocationError> {
    let mut used = counter.load(Ordering::Relaxed);
    loop {
        let next = used.saturating_add(bytes);
        if next > limit_bytes {
            return Err(AllocationError::LimitExceeded {
                requested: bytes,
                used,
                limit: limit_bytes,
            });
        }
        match counter.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
            Err(actual) => used = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_allocator_tracks_live_allocations() {
        let allocator = NativeAllocator::new(8);
        let mut allocation = allocator.allocate(4).unwrap();
        allocation.as_mut_slice().copy_from_slice(b"test");

        assert_eq!(allocation.as_slice(), b"test");
        assert_eq!(allocator.allocated_bytes(), 4);
        assert_eq!(
            allocator.allocate(5).unwrap_err(),
            AllocationError::LimitExceeded {
                requested: 5,
                used: 4,
                limit: 8,
            }
        );
        drop(allocation);
        assert_eq!(allocator.allocated_bytes(), 0);
    }

    #[test]
    fn slab_allocator_reserves_slab_chunks() {
        let allocator = SlabAllocator::new(8, 32);
        let first = allocator.allocate(3).unwrap();
        let second = allocator.allocate(4).unwrap();
        assert_eq!(allocator.allocated_bytes(), 7);
        assert_eq!(allocator.reserved_bytes(), 8);

        let large = allocator.allocate(12).unwrap();
        assert_eq!(allocator.reserved_bytes(), 20);
        drop((first, second, large));
        assert_eq!(allocator.allocated_bytes(), 0);
        assert_eq!(allocator.reserved_bytes(), 20);
    }

    #[test]
    fn memtable_pool_reservations_release_on_drop() {
        let pool = MemtablePool::new(10);
        let reservation = pool.try_reserve(6).unwrap();
        assert_eq!(reservation.bytes(), 6);
        assert_eq!(pool.used_bytes(), 6);
        assert_eq!(pool.available_bytes(), 4);
        assert!(pool.try_reserve(5).is_err());
        drop(reservation);
        assert_eq!(pool.used_bytes(), 0);
    }
}
