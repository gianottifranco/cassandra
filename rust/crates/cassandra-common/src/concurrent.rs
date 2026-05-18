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

//! Concurrency primitives mirroring the Java utility layer.

use std::{
    collections::{BTreeMap, HashSet},
    ops::Deref,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

type CloseCallback = Box<dyn FnOnce() + Send + 'static>;
type RefReleaseCallback<T> = Box<dyn FnOnce(&T) + Send + 'static>;

pub struct Ref<T> {
    state: Arc<RefState<T>>,
}

struct RefState<T> {
    value: T,
    released: AtomicBool,
    on_release: Mutex<Option<RefReleaseCallback<T>>>,
}

impl<T> Ref<T> {
    pub fn new(value: T) -> Self {
        Self::with_on_release(value, |_| {})
    }

    pub fn with_on_release<F>(value: T, on_release: F) -> Self
    where
        F: FnOnce(&T) + Send + 'static,
    {
        Self {
            state: Arc::new(RefState {
                value,
                released: AtomicBool::new(false),
                on_release: Mutex::new(Some(Box::new(on_release))),
            }),
        }
    }

    pub fn get(&self) -> &T {
        &self.state.value
    }

    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.state)
    }

    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::Acquire)
    }

    pub fn release(self) {
        drop(self);
    }
}

impl<T> Clone for Ref<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Deref for Ref<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<T> Drop for Ref<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) != 1 {
            return;
        }
        if self
            .state
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if let Some(on_release) = lock_unpoisoned(&self.state.on_release).take() {
            on_release(&self.state.value);
        }
    }
}

#[derive(Clone)]
pub struct SharedCloseable {
    state: Arc<SharedCloseableState>,
}

struct SharedCloseableState {
    closed: AtomicBool,
    on_close: Mutex<Option<CloseCallback>>,
}

impl SharedCloseable {
    pub fn new() -> Self {
        Self::with_on_close(|| {})
    }

    pub fn with_on_close<F>(on_close: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            state: Arc::new(SharedCloseableState {
                closed: AtomicBool::new(false),
                on_close: Mutex::new(Some(Box::new(on_close))),
            }),
        }
    }

    pub fn shared_copy(&self) -> Self {
        self.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub fn close(&self) -> bool {
        if self
            .state
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        if let Some(on_close) = lock_unpoisoned(&self.state.on_close).take() {
            on_close();
        }
        true
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.state)
    }
}

impl Default for SharedCloseable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Default)]
pub struct WaitQueue {
    state: Arc<WaitQueueState>,
}

#[derive(Default)]
struct WaitQueueState {
    inner: Mutex<WaitQueueInner>,
    condvar: Condvar,
}

#[derive(Default)]
struct WaitQueueInner {
    next_id: u64,
    waiting: Vec<u64>,
    signaled: HashSet<u64>,
}

#[derive(Clone)]
pub struct WaitToken {
    id: u64,
    state: Weak<WaitQueueState>,
}

impl WaitQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self) -> WaitToken {
        let mut inner = lock_unpoisoned(&self.state.inner);
        let id = inner.next_id;
        inner.next_id += 1;
        inner.waiting.push(id);
        WaitToken {
            id,
            state: Arc::downgrade(&self.state),
        }
    }

    pub fn signal_one(&self) -> bool {
        let mut inner = lock_unpoisoned(&self.state.inner);
        let Some(id) = inner.waiting.first().copied() else {
            return false;
        };
        inner.waiting.remove(0);
        inner.signaled.insert(id);
        self.state.condvar.notify_all();
        true
    }

    pub fn signal_all(&self) -> usize {
        let mut inner = lock_unpoisoned(&self.state.inner);
        let waiting = std::mem::take(&mut inner.waiting);
        let count = waiting.len();
        inner.signaled.extend(waiting);
        self.state.condvar.notify_all();
        count
    }

    pub fn waiting_count(&self) -> usize {
        lock_unpoisoned(&self.state.inner).waiting.len()
    }
}

impl WaitToken {
    pub fn is_signaled(&self) -> bool {
        let Some(state) = self.state.upgrade() else {
            return true;
        };
        lock_unpoisoned(&state.inner).signaled.contains(&self.id)
    }

    pub fn wait(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut inner = lock_unpoisoned(&state.inner);
        while !inner.signaled.contains(&self.id) {
            inner = state
                .condvar
                .wait(inner)
                .unwrap_or_else(|err| err.into_inner());
        }
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let Some(state) = self.state.upgrade() else {
            return true;
        };
        let inner = lock_unpoisoned(&state.inner);
        let (inner, _) = state
            .condvar
            .wait_timeout_while(inner, timeout, |inner| !inner.signaled.contains(&self.id))
            .unwrap_or_else(|err| err.into_inner());
        inner.signaled.contains(&self.id)
    }
}

#[derive(Clone, Default)]
pub struct OpOrder {
    state: Arc<OpOrderState>,
}

#[derive(Default)]
struct OpOrderState {
    inner: Mutex<OpOrderInner>,
    condvar: Condvar,
}

#[derive(Default)]
struct OpOrderInner {
    next_group: u64,
    active: BTreeMap<u64, usize>,
}

pub struct OpGroup {
    id: u64,
    state: Arc<OpOrderState>,
    closed: bool,
}

pub struct OpBarrier {
    waits_through: u64,
    state: Arc<OpOrderState>,
}

impl OpOrder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&self) -> OpGroup {
        let mut inner = lock_unpoisoned(&self.state.inner);
        let id = inner.next_group;
        inner.next_group += 1;
        *inner.active.entry(id).or_insert(0) += 1;
        OpGroup {
            id,
            state: Arc::clone(&self.state),
            closed: false,
        }
    }

    pub fn new_barrier(&self) -> OpBarrier {
        let inner = lock_unpoisoned(&self.state.inner);
        OpBarrier {
            waits_through: inner.next_group.saturating_sub(1),
            state: Arc::clone(&self.state),
        }
    }

    pub fn active_count(&self) -> usize {
        lock_unpoisoned(&self.state.inner).active.values().sum()
    }
}

impl OpGroup {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        let mut inner = lock_unpoisoned(&self.state.inner);
        if let Some(count) = inner.active.get_mut(&self.id) {
            *count -= 1;
            if *count == 0 {
                inner.active.remove(&self.id);
            }
        }
        self.closed = true;
        self.state.condvar.notify_all();
    }
}

impl Drop for OpGroup {
    fn drop(&mut self) {
        self.close();
    }
}

impl OpBarrier {
    pub fn is_complete(&self) -> bool {
        let inner = lock_unpoisoned(&self.state.inner);
        inner.active.keys().all(|id| *id > self.waits_through)
    }

    pub fn wait(&self) {
        let mut inner = lock_unpoisoned(&self.state.inner);
        while inner.active.keys().any(|id| *id <= self.waits_through) {
            inner = self
                .state
                .condvar
                .wait(inner)
                .unwrap_or_else(|err| err.into_inner());
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn ref_runs_cleanup_when_last_reference_is_released() {
        let released = Arc::new(AtomicUsize::new(0));
        let released_clone = Arc::clone(&released);
        let reference = Ref::with_on_release("segment".to_string(), move |value| {
            assert_eq!(value, "segment");
            released_clone.fetch_add(1, Ordering::SeqCst);
        });
        let copy = reference.clone();

        assert_eq!(reference.get(), "segment");
        assert_eq!(reference.ref_count(), 2);
        drop(reference);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        copy.release();
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shared_closeable_closes_once_across_copies() {
        let closed = Arc::new(AtomicUsize::new(0));
        let close_counter = Arc::clone(&closed);
        let closeable = SharedCloseable::with_on_close(move || {
            close_counter.fetch_add(1, Ordering::SeqCst);
        });
        let copy = closeable.shared_copy();

        assert_eq!(closeable.strong_count(), 2);
        assert!(copy.close());
        assert!(!closeable.close());
        assert!(closeable.is_closed());
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wait_queue_signals_registered_waiters_in_order() {
        let queue = WaitQueue::new();
        let first = queue.register();
        let second = queue.register();

        assert_eq!(queue.waiting_count(), 2);
        assert!(queue.signal_one());
        assert!(first.wait_timeout(Duration::from_millis(1)));
        assert!(!second.is_signaled());
        assert_eq!(queue.signal_all(), 1);
        second.wait();
        assert_eq!(queue.waiting_count(), 0);
    }

    #[test]
    fn op_order_barrier_waits_only_for_prior_groups() {
        let order = OpOrder::new();
        let mut first = order.start();
        let barrier = order.new_barrier();
        let second = order.start();

        assert!(!barrier.is_complete());
        first.close();
        barrier.wait();
        assert!(barrier.is_complete());
        assert_eq!(order.active_count(), 1);
        assert_eq!(second.id(), 1);
    }
}
