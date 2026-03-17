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

//! Outbound message queue with expiration support.
//!
//! Uses a dual-queue pattern: external crossbeam MPSC for producers,
//! internal VecDeque for drain/prune. Messages are stamped with an
//! expiration time and automatically pruned.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.OutboundMessageQueue`

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crossbeam::channel::{self, Receiver, Sender, TrySendError};

use crate::frame::Message;

/// Default channel capacity for the external queue.
const DEFAULT_CAPACITY: usize = 4096;

/// A queued message with expiration metadata.
#[derive(Debug)]
pub struct QueuedMessage {
    pub message: Message,
    pub expires_at: Instant,
    pub enqueued_at: Instant,
}

/// Queue metrics.
#[derive(Debug, Default)]
pub struct QueueMetrics {
    pub enqueued: AtomicU64,
    pub expired: AtomicU64,
    pub sent: AtomicU64,
    pub dropped: AtomicU64,
}

impl QueueMetrics {
    pub fn snapshot(&self) -> QueueMetricsSnapshot {
        QueueMetricsSnapshot {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            sent: self.sent.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of queue metrics.
#[derive(Debug, Clone)]
pub struct QueueMetricsSnapshot {
    pub enqueued: u64,
    pub expired: u64,
    pub sent: u64,
    pub dropped: u64,
}

/// Producer handle for enqueueing messages.
#[derive(Debug, Clone)]
pub struct QueueProducer {
    sender: Sender<QueuedMessage>,
}

impl QueueProducer {
    /// Enqueue a message with the given expiration time.
    ///
    /// Returns `false` if the queue is full (message dropped).
    pub fn enqueue(&self, message: Message, expires_at: Instant) -> bool {
        let queued = QueuedMessage {
            message,
            expires_at,
            enqueued_at: Instant::now(),
        };
        match self.sender.try_send(queued) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

/// Outbound message queue with expiration.
///
/// The consumer side: drains from the crossbeam channel into an internal
/// VecDeque, then serves messages via `poll_next()`, skipping expired ones.
pub struct OutboundMessageQueue {
    receiver: Receiver<QueuedMessage>,
    internal: VecDeque<QueuedMessage>,
    pub metrics: QueueMetrics,
}

impl OutboundMessageQueue {
    /// Create a new queue, returning (queue, producer).
    pub fn new() -> (Self, QueueProducer) {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create with a specific capacity.
    pub fn with_capacity(capacity: usize) -> (Self, QueueProducer) {
        let (sender, receiver) = channel::bounded(capacity);
        let queue = Self {
            receiver,
            internal: VecDeque::with_capacity(256),
            metrics: QueueMetrics::default(),
        };
        let producer = QueueProducer { sender };
        (queue, producer)
    }

    /// Drain all available messages from the external channel into the
    /// internal queue.
    pub fn drain_to_internal(&mut self) {
        while let Ok(msg) = self.receiver.try_recv() {
            self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
            self.internal.push_back(msg);
        }
    }

    /// Poll the next non-expired message.
    ///
    /// Drains external queue first, then returns the first non-expired
    /// message from the internal queue. Expired messages are counted
    /// and discarded.
    pub fn poll_next(&mut self) -> Option<QueuedMessage> {
        self.drain_to_internal();

        let now = Instant::now();
        while let Some(msg) = self.internal.pop_front() {
            if msg.expires_at > now {
                self.metrics.sent.fetch_add(1, Ordering::Relaxed);
                return Some(msg);
            }
            self.metrics.expired.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Remove all expired messages from the internal queue.
    ///
    /// Returns the number of messages pruned.
    pub fn prune_expired(&mut self) -> usize {
        self.drain_to_internal();

        let now = Instant::now();
        let before = self.internal.len();
        self.internal.retain(|msg| msg.expires_at > now);
        let pruned = before - self.internal.len();
        self.metrics
            .expired
            .fetch_add(pruned as u64, Ordering::Relaxed);
        pruned
    }

    /// Number of messages currently in the internal queue.
    pub fn internal_len(&self) -> usize {
        self.internal.len()
    }

    /// Whether the queue (both external and internal) appears empty.
    pub fn is_empty(&self) -> bool {
        self.internal.is_empty() && self.receiver.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verb::Verb;
    use std::time::Duration;

    fn make_msg(verb: Verb, id: u64) -> Message {
        Message::request(verb, id, Vec::new())
    }

    #[test]
    fn enqueue_and_poll() {
        let (mut queue, producer) = OutboundMessageQueue::new();
        let expires = Instant::now() + Duration::from_secs(10);

        assert!(producer.enqueue(make_msg(Verb::Mutation, 1), expires));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 2), expires));

        let msg1 = queue.poll_next().unwrap();
        assert_eq!(msg1.message.header.message_id, 1);

        let msg2 = queue.poll_next().unwrap();
        assert_eq!(msg2.message.header.message_id, 2);

        assert!(queue.poll_next().is_none());
    }

    #[test]
    fn expired_messages_skipped() {
        let (mut queue, producer) = OutboundMessageQueue::new();

        // Already expired
        let expired = Instant::now() - Duration::from_millis(1);
        let fresh = Instant::now() + Duration::from_secs(10);

        assert!(producer.enqueue(make_msg(Verb::Mutation, 1), expired));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 2), fresh));

        let msg = queue.poll_next().unwrap();
        assert_eq!(msg.message.header.message_id, 2);

        let snap = queue.metrics.snapshot();
        assert_eq!(snap.expired, 1);
    }

    #[test]
    fn prune_removes_expired() {
        let (mut queue, producer) = OutboundMessageQueue::new();

        let expired = Instant::now() - Duration::from_millis(1);
        let fresh = Instant::now() + Duration::from_secs(10);

        assert!(producer.enqueue(make_msg(Verb::Mutation, 1), expired));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 2), expired));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 3), fresh));

        let pruned = queue.prune_expired();
        assert_eq!(pruned, 2);
        assert_eq!(queue.internal_len(), 1);
    }

    #[test]
    fn queue_full_returns_false() {
        let (mut _queue, producer) = OutboundMessageQueue::with_capacity(2);
        let expires = Instant::now() + Duration::from_secs(10);

        assert!(producer.enqueue(make_msg(Verb::Ping, 1), expires));
        assert!(producer.enqueue(make_msg(Verb::Ping, 2), expires));
        assert!(!producer.enqueue(make_msg(Verb::Ping, 3), expires));
    }

    #[test]
    fn metrics_tracking() {
        let (mut queue, producer) = OutboundMessageQueue::new();
        let fresh = Instant::now() + Duration::from_secs(10);
        let expired = Instant::now() - Duration::from_millis(1);

        assert!(producer.enqueue(make_msg(Verb::Mutation, 1), fresh));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 2), expired));
        assert!(producer.enqueue(make_msg(Verb::Mutation, 3), fresh));

        let _ = queue.poll_next(); // msg 1 (sent)
        let _ = queue.poll_next(); // msg 2 expired, msg 3 (sent)

        let snap = queue.metrics.snapshot();
        assert_eq!(snap.enqueued, 3);
        assert_eq!(snap.sent, 2);
        assert_eq!(snap.expired, 1);
    }

    #[test]
    fn is_empty_checks_both_queues() {
        let (mut queue, producer) = OutboundMessageQueue::new();
        assert!(queue.is_empty());

        let expires = Instant::now() + Duration::from_secs(10);
        assert!(producer.enqueue(make_msg(Verb::Ping, 1), expires));
        assert!(!queue.is_empty());

        queue.drain_to_internal();
        assert!(!queue.is_empty());

        let _ = queue.poll_next();
        assert!(queue.is_empty());
    }
}
