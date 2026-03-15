# ADR-005: Runtime and Concurrency Model

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

The Java Cassandra server uses a custom thread-pool architecture:
- **NATIVE-TRANSPORT** threads for CQL connections (Netty-based).
- **READ/WRITE/COUNTER-MUTATION** thread pools for request processing.
- **COMPACTION** threads.
- **GOSSIP** thread.
- **INTERNAL** thread pool for miscellaneous work.
- **MEMTABLE-FLUSH** threads.
- Extensive use of `ListenableFuture` and callbacks.

The Rust rewrite must achieve at least equivalent throughput and latency
while leveraging Rust's async and concurrency primitives.

## Decision

### Async Runtime: Tokio

We use **Tokio** as the primary async runtime:
- Proven, actively maintained, excellent ecosystem.
- Multi-threaded work-stealing scheduler.
- Native epoll/kqueue/io_uring (with feature flag) support.
- Built-in timers, channels, synchronization primitives.

### Runtime Architecture

```
┌──────────────────────────────────────────────────┐
│                  Main Runtime                     │
│  (Tokio multi-threaded, cores = num_cpus * 0.75) │
│                                                   │
│  ┌─────────────┐ ┌────────────┐ ┌──────────────┐│
│  │ CQL Server  │ │ Gossip Loop│ │ Admin Server ││
│  │ (TCP accept)│ │            │ │ (HTTP/gRPC)  ││
│  └─────────────┘ └────────────┘ └──────────────┘│
└──────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│              Blocking Runtime                     │
│  (Tokio blocking pool or dedicated threads)       │
│                                                   │
│  ┌─────────────┐ ┌────────────┐ ┌──────────────┐│
│  │ Disk I/O    │ │ Compaction │ │ SSTable      ││
│  │ (flush,read)│ │ Workers    │ │ Streaming    ││
│  └─────────────┘ └────────────┘ └──────────────┘│
└──────────────────────────────────────────────────┘
```

### Threading Model

| Component          | Model                  | Rationale                      |
|--------------------|------------------------|--------------------------------|
| CQL protocol       | async (Tokio tasks)    | I/O-bound, many connections    |
| Gossip             | async (Tokio task)     | Timer-driven, lightweight      |
| Read path          | async + spawn_blocking | May touch disk                 |
| Write path         | async → channel → flush thread | Pipeline stages        |
| MemTable writes    | lock-free (crossbeam)  | Hot path, contention-sensitive |
| CommitLog          | dedicated thread + channel | Sequential append          |
| Compaction         | dedicated thread pool  | CPU + I/O bound, throttled     |
| Repair             | async tasks            | Coordination-heavy             |
| Streaming          | async (Tokio streams)  | Network I/O bound              |

### Key Design Choices

1. **No `async_trait` in hot path** – use manual `impl Future` or
   `Pin<Box<dyn Future>>` only at trait boundaries where unavoidable.
   Prefer concrete types and generics.

2. **Cancellation safety** – all async operations must be cancellation-safe.
   Use `tokio::select!` judiciously. Document cancellation behavior.

3. **Backpressure** – use bounded channels (`tokio::sync::mpsc`) between
   pipeline stages. Reject requests at the protocol layer when overloaded.

4. **No global mutable state** – pass configuration and shared state via
   `Arc<AppState>` from main. No `lazy_static!` for mutable data.

5. **Graceful shutdown** – `CancellationToken` propagated from main to all
   subsystems. Ordered shutdown: stop accepting → drain requests → flush
   memtables → close commitlog → exit.

### Dependencies

| Crate         | Purpose                    | Version Policy    |
|---------------|----------------------------|-------------------|
| `tokio`       | Async runtime              | Latest stable 1.x |
| `crossbeam`   | Lock-free data structures  | Latest stable     |
| `parking_lot` | Faster mutexes/rwlocks     | Latest stable     |
| `tracing`     | Structured logging + spans | Latest stable 0.1 |

## Consequences

- Tokio is a hard dependency; switching runtimes later would be expensive.
- The blocking pool size must be tunable (matching Java's thread pool config).
- Need to benchmark Tokio's work-stealing vs. thread-per-core (may revisit
  in performance phase).
