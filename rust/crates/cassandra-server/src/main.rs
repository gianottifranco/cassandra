// Licensed under Apache License, Version 2.0.

//! Cassandra Rust Server – single-node binary.
//!
//! ## Startup sequence
//!
//! 1. Initialize tracing
//! 2. Create data directories
//! 3. Open storage engine
//! 4. Create schema catalog
//! 5. Replay commit log (crash recovery)
//! 6. Log ready status

mod executor;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use cassandra_schema::SchemaCatalog;
use cassandra_storage::commitlog::CommitLogConfig;
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use crate::executor::QueryExecutor;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Tracing
    tracing_subscriber::fmt::init();

    info!("cassandra-server v{VERSION} starting");

    // 2. Data directories
    let data_dir = PathBuf::from("data");
    let commitlog_dir = data_dir.join("commitlog");

    // 3. Open storage engine
    let engine_config = EngineConfig {
        data_dir: data_dir.clone(),
        commitlog_config: CommitLogConfig {
            max_segment_size: 32 * 1024 * 1024, // 32 MiB
            directory: commitlog_dir,
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 128 * 1024 * 1024, // 128 MiB
        gc_grace_seconds: 864_000,                     // 10 days
    };

    let engine = Arc::new(
        StorageEngine::open(engine_config)
            .expect("Failed to open storage engine"),
    );

    // 4. Schema catalog
    let catalog = Arc::new(RwLock::new(SchemaCatalog::new()));

    // 5. Commit log replay
    let replayed = engine.replay_commitlog().expect("Commit log replay failed");
    info!(replayed, "Commit log replay complete");

    // 6. Build executor
    let _executor = QueryExecutor::new(Arc::clone(&engine), Arc::clone(&catalog));

    info!("cassandra-server v{VERSION} ready");

    // TODO: Native protocol listener on port 9042
    // For now, the server starts and immediately exits.
    // The next step is wiring the native protocol codec to the executor.

    // Keep alive for development (ctrl-c to exit)
    info!("Server initialized. Native protocol listener not yet wired.");
    info!("Use `cargo test --workspace` to verify all components.");

    Ok(())
}
