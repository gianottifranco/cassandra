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

//! # cassandra-storage
//!
//! Storage engine: CommitLog, MemTable, SSTable, compaction, and the
//! unified StorageEngine that ties them together.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db` (ColumnFamilyStore, Keyspace, Mutation)
//! - `org.apache.cassandra.io` (SSTable readers/writers)
//! - `org.apache.cassandra.db.commitlog` (CommitLog)
//! - `org.apache.cassandra.db.compaction` (CompactionManager, strategies)
//!
//! ## Module Summary
//!
//! | Module       | Status     | Description                                      |
//! |-------------|------------|--------------------------------------------------|
//! | commitlog   | Functional | Segmented WAL with CRC32C, rotation, replay      |
//! | memtable    | Functional | Skiplist-based with manager and backpressure      |
//! | sstable     | Functional | Big-format compatible writer/reader + bloom       |
//! | compaction  | Functional | STCS strategy, merge, tombstone GC                |
//! | engine      | Functional | Unified write/read/flush/compact/snapshot/replay  |

pub mod commitlog;
pub mod memtable;
pub mod sstable;
pub mod compaction;
pub mod engine;
