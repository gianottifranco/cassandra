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

//! # Secondary Index Lifecycle Manager
//!
//! Per-table lifecycle management for secondary indexes: add, remove, rebuild, initialize.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.SecondaryIndexManager`

use std::sync::Arc;

use crate::index::legacy::LegacyIndex;
use crate::index::sai::SaiIndex;
use crate::index::{
    IndexDefinition, IndexEntry, IndexError, IndexManager, IndexStatus, IndexType, SecondaryIndex,
};
use crate::memtable::partition::PartitionData;

/// Events in the lifecycle of a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLifecycleEvent {
    Created,
    BuildStarted,
    BuildCompleted,
    BuildFailed,
    Queryable,
    Dropped,
    Invalidated,
}

impl IndexLifecycleEvent {
    /// Map a lifecycle event to the resulting index status.
    pub fn resulting_status(&self) -> Option<IndexStatus> {
        match self {
            Self::BuildStarted | Self::Created => Some(IndexStatus::Building),
            Self::BuildCompleted => Some(IndexStatus::Built),
            Self::Queryable => Some(IndexStatus::QueryReady),
            Self::Invalidated => Some(IndexStatus::Building),
            Self::Dropped | Self::BuildFailed => None,
        }
    }
}

/// Per-table secondary index lifecycle manager.
///
/// Wraps an `IndexManager` and provides lifecycle operations:
/// add, remove, rebuild, and bulk initialization.
#[derive(Debug)]
pub struct SecondaryIndexManager {
    manager: Arc<IndexManager>,
}

impl SecondaryIndexManager {
    pub fn new(manager: Arc<IndexManager>) -> Self {
        Self { manager }
    }

    /// Access the underlying `IndexManager`.
    pub fn manager(&self) -> &Arc<IndexManager> {
        &self.manager
    }

    /// Add a new index: create the implementation, register it, and mark as Building.
    pub fn add_index(&self, def: IndexDefinition) -> Result<(), IndexError> {
        if self.manager.has_index(&def.name) {
            return Err(IndexError::General(format!(
                "Index '{}' already exists",
                def.name
            )));
        }

        let name = def.name.clone();
        let index = Self::create_index(def)?;

        self.manager.set_status(&name, IndexStatus::Building);
        self.manager.register(index);
        // register sets QueryReady, override to Building
        self.manager.set_status(&name, IndexStatus::Building);

        Ok(())
    }

    /// Remove an index: unregister and clean up.
    pub fn remove_index(&self, name: &str) -> Result<(), IndexError> {
        if !self.manager.unregister(name) {
            return Err(IndexError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Rebuild an index from the given partition data.
    pub fn rebuild_index(
        &self,
        name: &str,
        partitions: &[(Vec<u8>, PartitionData)],
    ) -> Result<(), IndexError> {
        let def = self
            .manager
            .get_definition(name)
            .ok_or_else(|| IndexError::NotFound(name.to_string()))?;

        self.manager.set_status(name, IndexStatus::Building);

        for (pk, pd) in partitions {
            for (ck, row) in &pd.rows {
                if row.is_tombstone {
                    continue;
                }
                for cell in &row.cells {
                    if cell.is_tombstone || cell.value.is_none() {
                        continue;
                    }
                    if cell.column == def.column {
                        let entry = IndexEntry {
                            term: cell.value.as_ref().unwrap().clone(),
                            partition_key: pk.clone(),
                            clustering_key: ck.clone(),
                        };
                        self.manager
                            .on_write(
                                &entry.partition_key,
                                &entry.clustering_key,
                                &def.column,
                                &entry.term,
                            )
                            .map_err(|e| IndexError::BuildFailed(e.to_string()))?;
                    }
                }
            }
        }

        self.manager.set_status(name, IndexStatus::QueryReady);
        Ok(())
    }

    /// Bulk-initialize multiple indexes from partition data.
    pub fn initialize(
        &self,
        indexes: &[IndexDefinition],
        partitions: &[(Vec<u8>, PartitionData)],
    ) -> Result<(), IndexError> {
        for def in indexes {
            self.add_index(def.clone())?;
        }
        for def in indexes {
            self.rebuild_index(&def.name, partitions)?;
        }
        Ok(())
    }

    /// Factory method: create the appropriate index implementation based on type.
    fn create_index(def: IndexDefinition) -> Result<Box<dyn SecondaryIndex>, IndexError> {
        match def.index_type {
            IndexType::Legacy => Ok(Box::new(LegacyIndex::new(def))),
            IndexType::Sai => Ok(Box::new(SaiIndex::new(def))),
            IndexType::Sasi => {
                #[cfg(feature = "sasi")]
                {
                    Ok(Box::new(crate::index::sasi::SasiIndex::new(def)))
                }
                #[cfg(not(feature = "sasi"))]
                {
                    Err(IndexError::General(
                        "SASI index not enabled in build".into(),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_def(name: &str, column: &str, index_type: IndexType) -> IndexDefinition {
        IndexDefinition {
            name: name.into(),
            keyspace: "ks".into(),
            table: "tbl".into(),
            column: column.into(),
            index_type,
            options: HashMap::new(),
        }
    }

    #[test]
    fn lifecycle_event_status_mapping() {
        assert_eq!(
            IndexLifecycleEvent::BuildStarted.resulting_status(),
            Some(IndexStatus::Building)
        );
        assert_eq!(
            IndexLifecycleEvent::Queryable.resulting_status(),
            Some(IndexStatus::QueryReady)
        );
        assert_eq!(IndexLifecycleEvent::Dropped.resulting_status(), None);
    }

    #[test]
    fn add_and_remove_legacy_index() {
        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr.clone());

        let def = make_def("idx1", "col1", IndexType::Legacy);
        sim.add_index(def).unwrap();
        assert!(mgr.has_index("idx1"));
        assert_eq!(mgr.get_status("idx1"), Some(IndexStatus::Building));

        sim.remove_index("idx1").unwrap();
        assert!(!mgr.has_index("idx1"));
    }

    #[test]
    fn add_duplicate_index_fails() {
        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr);

        let def = make_def("idx1", "col1", IndexType::Legacy);
        sim.add_index(def.clone()).unwrap();
        assert!(sim.add_index(def).is_err());
    }

    #[test]
    fn remove_nonexistent_fails() {
        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr);
        assert!(sim.remove_index("nonexistent").is_err());
    }

    #[test]
    fn rebuild_index_from_data() {
        use crate::memtable::partition::{Cell, Row};

        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr.clone());

        let def = make_def("idx1", "col1", IndexType::Legacy);
        sim.add_index(def).unwrap();

        let mut pd1 = PartitionData::new();
        pd1.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "col1".into(),
                value: Some(b"hello".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        let mut pd2 = PartitionData::new();
        pd2.apply_row(Row {
            clustering_key: b"ck2".to_vec(),
            cells: vec![Cell {
                column: "col1".into(),
                value: Some(b"world".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let partitions = vec![(b"pk1".to_vec(), pd1), (b"pk2".to_vec(), pd2)];
        sim.rebuild_index("idx1", &partitions).unwrap();

        assert_eq!(mgr.get_status("idx1"), Some(IndexStatus::QueryReady));
        let results = mgr.search("idx1", b"hello").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");
    }

    #[test]
    fn initialize_bulk() {
        use crate::memtable::partition::{Cell, Row};

        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr.clone());

        let defs = vec![
            make_def("idx1", "col1", IndexType::Legacy),
            make_def("idx2", "col2", IndexType::Legacy),
        ];

        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![
                Cell {
                    column: "col1".into(),
                    value: Some(b"v1".to_vec()),
                    timestamp: 1,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                },
                Cell {
                    column: "col2".into(),
                    value: Some(b"v2".to_vec()),
                    timestamp: 1,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                },
            ],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let partitions = vec![(b"pk1".to_vec(), pd)];
        sim.initialize(&defs, &partitions).unwrap();

        assert_eq!(mgr.count(), 2);
        assert_eq!(mgr.get_status("idx1"), Some(IndexStatus::QueryReady));
        assert_eq!(mgr.get_status("idx2"), Some(IndexStatus::QueryReady));

        let r1 = mgr.search("idx1", b"v1").unwrap();
        assert_eq!(r1.len(), 1);
        let r2 = mgr.search("idx2", b"v2").unwrap();
        assert_eq!(r2.len(), 1);
    }

    #[test]
    fn add_sai_index() {
        let mgr = Arc::new(IndexManager::new());
        let sim = SecondaryIndexManager::new(mgr.clone());

        let def = make_def("sai_idx", "col1", IndexType::Sai);
        sim.add_index(def).unwrap();
        assert!(mgr.has_index("sai_idx"));
    }
}
