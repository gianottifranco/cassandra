// Licensed under Apache License, Version 2.0.

//! Rich partition model: decorated keys, partition updates, iterators.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.DecoratedKey`
//! - `org.apache.cassandra.db.partitions.PartitionUpdate`
//! - `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`

pub mod decorated_key;
pub mod filtered_partition;
pub mod iterators;
pub mod partition_update;

pub use decorated_key::DecoratedKey;
pub use filtered_partition::FilteredPartition;
pub use iterators::{InMemoryRowIterator, UnfilteredPartitionIterator, UnfilteredRowIterator};
pub use partition_update::PartitionUpdate;
