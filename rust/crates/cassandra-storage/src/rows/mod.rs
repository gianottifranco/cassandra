// Licensed under Apache License, Version 2.0.

//! Rich row model matching Java `db.rows.*`.
//!
//! These types provide a proper Cassandra row model with liveness info,
//! complex column data, range tombstone markers, and the `Unfiltered`
//! iterator abstraction. They coexist with the simpler `memtable::partition`
//! types and provide `From`/`Into` conversions for backward compatibility.

pub mod cell;
pub mod complex_column;
pub mod deletion;
pub mod encoding_stats;
pub mod liveness;
pub mod unfiltered;

pub use cell::{CellData, CellPath};
pub use complex_column::ComplexColumnData;
pub use deletion::MutableDeletionInfo;
pub use encoding_stats::EncodingStats;
pub use liveness::LivenessInfo;
pub use unfiltered::{ColumnData, RangeTombstoneMarker, RowData, Unfiltered};
