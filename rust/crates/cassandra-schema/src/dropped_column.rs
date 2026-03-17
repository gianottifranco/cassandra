// Licensed under Apache License, Version 2.0.

//! Dropped column tracking for tables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.DroppedColumn`

use crate::column::ColumnKind;
use serde::{Deserialize, Serialize};

/// Record of a column that was dropped from a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedColumn {
    /// Column name.
    pub name: String,
    /// Column type (as CQL type string).
    pub column_type: String,
    /// Timestamp when the column was dropped (microseconds since epoch).
    pub dropped_time: i64,
    /// The kind of column that was dropped.
    pub kind: ColumnKind,
}

impl DroppedColumn {
    /// Create a new dropped column record.
    pub fn new(
        name: impl Into<String>,
        column_type: impl Into<String>,
        dropped_time: i64,
        kind: ColumnKind,
    ) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
            dropped_time,
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_dropped_column() {
        let dc = DroppedColumn::new("old_col", "text", 1234567890, ColumnKind::Regular);
        assert_eq!(dc.name, "old_col");
        assert_eq!(dc.column_type, "text");
        assert_eq!(dc.dropped_time, 1234567890);
        assert_eq!(dc.kind, ColumnKind::Regular);
    }

    #[test]
    fn serde_round_trip() {
        let dc = DroppedColumn::new("x", "int", 999, ColumnKind::Static);
        let json = serde_json::to_string(&dc).unwrap();
        let deserialized: DroppedColumn = serde_json::from_str(&json).unwrap();
        assert_eq!(dc, deserialized);
    }
}
