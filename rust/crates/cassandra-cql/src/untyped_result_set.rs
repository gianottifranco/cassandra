// Licensed under Apache License, Version 2.0.

//! UntypedResultSet for internal system queries.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.UntypedResultSet`

use std::collections::HashMap;

/// A result set where rows are maps of column name → raw bytes.
/// Used for internal system keyspace queries.
#[derive(Debug, Clone)]
pub struct UntypedResultSet {
    columns: Vec<String>,
    rows: Vec<HashMap<String, Option<Vec<u8>>>>,
}

/// A single row in an `UntypedResultSet`.
#[derive(Debug, Clone)]
pub struct UntypedRow {
    data: HashMap<String, Option<Vec<u8>>>,
}

impl UntypedResultSet {
    /// Create from column names and raw row data.
    pub fn from_columns_and_rows(
        columns: Vec<String>,
        raw_rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Self {
        let rows = raw_rows
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .zip(row)
                    .map(|(col, val)| (col.clone(), val))
                    .collect()
            })
            .collect();
        Self { columns, rows }
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = UntypedRow> + '_ {
        self.rows.iter().map(|data| UntypedRow { data: data.clone() })
    }

    /// Get a specific row by index.
    pub fn row(&self, index: usize) -> Option<UntypedRow> {
        self.rows.get(index).map(|data| UntypedRow { data: data.clone() })
    }
}

impl UntypedRow {
    /// Check if column exists and is non-null.
    pub fn has(&self, column: &str) -> bool {
        matches!(self.data.get(column), Some(Some(_)))
    }

    /// Get raw bytes for a column.
    pub fn get_blob(&self, column: &str) -> Option<&[u8]> {
        self.data.get(column)?.as_deref()
    }

    /// Get a UTF-8 string.
    pub fn get_string(&self, column: &str) -> Option<String> {
        let bytes = self.get_blob(column)?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    /// Get a 32-bit integer (big-endian).
    pub fn get_int(&self, column: &str) -> Option<i32> {
        let bytes = self.get_blob(column)?;
        if bytes.len() == 4 {
            Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        } else {
            None
        }
    }

    /// Get a 64-bit integer (big-endian).
    pub fn get_long(&self, column: &str) -> Option<i64> {
        let bytes = self.get_blob(column)?;
        if bytes.len() == 8 {
            Some(i64::from_be_bytes(bytes.try_into().ok()?))
        } else {
            None
        }
    }

    /// Get a boolean.
    pub fn get_boolean(&self, column: &str) -> Option<bool> {
        let bytes = self.get_blob(column)?;
        if bytes.len() == 1 {
            Some(bytes[0] != 0)
        } else {
            None
        }
    }

    /// Get a UUID as string.
    pub fn get_uuid(&self, column: &str) -> Option<String> {
        let bytes = self.get_blob(column)?;
        if bytes.len() == 16 {
            Some(format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            ))
        } else {
            None
        }
    }

    /// Get a timestamp as microseconds since epoch (i64 big-endian).
    pub fn get_timestamp(&self, column: &str) -> Option<i64> {
        self.get_long(column)
    }

    /// Get raw bytes for a set column (opaque — caller parses).
    pub fn get_set_bytes(&self, column: &str) -> Option<&[u8]> {
        self.get_blob(column)
    }

    /// Get raw bytes for a map column (opaque — caller parses).
    pub fn get_map_bytes(&self, column: &str) -> Option<&[u8]> {
        self.get_blob(column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_result_set() -> UntypedResultSet {
        UntypedResultSet::from_columns_and_rows(
            vec!["name".into(), "age".into(), "active".into()],
            vec![
                vec![
                    Some(b"Alice".to_vec()),
                    Some(30i32.to_be_bytes().to_vec()),
                    Some(vec![1]),
                ],
                vec![
                    Some(b"Bob".to_vec()),
                    Some(25i32.to_be_bytes().to_vec()),
                    Some(vec![0]),
                ],
            ],
        )
    }

    #[test]
    fn basic_structure() {
        let rs = sample_result_set();
        assert_eq!(rs.len(), 2);
        assert!(!rs.is_empty());
        assert_eq!(rs.columns(), &["name", "age", "active"]);
    }

    #[test]
    fn typed_accessors() {
        let rs = sample_result_set();
        let row = rs.row(0).unwrap();

        assert_eq!(row.get_string("name"), Some("Alice".to_string()));
        assert_eq!(row.get_int("age"), Some(30));
        assert_eq!(row.get_boolean("active"), Some(true));
        assert!(row.has("name"));
    }

    #[test]
    fn second_row() {
        let rs = sample_result_set();
        let row = rs.row(1).unwrap();

        assert_eq!(row.get_string("name"), Some("Bob".to_string()));
        assert_eq!(row.get_int("age"), Some(25));
        assert_eq!(row.get_boolean("active"), Some(false));
    }

    #[test]
    fn missing_column() {
        let rs = sample_result_set();
        let row = rs.row(0).unwrap();

        assert!(row.get_string("nonexistent").is_none());
        assert!(!row.has("nonexistent"));
    }

    #[test]
    fn null_value() {
        let rs = UntypedResultSet::from_columns_and_rows(
            vec!["name".into()],
            vec![vec![None]],
        );
        let row = rs.row(0).unwrap();
        assert!(row.get_string("name").is_none());
        assert!(!row.has("name"));
    }

    #[test]
    fn get_long_and_timestamp() {
        let ts: i64 = 1_700_000_000_000_000;
        let rs = UntypedResultSet::from_columns_and_rows(
            vec!["ts".into()],
            vec![vec![Some(ts.to_be_bytes().to_vec())]],
        );
        let row = rs.row(0).unwrap();
        assert_eq!(row.get_long("ts"), Some(ts));
        assert_eq!(row.get_timestamp("ts"), Some(ts));
    }

    #[test]
    fn get_uuid() {
        let uuid_bytes = vec![
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];
        let rs = UntypedResultSet::from_columns_and_rows(
            vec!["id".into()],
            vec![vec![Some(uuid_bytes)]],
        );
        let row = rs.row(0).unwrap();
        let uuid = row.get_uuid("id").unwrap();
        assert_eq!(uuid, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn iterator() {
        let rs = sample_result_set();
        let names: Vec<String> = rs.iter().filter_map(|r| r.get_string("name")).collect();
        assert_eq!(names, vec!["Alice", "Bob"]);
    }

    #[test]
    fn empty_result_set() {
        let rs = UntypedResultSet::from_columns_and_rows(vec!["x".into()], vec![]);
        assert!(rs.is_empty());
        assert_eq!(rs.len(), 0);
        assert!(rs.row(0).is_none());
    }

    #[test]
    fn get_blob_raw() {
        let rs = UntypedResultSet::from_columns_and_rows(
            vec!["data".into()],
            vec![vec![Some(vec![0xDE, 0xAD, 0xBE, 0xEF])]],
        );
        let row = rs.row(0).unwrap();
        assert_eq!(row.get_blob("data"), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }
}
