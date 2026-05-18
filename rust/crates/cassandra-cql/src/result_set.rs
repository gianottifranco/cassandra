// Licensed under Apache License, Version 2.0.

//! ResultSet with metadata, paging support, and protocol conversion.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.ResultSet`
//! - `org.apache.cassandra.cql3.ResultSet.ResultMetadata`

use cassandra_native_protocol::message::{
    ColumnSpec, PreparedResult, RowsMetadata, RowsResult, rows_flags,
};
use md5::{Digest, Md5};

/// Metadata describing the columns of a result set.
#[derive(Debug, Clone)]
pub struct ResultMetadata {
    pub column_specs: Vec<ColumnSpec>,
    pub paging_state: Option<Vec<u8>>,
    pub has_more_pages: bool,
    pub metadata_changed: bool,
}

impl ResultMetadata {
    pub fn new(column_specs: Vec<ColumnSpec>) -> Self {
        Self {
            column_specs,
            paging_state: None,
            has_more_pages: false,
            metadata_changed: false,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Compute MD5 of column metadata for METADATA_CHANGED detection.
    pub fn compute_metadata_id(&self) -> [u8; 16] {
        let mut hasher = Md5::new();
        for spec in &self.column_specs {
            if let Some(ks) = &spec.ksname {
                hasher.update(ks.as_bytes());
            }
            if let Some(tbl) = &spec.tablename {
                hasher.update(tbl.as_bytes());
            }
            hasher.update(spec.name.as_bytes());
            hasher.update(spec.col_type.id().to_be_bytes());
        }
        let result = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&result);
        id
    }

    /// Build protocol flags from this metadata's state.
    fn build_flags(&self, global_table_spec: bool) -> i32 {
        let mut flags = 0i32;
        if global_table_spec {
            flags |= rows_flags::GLOBAL_TABLES_SPEC;
        }
        if self.has_more_pages {
            flags |= rows_flags::HAS_MORE_PAGES;
        }
        if self.metadata_changed {
            flags |= rows_flags::METADATA_CHANGED;
        }
        flags
    }
}

/// Metadata for prepared statement bind variables.
#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub column_specs: Vec<ColumnSpec>,
    pub partition_key_bind_indexes: Vec<u16>,
}

impl PreparedMetadata {
    pub fn new(column_specs: Vec<ColumnSpec>, partition_key_bind_indexes: Vec<u16>) -> Self {
        Self {
            column_specs,
            partition_key_bind_indexes,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

/// A result set with metadata, rows, and optional diagnostic info.
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub metadata: ResultMetadata,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub warnings: Vec<String>,
    pub tracing_id: Option<uuid::Uuid>,
}

impl ResultSet {
    pub fn new(metadata: ResultMetadata, rows: Vec<Vec<Option<Vec<u8>>>>) -> Self {
        Self {
            metadata,
            rows,
            warnings: Vec::new(),
            tracing_id: None,
        }
    }

    pub fn empty() -> Self {
        Self::new(ResultMetadata::empty(), Vec::new())
    }

    /// Number of rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Determine if all columns share the same keyspace and table.
fn detect_global_table_spec(specs: &[ColumnSpec]) -> Option<(String, String)> {
    if specs.is_empty() {
        return None;
    }
    let first_ks = specs[0].ksname.as_deref()?;
    let first_tbl = specs[0].tablename.as_deref()?;
    for spec in &specs[1..] {
        if spec.ksname.as_deref() != Some(first_ks) || spec.tablename.as_deref() != Some(first_tbl)
        {
            return None;
        }
    }
    Some((first_ks.to_string(), first_tbl.to_string()))
}

/// Convert a `ResultSet` into protocol `RowsResult`.
impl From<ResultSet> for RowsResult {
    fn from(rs: ResultSet) -> Self {
        let global_spec = detect_global_table_spec(&rs.metadata.column_specs);
        let flags = rs.metadata.build_flags(global_spec.is_some());
        let columns_count = rs.metadata.column_specs.len() as i32;
        let rows_count = rs.rows.len() as i32;
        let new_metadata_id = if rs.metadata.metadata_changed {
            Some(rs.metadata.compute_metadata_id().to_vec())
        } else {
            None
        };

        let metadata = RowsMetadata {
            flags,
            columns_count,
            paging_state: rs.metadata.paging_state,
            new_metadata_id,
            global_table_spec: global_spec,
            col_specs: rs.metadata.column_specs,
        };

        RowsResult {
            metadata,
            rows_count,
            rows: rs.rows,
        }
    }
}

/// Convert a `ResultSet` into a protocol `PreparedResult` with given ID and bind metadata.
pub fn to_prepared_result(
    id: Vec<u8>,
    bind_metadata: &PreparedMetadata,
    result_metadata: &ResultMetadata,
) -> PreparedResult {
    let result_metadata_id = Some(result_metadata.compute_metadata_id().to_vec());

    let bind_meta = RowsMetadata {
        flags: 0,
        columns_count: bind_metadata.column_specs.len() as i32,
        paging_state: None,
        new_metadata_id: None,
        global_table_spec: None,
        col_specs: bind_metadata.column_specs.clone(),
    };

    let result_global = detect_global_table_spec(&result_metadata.column_specs);
    let result_meta = RowsMetadata {
        flags: if result_global.is_some() {
            rows_flags::GLOBAL_TABLES_SPEC
        } else {
            0
        },
        columns_count: result_metadata.column_specs.len() as i32,
        paging_state: None,
        new_metadata_id: None,
        global_table_spec: result_global,
        col_specs: result_metadata.column_specs.clone(),
    };

    PreparedResult {
        id,
        result_metadata_id,
        bind_metadata: bind_meta,
        result_metadata: result_meta,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_native_protocol::message::ColumnType;

    fn make_spec(ks: &str, table: &str, name: &str, col_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            ksname: Some(ks.to_string()),
            tablename: Some(table.to_string()),
            name: name.to_string(),
            col_type,
        }
    }

    #[test]
    fn result_metadata_compute_id_deterministic() {
        let meta = ResultMetadata::new(vec![
            make_spec("ks", "t", "id", ColumnType::Uuid),
            make_spec("ks", "t", "name", ColumnType::Varchar),
        ]);
        let id1 = meta.compute_metadata_id();
        let id2 = meta.compute_metadata_id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn result_metadata_different_columns_different_id() {
        let meta1 = ResultMetadata::new(vec![make_spec("ks", "t", "id", ColumnType::Uuid)]);
        let meta2 = ResultMetadata::new(vec![make_spec("ks", "t", "name", ColumnType::Varchar)]);
        assert_ne!(meta1.compute_metadata_id(), meta2.compute_metadata_id());
    }

    #[test]
    fn result_set_empty() {
        let rs = ResultSet::empty();
        assert!(rs.is_empty());
        assert_eq!(rs.row_count(), 0);
        assert!(rs.warnings.is_empty());
        assert!(rs.tracing_id.is_none());
    }

    #[test]
    fn result_set_to_rows_result_global_spec() {
        let specs = vec![
            make_spec("ks", "users", "id", ColumnType::Uuid),
            make_spec("ks", "users", "name", ColumnType::Varchar),
        ];
        let rows = vec![vec![Some(vec![0u8; 16]), Some(b"Alice".to_vec())]];
        let rs = ResultSet::new(ResultMetadata::new(specs), rows);

        let rr: RowsResult = rs.into();
        assert_eq!(rr.rows_count, 1);
        assert_eq!(rr.metadata.columns_count, 2);
        assert!(rr.metadata.flags & rows_flags::GLOBAL_TABLES_SPEC != 0);
        assert_eq!(
            rr.metadata.global_table_spec,
            Some(("ks".to_string(), "users".to_string()))
        );
    }

    #[test]
    fn result_set_to_rows_result_no_global_spec() {
        let specs = vec![
            make_spec("ks1", "t1", "id", ColumnType::Int),
            make_spec("ks2", "t2", "name", ColumnType::Varchar),
        ];
        let rs = ResultSet::new(ResultMetadata::new(specs), Vec::new());
        let rr: RowsResult = rs.into();
        assert!(rr.metadata.flags & rows_flags::GLOBAL_TABLES_SPEC == 0);
        assert!(rr.metadata.global_table_spec.is_none());
    }

    #[test]
    fn result_set_has_more_pages_flag() {
        let mut meta = ResultMetadata::new(vec![make_spec("ks", "t", "id", ColumnType::Int)]);
        meta.has_more_pages = true;
        meta.paging_state = Some(vec![1, 2, 3]);
        let rs = ResultSet::new(meta, Vec::new());
        let rr: RowsResult = rs.into();
        assert!(rr.metadata.flags & rows_flags::HAS_MORE_PAGES != 0);
        assert_eq!(rr.metadata.paging_state, Some(vec![1, 2, 3]));
    }

    #[test]
    fn to_prepared_result_basic() {
        let bind =
            PreparedMetadata::new(vec![make_spec("ks", "t", "id", ColumnType::Uuid)], vec![0]);
        let result = ResultMetadata::new(vec![
            make_spec("ks", "t", "id", ColumnType::Uuid),
            make_spec("ks", "t", "name", ColumnType::Varchar),
        ]);

        let pr = to_prepared_result(vec![0u8; 16], &bind, &result);
        assert_eq!(pr.id.len(), 16);
        assert!(pr.result_metadata_id.is_some());
        assert_eq!(pr.bind_metadata.columns_count, 1);
        assert_eq!(pr.result_metadata.columns_count, 2);
    }

    #[test]
    fn detect_global_spec_empty() {
        assert!(detect_global_table_spec(&[]).is_none());
    }
}
