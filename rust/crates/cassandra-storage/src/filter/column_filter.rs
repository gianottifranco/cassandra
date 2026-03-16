// Licensed under Apache License, Version 2.0.

//! Rich ColumnFilter with fetched/queried column distinction.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.filter.ColumnFilter`

use std::collections::BTreeSet;

/// Rich column filter distinguishing fetched vs queried columns.
///
/// - **Fetched columns**: all columns the storage engine must read from disk/memtable
/// - **Queried columns**: the subset the client actually asked for (for SELECT)
///
/// Fetched >= Queried because some fetched columns are needed for filtering
/// but not returned to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnFilter {
    /// Fetch and query all columns.
    AllColumns,
    /// Fetch all regular columns but only query specific static columns.
    AllRegularsAndQueriedStatics(BTreeSet<String>),
    /// Only fetch/query static columns.
    OnlyStatic(BTreeSet<String>),
    /// Only fetch/query specific columns.
    OnlyQueried {
        fetched: BTreeSet<String>,
        queried: BTreeSet<String>,
    },
}

impl ColumnFilter {
    /// Returns `true` if the given column should be fetched from storage.
    pub fn fetches(&self, column: &str) -> bool {
        match self {
            Self::AllColumns => true,
            Self::AllRegularsAndQueriedStatics(_) => {
                // All regulars are fetched, plus the listed statics.
                // Since we don't know column kind here, return true for all.
                // The caller should check kind separately for static-only filtering.
                true
            }
            Self::OnlyStatic(statics) => statics.contains(column),
            Self::OnlyQueried { fetched, .. } => fetched.contains(column),
        }
    }

    /// Returns `true` if the given column is queried (returned to client).
    pub fn queries(&self, column: &str) -> bool {
        match self {
            Self::AllColumns => true,
            Self::AllRegularsAndQueriedStatics(statics) => statics.contains(column),
            Self::OnlyStatic(statics) => statics.contains(column),
            Self::OnlyQueried { queried, .. } => queried.contains(column),
        }
    }

    /// Returns `true` if all columns are fetched.
    pub fn fetches_all_columns(&self) -> bool {
        matches!(self, Self::AllColumns | Self::AllRegularsAndQueriedStatics(_))
    }
}

/// Builder for constructing `ColumnFilter` incrementally.
pub struct ColumnFilterBuilder {
    fetched: BTreeSet<String>,
    queried: BTreeSet<String>,
    fetch_all: bool,
}

impl ColumnFilterBuilder {
    pub fn new() -> Self {
        Self {
            fetched: BTreeSet::new(),
            queried: BTreeSet::new(),
            fetch_all: false,
        }
    }

    /// Mark all columns as fetched.
    pub fn fetch_all(mut self) -> Self {
        self.fetch_all = true;
        self
    }

    /// Add a column to both fetched and queried sets.
    pub fn add_column(mut self, column: impl Into<String>) -> Self {
        let col = column.into();
        self.fetched.insert(col.clone());
        self.queried.insert(col);
        self
    }

    /// Add a column to fetched only (for filtering, not returned to client).
    pub fn add_fetched(mut self, column: impl Into<String>) -> Self {
        self.fetched.insert(column.into());
        self
    }

    pub fn build(self) -> ColumnFilter {
        if self.fetch_all {
            if self.queried.is_empty() {
                ColumnFilter::AllColumns
            } else {
                ColumnFilter::AllRegularsAndQueriedStatics(self.queried)
            }
        } else if self.fetched.is_empty() && self.queried.is_empty() {
            ColumnFilter::AllColumns
        } else {
            ColumnFilter::OnlyQueried {
                fetched: self.fetched,
                queried: self.queried,
            }
        }
    }
}

impl Default for ColumnFilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_columns() {
        let f = ColumnFilter::AllColumns;
        assert!(f.fetches("any"));
        assert!(f.queries("any"));
        assert!(f.fetches_all_columns());
    }

    #[test]
    fn only_queried() {
        let f = ColumnFilterBuilder::new()
            .add_column("name")
            .add_column("age")
            .add_fetched("score") // fetched for filtering but not queried
            .build();

        if let ColumnFilter::OnlyQueried { fetched, queried } = &f {
            assert!(fetched.contains("name"));
            assert!(fetched.contains("score"));
            assert!(queried.contains("name"));
            assert!(!queried.contains("score"));
        } else {
            panic!("expected OnlyQueried");
        }

        assert!(f.fetches("name"));
        assert!(f.fetches("score"));
        assert!(!f.fetches("other"));
        assert!(f.queries("name"));
        assert!(!f.queries("score"));
    }

    #[test]
    fn builder_fetch_all() {
        let f = ColumnFilterBuilder::new().fetch_all().build();
        assert!(matches!(f, ColumnFilter::AllColumns));
    }

    #[test]
    fn builder_fetch_all_with_queried_statics() {
        let f = ColumnFilterBuilder::new()
            .fetch_all()
            .add_column("static_col")
            .build();
        assert!(matches!(f, ColumnFilter::AllRegularsAndQueriedStatics(_)));
    }

    #[test]
    fn only_static() {
        let mut statics = BTreeSet::new();
        statics.insert("s1".to_string());
        let f = ColumnFilter::OnlyStatic(statics);
        assert!(f.fetches("s1"));
        assert!(!f.fetches("regular"));
    }
}
