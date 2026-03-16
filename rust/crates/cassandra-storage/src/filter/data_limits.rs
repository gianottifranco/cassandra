// Licensed under Apache License, Version 2.0.

//! Data limits for CQL LIMIT and paging.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.filter.DataLimits`

/// Data limits applied to read operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataLimits {
    /// Standard CQL LIMIT.
    CqlLimit {
        /// Maximum total rows to return. `u32::MAX` = unlimited.
        rows_limit: u32,
        /// Maximum rows per partition. `u32::MAX` = unlimited.
        per_partition_limit: u32,
    },
    /// CQL LIMIT with paging state.
    CqlPagingLimit {
        /// Maximum total rows to return.
        rows_limit: u32,
        /// Maximum rows per partition.
        per_partition_limit: u32,
        /// Remaining rows in current page.
        remaining: u32,
    },
}

impl DataLimits {
    /// Unlimited (no row or partition limit).
    pub const UNLIMITED: Self = Self::CqlLimit {
        rows_limit: u32::MAX,
        per_partition_limit: u32::MAX,
    };

    /// Returns the effective total rows limit.
    pub fn rows_limit(&self) -> u32 {
        match self {
            Self::CqlLimit { rows_limit, .. } => *rows_limit,
            Self::CqlPagingLimit { remaining, .. } => *remaining,
        }
    }

    /// Returns the effective per-partition limit.
    pub fn per_partition_limit(&self) -> u32 {
        match self {
            Self::CqlLimit {
                per_partition_limit,
                ..
            } => *per_partition_limit,
            Self::CqlPagingLimit {
                per_partition_limit,
                ..
            } => *per_partition_limit,
        }
    }

    /// Count a row. Returns the new remaining count.
    pub fn count_row(&self, counted: u32) -> u32 {
        let limit = self.rows_limit();
        if limit == u32::MAX {
            u32::MAX
        } else {
            limit.saturating_sub(counted)
        }
    }

    /// Returns `true` if the limit is exhausted.
    pub fn is_exhausted(&self, counted: u32) -> bool {
        counted >= self.rows_limit()
    }

    /// Create a new limit for paging after the given number of rows.
    pub fn for_paging_after(&self, rows_counted: u32) -> Self {
        match self {
            Self::CqlLimit {
                rows_limit,
                per_partition_limit,
            } => Self::CqlPagingLimit {
                rows_limit: *rows_limit,
                per_partition_limit: *per_partition_limit,
                remaining: rows_limit.saturating_sub(rows_counted),
            },
            Self::CqlPagingLimit {
                rows_limit,
                per_partition_limit,
                remaining,
            } => Self::CqlPagingLimit {
                rows_limit: *rows_limit,
                per_partition_limit: *per_partition_limit,
                remaining: remaining.saturating_sub(rows_counted),
            },
        }
    }

    /// Returns `true` if this is unlimited.
    pub fn is_unlimited(&self) -> bool {
        *self == Self::UNLIMITED
    }
}

impl Default for DataLimits {
    fn default() -> Self {
        Self::UNLIMITED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited() {
        let l = DataLimits::UNLIMITED;
        assert!(l.is_unlimited());
        assert_eq!(l.rows_limit(), u32::MAX);
        assert_eq!(l.per_partition_limit(), u32::MAX);
        assert!(!l.is_exhausted(1_000_000));
    }

    #[test]
    fn cql_limit() {
        let l = DataLimits::CqlLimit {
            rows_limit: 10,
            per_partition_limit: 5,
        };
        assert!(!l.is_unlimited());
        assert_eq!(l.rows_limit(), 10);
        assert_eq!(l.per_partition_limit(), 5);
        assert!(!l.is_exhausted(9));
        assert!(l.is_exhausted(10));
    }

    #[test]
    fn count_row() {
        let l = DataLimits::CqlLimit {
            rows_limit: 10,
            per_partition_limit: u32::MAX,
        };
        assert_eq!(l.count_row(0), 10);
        assert_eq!(l.count_row(5), 5);
        assert_eq!(l.count_row(10), 0);
        assert_eq!(l.count_row(15), 0); // saturates
    }

    #[test]
    fn paging() {
        let l = DataLimits::CqlLimit {
            rows_limit: 100,
            per_partition_limit: u32::MAX,
        };
        let paged = l.for_paging_after(30);
        assert_eq!(paged.rows_limit(), 70);

        let paged2 = paged.for_paging_after(20);
        assert_eq!(paged2.rows_limit(), 50);
    }

    #[test]
    fn paging_exhaustion() {
        let l = DataLimits::CqlLimit {
            rows_limit: 10,
            per_partition_limit: u32::MAX,
        };
        let paged = l.for_paging_after(10);
        assert_eq!(paged.rows_limit(), 0);
        assert!(paged.is_exhausted(0));
    }
}
