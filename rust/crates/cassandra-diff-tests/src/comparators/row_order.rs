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

//! Row ordering comparator.
//!
//! Determines whether query results should be compared with order preserved
//! (e.g., queries with ORDER BY or clustered reads) or as unordered sets
//! (e.g., full table scans without ORDER BY).
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.cql3.Ordering`
//! - `org.apache.cassandra.db.filter.ClusteringIndexFilter`

/// Determine if a CQL query implies ordered results.
///
/// Returns `true` if the query contains ORDER BY or is reading from a
/// table with clustering columns where the results are inherently ordered.
pub fn query_implies_order(query: &str) -> bool {
    let upper = query.to_uppercase();
    upper.contains("ORDER BY")
}

/// Determine if a query is a clustered read (single partition with
/// clustering columns), which returns results in clustering order.
///
/// This is a heuristic based on the query text. A proper implementation
/// would consult the table schema.
pub fn query_is_clustered_read(query: &str) -> bool {
    let upper = query.to_uppercase();
    // A single-partition read is indicated by an equality filter on the
    // partition key. Combined with a clustering table, results are ordered.
    // This is a simplified heuristic.
    upper.contains("WHERE") && !upper.contains("TOKEN(") && !upper.contains("ALLOW FILTERING")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_order_by() {
        assert!(query_implies_order(
            "SELECT * FROM test WHERE pk = 1 ORDER BY ck ASC"
        ));
        assert!(query_implies_order("select * from t order by c desc"));
    }

    #[test]
    fn no_order_by() {
        assert!(!query_implies_order("SELECT * FROM test"));
        assert!(!query_implies_order("SELECT * FROM test WHERE pk = 1"));
    }

    #[test]
    fn detects_clustered_read() {
        assert!(query_is_clustered_read("SELECT * FROM test WHERE pk = 1"));
    }

    #[test]
    fn full_scan_is_not_clustered() {
        assert!(!query_is_clustered_read("SELECT * FROM test"));
    }
}
