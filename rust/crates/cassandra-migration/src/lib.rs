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

//! # cassandra-migration
//!
//! Migration, upgrade, and rollback tooling for the Cassandra Java→Rust rewrite.
//!
//! ## Modules
//!
//! | Module            | Purpose                                           |
//! |-------------------|---------------------------------------------------|
//! | `version_matrix`  | Queryable upgrade compatibility matrix             |
//! | `sstable_import`  | Java SSTable (big/bti) import & conversion         |
//! | `schema_diff`     | Schema comparison between Java and Rust catalogs   |
//! | `data_validator`  | Row-count, checksum, and query-level validation    |
//! | `cdc_continuity`  | CDC segment continuity across migration boundary   |
//! | `fql_converter`   | FQL log format conversion (Chronicle→JSON)         |
//!
//! ## Java Oracle References
//!
//! - `org.apache.cassandra.io.sstable.format.*` (SSTable formats)
//! - `org.apache.cassandra.schema.SchemaKeyspace` (schema export)
//! - `org.apache.cassandra.db.commitlog.CommitLog` (CDC)
//! - `org.apache.cassandra.tools.fqltool.*` (FQL)

pub mod cdc_continuity;
pub mod data_validator;
pub mod fql_converter;
pub mod schema_diff;
pub mod sstable_import;
pub mod version_matrix;

#[cfg(test)]
mod tests {
    #[test]
    fn exported_modules_are_reachable() {
        let supported = crate::version_matrix::supported_paths();
        assert!(!supported.is_empty());
    }
}
