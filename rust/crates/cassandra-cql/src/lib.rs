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

//! # cassandra-cql
//!
//! CQL parser, statement types, query validation, and prepared statement caching.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.cql3`
//!
//! ## Architecture
//!
//! Pipeline: input → **Lexer** → tokens → **Parser** → AST → **Planner** → QueryPlan
//!
//! The parser is hand-rolled recursive descent for:
//! - Full control over error messages with position info
//! - Zero external grammar files or build-time codegen
//! - Streaming-friendly design for future zero-copy parsing

pub mod lexer;
pub mod ast;
pub mod parser;
pub mod planner;
pub mod prepared;
pub mod udf;
pub mod uda;
pub mod triggers;
