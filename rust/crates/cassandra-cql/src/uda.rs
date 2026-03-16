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

//! User-Defined Aggregate (UDA) registry and lifecycle.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.UDAggregate`
//!
//! ## Lifecycle
//! 1. SFUNC is called for each row → accumulates state
//! 2. FINALFUNC (optional) transforms final state → result

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::udf::{UdfExecutor, UdfValue};

/// Metadata for a registered UDA.
#[derive(Debug, Clone)]
pub struct UdaMetadata {
    pub keyspace: String,
    pub name: String,
    pub arg_types: Vec<String>,
    pub state_type: String,
    pub sfunc_name: String,
    pub finalfunc_name: Option<String>,
    pub initcond: Option<String>,
}

/// A running aggregate instance.
pub struct AggregateState {
    pub metadata: UdaMetadata,
    pub state: UdfValue,
    pub sfunc: Arc<dyn UdfExecutor>,
    pub finalfunc: Option<Arc<dyn UdfExecutor>>,
}

impl AggregateState {
    /// Create a new aggregate instance with initial condition.
    pub fn new(
        metadata: UdaMetadata,
        sfunc: Arc<dyn UdfExecutor>,
        finalfunc: Option<Arc<dyn UdfExecutor>>,
        init: UdfValue,
    ) -> Self {
        Self {
            metadata,
            state: init,
            sfunc,
            finalfunc,
        }
    }

    /// Accumulate a new value.
    pub fn accumulate(&mut self, args: &[UdfValue]) -> Result<(), String> {
        let mut all_args = vec![self.state.clone()];
        all_args.extend_from_slice(args);
        self.state = self.sfunc.execute(&all_args)?;
        Ok(())
    }

    /// Finalize and return the result.
    pub fn finalize(self) -> Result<UdfValue, String> {
        match self.finalfunc {
            Some(ff) => ff.execute(&[self.state]),
            None => Ok(self.state),
        }
    }
}

/// Registry for user-defined aggregates.
pub struct UdaRegistry {
    aggregates: RwLock<HashMap<String, UdaMetadata>>,
}

impl UdaRegistry {
    pub fn new() -> Self {
        Self {
            aggregates: RwLock::new(HashMap::new()),
        }
    }

    fn make_key(keyspace: &str, name: &str, arg_types: &[String]) -> String {
        format!("{}.{}({})", keyspace, name, arg_types.join(","))
    }

    pub fn register(&self, metadata: UdaMetadata) -> Result<(), String> {
        let key = Self::make_key(&metadata.keyspace, &metadata.name, &metadata.arg_types);
        self.aggregates.write().insert(key, metadata);
        Ok(())
    }

    pub fn get(&self, keyspace: &str, name: &str, arg_types: &[String]) -> Option<UdaMetadata> {
        let key = Self::make_key(keyspace, name, arg_types);
        self.aggregates.read().get(&key).cloned()
    }

    pub fn unregister(&self, keyspace: &str, name: &str, arg_types: &[String]) -> bool {
        let key = Self::make_key(keyspace, name, arg_types);
        self.aggregates.write().remove(&key).is_some()
    }

    pub fn len(&self) -> usize {
        self.aggregates.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.aggregates.read().is_empty()
    }
}

impl Default for UdaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get() {
        let registry = UdaRegistry::new();
        let meta = UdaMetadata {
            keyspace: "ks".into(),
            name: "my_sum".into(),
            arg_types: vec!["int".into()],
            state_type: "int".into(),
            sfunc_name: "plus".into(),
            finalfunc_name: None,
            initcond: Some("0".into()),
        };
        registry.register(meta).unwrap();
        assert_eq!(registry.len(), 1);

        let got = registry.get("ks", "my_sum", &["int".into()]).unwrap();
        assert_eq!(got.sfunc_name, "plus");
    }

    #[test]
    fn accumulate_and_finalize() {
        use crate::udf::UdfExecutor;

        struct SumFn;
        impl UdfExecutor for SumFn {
            fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String> {
                let mut total = 0i32;
                for a in args {
                    if let UdfValue::Int(n) = a {
                        total += n;
                    }
                }
                Ok(UdfValue::Int(total))
            }
            fn language(&self) -> &str {
                "rust"
            }
        }

        let meta = UdaMetadata {
            keyspace: "ks".into(),
            name: "my_sum".into(),
            arg_types: vec!["int".into()],
            state_type: "int".into(),
            sfunc_name: "plus".into(),
            finalfunc_name: None,
            initcond: Some("0".into()),
        };

        let mut agg = AggregateState::new(meta, Arc::new(SumFn), None, UdfValue::Int(0));

        agg.accumulate(&[UdfValue::Int(10)]).unwrap();
        agg.accumulate(&[UdfValue::Int(20)]).unwrap();
        agg.accumulate(&[UdfValue::Int(30)]).unwrap();

        let result = agg.finalize().unwrap();
        assert!(matches!(result, UdfValue::Int(60)));
    }
}
