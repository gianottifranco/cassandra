// Licensed under Apache License, Version 2.0.

//! Cluster metadata helper functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.ClusterMetadataFcts`
//! - `org.apache.cassandra.tcm.Transformation.Kind`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::{CqlType, CqlValue};
use std::sync::Arc;

const TRANSFORMATION_KIND_NAMES: &[&str] = &[
    "PRE_INITIALIZE_CMS",
    "INITIALIZE_CMS",
    "FORCE_SNAPSHOT",
    "TRIGGER_SNAPSHOT",
    "SCHEMA_CHANGE",
    "REGISTER",
    "UNREGISTER",
    "UNSAFE_JOIN",
    "PREPARE_JOIN",
    "START_JOIN",
    "MID_JOIN",
    "FINISH_JOIN",
    "PREPARE_MOVE",
    "START_MOVE",
    "MID_MOVE",
    "FINISH_MOVE",
    "PREPARE_LEAVE",
    "START_LEAVE",
    "MID_LEAVE",
    "FINISH_LEAVE",
    "ASSASSINATE",
    "PREPARE_REPLACE",
    "START_REPLACE",
    "MID_REPLACE",
    "FINISH_REPLACE",
    "CANCEL_SEQUENCE",
    "START_ADD_TO_CMS",
    "FINISH_ADD_TO_CMS",
    "REMOVE_FROM_CMS",
    "STARTUP",
    "CUSTOM",
    "PREPARE_SIMPLE_CMS_RECONFIGURATION",
    "PREPARE_COMPLEX_CMS_RECONFIGURATION",
    "ADVANCE_CMS_RECONFIGURATION",
    "CANCEL_CMS_RECONFIGURATION",
    "ALTER_TOPOLOGY",
    "UPDATE_AVAILABILITY",
    "BEGIN_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE",
    "MAYBE_FINISH_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE",
    "ACCORD_MARK_STALE",
    "ACCORD_MARK_REJOINING",
    "PREPARE_DROP_ACCORD_TABLE",
    "FINISH_DROP_ACCORD_TABLE",
    "ACCORD_MARK_HARD_REMOVED",
];

pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(TransformationKindFunction));
}

struct TransformationKindFunction;

impl CqlFunction for TransformationKindFunction {
    fn name(&self) -> &str {
        "transformation_kind"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Int]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let id = match CqlValue::deserialize_value(&CqlType::Int, bytes)
            .map_err(|error| error.to_string())?
        {
            CqlValue::Int(value) => value,
            _ => unreachable!("deserialize_value(Int) returns Int"),
        };
        let Some(name) = usize::try_from(id)
            .ok()
            .and_then(|id| TRANSFORMATION_KIND_NAMES.get(id))
        else {
            return Err(format!("{id} is not a valid Transformation.Kind id"));
        };
        Ok(Some(
            CqlValue::Varchar((*name).to_string()).serialize_value(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::FunctionRegistry;

    #[test]
    fn transformation_kind_resolves_java_ids() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("transformation_kind", &[CqlType::Int])
            .unwrap();

        assert_eq!(function.return_type(), CqlType::Varchar);
        assert_eq!(
            decode(function.execute(&[Some(&0i32.to_be_bytes())]).unwrap()),
            Some("PRE_INITIALIZE_CMS".to_string())
        );
        assert_eq!(
            decode(function.execute(&[Some(&4i32.to_be_bytes())]).unwrap()),
            Some("SCHEMA_CHANGE".to_string())
        );
        assert_eq!(
            decode(function.execute(&[Some(&43i32.to_be_bytes())]).unwrap()),
            Some("ACCORD_MARK_HARD_REMOVED".to_string())
        );
    }

    #[test]
    fn transformation_kind_rejects_invalid_ids() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("transformation_kind", &[CqlType::Int])
            .unwrap();

        assert_eq!(
            function.execute(&[Some(&(-1i32).to_be_bytes())]),
            Err("-1 is not a valid Transformation.Kind id".to_string())
        );
        assert_eq!(
            function.execute(&[Some(&44i32.to_be_bytes())]),
            Err("44 is not a valid Transformation.Kind id".to_string())
        );
    }

    #[test]
    fn transformation_kind_returns_null_for_null_input() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("transformation_kind", &[CqlType::Int])
            .unwrap();

        assert_eq!(function.execute(&[None]).unwrap(), None);
    }

    fn decode(bytes: Option<Vec<u8>>) -> Option<String> {
        bytes.map(
            |bytes| match CqlValue::deserialize_value(&CqlType::Varchar, &bytes).unwrap() {
                CqlValue::Varchar(value) => value,
                _ => unreachable!("deserialize_value(Varchar) returns Varchar"),
            },
        )
    }
}
