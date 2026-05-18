// Licensed under Apache License, Version 2.0.

//! User-defined function (UDF) and user-defined aggregate (UDA) metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.Functions`
//! - `org.apache.cassandra.cql3.functions.UDFunction`
//! - `org.apache.cassandra.cql3.functions.UDAggregate`

use serde::{Deserialize, Serialize};

/// Canonical CQL type name for function signatures.
///
/// Cassandra keys functions and aggregates by resolved CQL types, so aliases
/// such as `varchar` and `text` must map to the same signature.
pub fn canonical_cql_type_name(cql_type: &str) -> String {
    let normalized = cql_type.trim().to_ascii_lowercase();
    cassandra_types::native::parse_cql_type(&normalized)
        .map(|ty| ty.cql_name())
        .unwrap_or(normalized)
}

/// Metadata for a user-defined function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserFunction {
    /// Keyspace containing the function.
    pub keyspace: String,
    /// Function name.
    pub name: String,
    /// Argument names.
    pub arg_names: Vec<String>,
    /// Argument types (as CQL type strings).
    pub arg_types: Vec<String>,
    /// Return type (as CQL type string).
    pub return_type: String,
    /// Implementation language (e.g., "java", "javascript").
    pub language: String,
    /// Function body source code.
    pub body: String,
    /// Whether the function is called on null input.
    pub called_on_null_input: bool,
}

impl UserFunction {
    /// Create a new user-defined function.
    pub fn new(
        keyspace: impl Into<String>,
        name: impl Into<String>,
        return_type: impl Into<String>,
        language: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        let return_type = return_type.into();
        Self {
            keyspace: keyspace.into(),
            name: name.into(),
            arg_names: Vec::new(),
            arg_types: Vec::new(),
            return_type: canonical_cql_type_name(&return_type),
            language: language.into(),
            body: body.into(),
            called_on_null_input: false,
        }
    }

    /// Add an argument to the function.
    pub fn with_arg(mut self, name: impl Into<String>, arg_type: impl Into<String>) -> Self {
        let arg_type = arg_type.into();
        self.arg_names.push(name.into());
        self.arg_types.push(canonical_cql_type_name(&arg_type));
        self
    }

    /// Set called_on_null_input.
    pub fn with_called_on_null_input(mut self, called: bool) -> Self {
        self.called_on_null_input = called;
        self
    }

    /// Signature key for deduplication: name(arg_types).
    pub fn signature(&self) -> String {
        let arg_types = self
            .arg_types
            .iter()
            .map(|arg_type| canonical_cql_type_name(arg_type))
            .collect::<Vec<_>>();
        format!("{}({})", self.name, arg_types.join(", "))
    }
}

/// Metadata for a user-defined aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAggregate {
    /// Keyspace containing the aggregate.
    pub keyspace: String,
    /// Aggregate name.
    pub name: String,
    /// Argument types (as CQL type strings).
    pub arg_types: Vec<String>,
    /// State type (as CQL type string).
    pub state_type: String,
    /// Return type (as CQL type string).
    #[serde(default)]
    pub return_type: String,
    /// State function name.
    pub sfunc: String,
    /// Final function name (optional).
    pub finalfunc: Option<String>,
    /// Initial condition value (optional, as CQL literal string).
    pub initcond: Option<String>,
}

impl UserAggregate {
    /// Create a new user-defined aggregate.
    pub fn new(
        keyspace: impl Into<String>,
        name: impl Into<String>,
        state_type: impl Into<String>,
        sfunc: impl Into<String>,
    ) -> Self {
        let state_type = state_type.into();
        Self {
            keyspace: keyspace.into(),
            name: name.into(),
            arg_types: Vec::new(),
            state_type: canonical_cql_type_name(&state_type),
            return_type: canonical_cql_type_name(&state_type),
            sfunc: sfunc.into(),
            finalfunc: None,
            initcond: None,
        }
    }

    /// Add an argument type.
    pub fn with_arg_type(mut self, arg_type: impl Into<String>) -> Self {
        let arg_type = arg_type.into();
        self.arg_types.push(canonical_cql_type_name(&arg_type));
        self
    }

    /// Set the final function.
    pub fn with_finalfunc(mut self, finalfunc: impl Into<String>) -> Self {
        self.finalfunc = Some(finalfunc.into());
        self
    }

    /// Set the aggregate return type.
    pub fn with_return_type(mut self, return_type: impl Into<String>) -> Self {
        let return_type = return_type.into();
        self.return_type = canonical_cql_type_name(&return_type);
        self
    }

    /// Set the initial condition.
    pub fn with_initcond(mut self, initcond: impl Into<String>) -> Self {
        self.initcond = Some(initcond.into());
        self
    }

    /// Signature key for deduplication: name(arg_types).
    pub fn signature(&self) -> String {
        let arg_types = self
            .arg_types
            .iter()
            .map(|arg_type| canonical_cql_type_name(arg_type))
            .collect::<Vec<_>>();
        format!("{}({})", self.name, arg_types.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_user_function() {
        let udf = UserFunction::new("ks", "double_val", "int", "java", "return input * 2;")
            .with_arg("input", "int")
            .with_called_on_null_input(true);

        assert_eq!(udf.name, "double_val");
        assert_eq!(udf.keyspace, "ks");
        assert_eq!(udf.arg_names, vec!["input"]);
        assert_eq!(udf.arg_types, vec!["int"]);
        assert_eq!(udf.return_type, "int");
        assert_eq!(udf.language, "java");
        assert!(udf.called_on_null_input);
        assert_eq!(udf.signature(), "double_val(int)");
    }

    #[test]
    fn function_signature_canonicalizes_type_aliases() {
        let udf = UserFunction::new("ks", "f", "varchar", "java", "return x;")
            .with_arg("x", "VARCHAR")
            .with_arg("items", "list<varchar>");

        assert_eq!(udf.return_type, "text");
        assert_eq!(udf.arg_types, vec!["text", "list<text>"]);
        assert_eq!(udf.signature(), "f(text, list<text>)");
    }

    #[test]
    fn create_user_aggregate() {
        let uda = UserAggregate::new("ks", "my_avg", "tuple<int, bigint>", "avg_state")
            .with_arg_type("int")
            .with_finalfunc("avg_final")
            .with_initcond("(0, 0)");

        assert_eq!(uda.name, "my_avg");
        assert_eq!(uda.state_type, "tuple<int, bigint>");
        assert_eq!(uda.return_type, "tuple<int, bigint>");
        assert_eq!(uda.sfunc, "avg_state");
        assert_eq!(uda.finalfunc, Some("avg_final".into()));
        assert_eq!(uda.initcond, Some("(0, 0)".into()));
        assert_eq!(uda.signature(), "my_avg(int)");
    }

    #[test]
    fn aggregate_return_type_is_canonicalized() {
        let uda = UserAggregate::new("ks", "a", "varchar", "state")
            .with_return_type("list<varchar>")
            .with_arg_type("varchar");

        assert_eq!(uda.state_type, "text");
        assert_eq!(uda.return_type, "list<text>");
        assert_eq!(uda.signature(), "a(text)");
    }

    #[test]
    fn serde_round_trip_function() {
        let udf = UserFunction::new("ks", "f", "text", "java", "return x;").with_arg("x", "text");
        let json = serde_json::to_string(&udf).unwrap();
        let deserialized: UserFunction = serde_json::from_str(&json).unwrap();
        assert_eq!(udf, deserialized);
    }

    #[test]
    fn serde_round_trip_aggregate() {
        let uda = UserAggregate::new("ks", "a", "int", "sfn").with_arg_type("int");
        let json = serde_json::to_string(&uda).unwrap();
        let deserialized: UserAggregate = serde_json::from_str(&json).unwrap();
        assert_eq!(uda, deserialized);
    }
}
