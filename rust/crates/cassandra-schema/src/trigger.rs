// Licensed under Apache License, Version 2.0.

//! Trigger metadata for tables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.TriggerMetadata`

use serde::{Deserialize, Serialize};

/// Definition of a trigger attached to a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDefinition {
    /// Trigger name.
    pub name: String,
    /// Fully qualified Java class implementing the trigger.
    pub trigger_class: String,
}

impl TriggerDefinition {
    /// Create a new trigger definition.
    pub fn new(name: impl Into<String>, trigger_class: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            trigger_class: trigger_class.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_trigger_definition() {
        let trigger = TriggerDefinition::new("audit_trigger", "com.example.AuditTrigger");
        assert_eq!(trigger.name, "audit_trigger");
        assert_eq!(trigger.trigger_class, "com.example.AuditTrigger");
    }

    #[test]
    fn serde_round_trip() {
        let trigger = TriggerDefinition::new("t1", "com.Foo");
        let json = serde_json::to_string(&trigger).unwrap();
        let deserialized: TriggerDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(trigger, deserialized);
    }
}
