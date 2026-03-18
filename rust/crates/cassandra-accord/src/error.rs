// Licensed under Apache License, Version 2.0.

//! Error types for the Accord subsystem.

use crate::types::{CommandStatus, TxnId};

/// Errors from Accord operations.
#[derive(Debug, thiserror::Error)]
pub enum AccordError {
    #[error("Accord is disabled")]
    Disabled,

    #[error("Transaction {txn_id} timed out")]
    Timeout { txn_id: TxnId },

    #[error("Transaction {txn_id} was invalidated")]
    Invalidated { txn_id: TxnId },

    #[error("Preempted by higher timestamp transaction")]
    Preempted,

    #[error("Insufficient replicas: required {required}, available {available}")]
    Unavailable { required: usize, available: usize },

    #[error("Invalid status transition from {from} to {to}")]
    InvalidTransition {
        from: CommandStatus,
        to: CommandStatus,
    },

    #[error("Journal write failed: {0}")]
    JournalError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for Accord operations.
pub type AccordResult<T> = Result<T, AccordError>;

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn error_display() {
        let err = AccordError::Disabled;
        assert_eq!(err.to_string(), "Accord is disabled");
    }

    #[test]
    fn timeout_includes_txn_id() {
        let txn_id = TxnId::with_timestamp(100, Uuid::nil(), 0);
        let err = AccordError::Timeout { txn_id };
        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn result_type_works() {
        let ok: AccordResult<u32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: AccordResult<u32> = Err(AccordError::Disabled);
        assert!(err.is_err());
    }
}
