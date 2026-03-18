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

//! Type compatibility and assignment checking.
//!
//! Mirrors the assignment/compatibility logic from Java's `AbstractType` hierarchy.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.AbstractType.isCompatibleWith`
//! - `org.apache.cassandra.db.marshal.AbstractType.isValueCompatibleWith`
//! - `org.apache.cassandra.cql3.AssignmentTestable.TestResult`

use crate::native::CqlType;

/// Result of an assignment compatibility test between two CQL types.
///
/// ## Java Oracle
/// `org.apache.cassandra.cql3.AssignmentTestable.TestResult`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentResult {
    /// The types are identical.
    Exact,
    /// The value can be assigned but may require implicit conversion.
    Compatible,
    /// The types are not compatible for assignment.
    NotAssignable,
}

impl AssignmentResult {
    /// Returns `true` if the result is `Exact` or `Compatible`.
    pub fn is_assignable(&self) -> bool {
        !matches!(self, AssignmentResult::NotAssignable)
    }
}

/// Check if `from` is wire-format compatible with `to`.
///
/// "Wire compatible" means the same bytes decode correctly in both types.
/// For example, `tinyint` bytes are a valid subset of `smallint` bytes.
///
/// ## Java Oracle
/// `AbstractType.isCompatibleWith(AbstractType<?> previous)`
pub fn is_compatible_with(from: &CqlType, to: &CqlType) -> bool {
    if from == to {
        return true;
    }
    match (from, to) {
        // Numeric promotions (widening, same wire encoding family)
        (
            CqlType::Tinyint,
            CqlType::Smallint | CqlType::Int | CqlType::Bigint | CqlType::Varint,
        ) => true,
        (CqlType::Smallint, CqlType::Int | CqlType::Bigint | CqlType::Varint) => true,
        (CqlType::Int, CqlType::Bigint | CqlType::Varint) => true,
        (CqlType::Bigint, CqlType::Varint) => true,
        (CqlType::Float, CqlType::Double) => true,

        // Text/varchar/ascii compatibility
        (CqlType::Ascii, CqlType::Varchar) => true,
        (CqlType::Varchar, CqlType::Ascii) => false, // ASCII is stricter

        // Timestamp and bigint share the same wire format
        (CqlType::Timestamp, CqlType::Bigint) => true,
        (CqlType::Bigint, CqlType::Timestamp) => true,

        // Counter is a special bigint
        (CqlType::Counter, CqlType::Bigint) => true,

        // UUID variants
        (CqlType::Timeuuid, CqlType::Uuid) => true,

        // Collection covariance: inner types must be compatible
        (CqlType::List(from_inner, from_frozen), CqlType::List(to_inner, to_frozen)) => {
            from_frozen == to_frozen && is_compatible_with(from_inner, to_inner)
        }
        (CqlType::Set(from_inner, from_frozen), CqlType::Set(to_inner, to_frozen)) => {
            from_frozen == to_frozen && is_compatible_with(from_inner, to_inner)
        }
        (CqlType::Map(from_k, from_v, from_frozen), CqlType::Map(to_k, to_v, to_frozen)) => {
            from_frozen == to_frozen
                && is_compatible_with(from_k, to_k)
                && is_compatible_with(from_v, to_v)
        }

        // Frozen/unfrozen: frozen is compatible with same frozen state
        // Reversed: unwrap and check inner
        (CqlType::Reversed(from_inner), CqlType::Reversed(to_inner)) => {
            is_compatible_with(from_inner, to_inner)
        }

        _ => false,
    }
}

/// Check if values of `from` can be read as `to` without re-encoding.
///
/// This is a looser check than [`is_compatible_with`] — it only asks whether
/// the value bytes are valid for `to`, ignoring ordering guarantees.
///
/// ## Java Oracle
/// `AbstractType.isValueCompatibleWith(AbstractType<?> otherType)`
pub fn is_value_compatible_with(from: &CqlType, to: &CqlType) -> bool {
    if is_compatible_with(from, to) {
        return true;
    }
    match (from, to) {
        // Additional value-only compat (not ordering-compatible)
        (CqlType::Int, CqlType::Float) => true, // both 4 bytes
        (CqlType::Bigint, CqlType::Double) => true, // both 8 bytes
        (CqlType::Bigint, CqlType::Timestamp) => true,
        (CqlType::Timestamp, CqlType::Bigint) => true,
        _ => false,
    }
}

/// Determine how well `actual` can be assigned to a column/variable of type `expected`.
///
/// ## Java Oracle
/// `AssignmentTestable.testAssignment(String, ColumnSpecification)`
pub fn test_assignment(expected: &CqlType, actual: &CqlType) -> AssignmentResult {
    if expected == actual {
        return AssignmentResult::Exact;
    }
    // Unwrap reversed for assignment testing
    let expected = expected.unwrap_reversed();
    let actual = actual.unwrap_reversed();

    if expected == actual {
        return AssignmentResult::Exact;
    }

    if is_compatible_with(actual, expected) {
        return AssignmentResult::Compatible;
    }

    AssignmentResult::NotAssignable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_type_is_compatible() {
        assert!(is_compatible_with(&CqlType::Int, &CqlType::Int));
        assert!(is_compatible_with(&CqlType::Varchar, &CqlType::Varchar));
    }

    #[test]
    fn numeric_promotion() {
        assert!(is_compatible_with(&CqlType::Tinyint, &CqlType::Bigint));
        assert!(is_compatible_with(&CqlType::Smallint, &CqlType::Int));
        assert!(is_compatible_with(&CqlType::Float, &CqlType::Double));
        // Not downwards
        assert!(!is_compatible_with(&CqlType::Bigint, &CqlType::Int));
        assert!(!is_compatible_with(&CqlType::Double, &CqlType::Float));
    }

    #[test]
    fn text_compat() {
        assert!(is_compatible_with(&CqlType::Ascii, &CqlType::Varchar));
        // varchar does not narrow to ascii
        assert!(!is_compatible_with(&CqlType::Varchar, &CqlType::Ascii));
    }

    #[test]
    fn timeuuid_to_uuid() {
        assert!(is_compatible_with(&CqlType::Timeuuid, &CqlType::Uuid));
        assert!(!is_compatible_with(&CqlType::Uuid, &CqlType::Timeuuid));
    }

    #[test]
    fn collection_covariance() {
        let list_int = CqlType::List(Box::new(CqlType::Int), false);
        let list_bigint = CqlType::List(Box::new(CqlType::Bigint), false);
        assert!(is_compatible_with(&list_int, &list_bigint));
        assert!(!is_compatible_with(&list_bigint, &list_int));
    }

    #[test]
    fn assignment_exact() {
        assert_eq!(
            test_assignment(&CqlType::Int, &CqlType::Int),
            AssignmentResult::Exact
        );
    }

    #[test]
    fn assignment_compatible() {
        assert_eq!(
            test_assignment(&CqlType::Bigint, &CqlType::Int),
            AssignmentResult::Compatible
        );
    }

    #[test]
    fn assignment_not_assignable() {
        assert_eq!(
            test_assignment(&CqlType::Boolean, &CqlType::Int),
            AssignmentResult::NotAssignable
        );
    }

    #[test]
    fn assignment_result_is_assignable() {
        assert!(AssignmentResult::Exact.is_assignable());
        assert!(AssignmentResult::Compatible.is_assignable());
        assert!(!AssignmentResult::NotAssignable.is_assignable());
    }
}
