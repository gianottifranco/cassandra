// Licensed under Apache License, Version 2.0.

//! Abstract bounds hierarchy for token ranges.
//!
//! Models Java's `AbstractBounds<T>` hierarchy with four variants:
//! - `Range` — (left, right] (exclusive start, inclusive end)
//! - `Bounds` — [left, right] (both inclusive)
//! - `IncludingExcludingBounds` — [left, right) (inclusive start, exclusive end)
//! - `ExcludingBounds` — (left, right) (both exclusive)
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.AbstractBounds`
//! - `org.apache.cassandra.dht.Range`
//! - `org.apache.cassandra.dht.Bounds`
//! - `org.apache.cassandra.dht.IncludingExcludingBounds`
//! - `org.apache.cassandra.dht.ExcludingBounds`

use std::fmt;

/// Abstract bounds over an ordered type.
///
/// The four variants correspond to the combinations of inclusive/exclusive
/// start and end boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AbstractBounds<T: Ord + Clone> {
    /// (left, right] — exclusive start, inclusive end.
    Range { left: T, right: T },
    /// [left, right] — both inclusive.
    Bounds { left: T, right: T },
    /// [left, right) — inclusive start, exclusive end.
    IncludingExcluding { left: T, right: T },
    /// (left, right) — both exclusive.
    Excluding { left: T, right: T },
}

impl<T: Ord + Clone> AbstractBounds<T> {
    /// The left (start) boundary.
    pub fn left(&self) -> &T {
        match self {
            Self::Range { left, .. }
            | Self::Bounds { left, .. }
            | Self::IncludingExcluding { left, .. }
            | Self::Excluding { left, .. } => left,
        }
    }

    /// The right (end) boundary.
    pub fn right(&self) -> &T {
        match self {
            Self::Range { right, .. }
            | Self::Bounds { right, .. }
            | Self::IncludingExcluding { right, .. }
            | Self::Excluding { right, .. } => right,
        }
    }

    /// Whether the left boundary is inclusive.
    pub fn includes_left(&self) -> bool {
        matches!(
            self,
            Self::Bounds { .. } | Self::IncludingExcluding { .. }
        )
    }

    /// Whether the right boundary is inclusive.
    pub fn includes_right(&self) -> bool {
        matches!(self, Self::Range { .. } | Self::Bounds { .. })
    }

    /// Returns true if `point` is within these bounds.
    pub fn contains(&self, point: &T) -> bool {
        let left_ok = if self.includes_left() {
            point >= self.left()
        } else {
            point > self.left()
        };
        let right_ok = if self.includes_right() {
            point <= self.right()
        } else {
            point < self.right()
        };
        // Handle wrap-around: if left >= right, the range wraps
        if self.left() < self.right() {
            left_ok && right_ok
        } else if self.left() == self.right() {
            // Full range or empty depending on inclusivity
            self.includes_left() || self.includes_right() || point != self.left()
        } else {
            // Wrapping: either in [left, max] or [min, right]
            left_ok || right_ok
        }
    }

    /// Returns true if these bounds intersect with `other`.
    ///
    /// Two ranges intersect if they share at least one point.
    pub fn intersects(&self, other: &Self) -> bool {
        // Simple non-wrapping check: ranges don't intersect if one is
        // entirely before the other.
        if self.left() < self.right() && other.left() < other.right() {
            // Neither wraps
            let self_before_other = if self.includes_right() && other.includes_left() {
                self.right() < other.left()
            } else {
                self.right() <= other.left()
            };
            let other_before_self = if other.includes_right() && self.includes_left() {
                other.right() < self.left()
            } else {
                other.right() <= self.left()
            };
            !self_before_other && !other_before_self
        } else {
            // At least one wraps — use containment checks
            self.contains(other.left())
                || self.contains(other.right())
                || other.contains(self.left())
                || other.contains(self.right())
        }
    }

    /// Unwrap a wrapping range into one or two non-wrapping ranges.
    ///
    /// If the range wraps around (left >= right), it is split into two
    /// ranges: [left, max_val] and [min_val, right]. Otherwise returns
    /// the range as-is.
    pub fn unwrap(&self, min_val: &T, max_val: &T) -> Vec<Self> {
        if self.left() < self.right() {
            return vec![self.clone()];
        }
        // Split wrapping range into two non-wrapping ranges
        match self {
            Self::Range { left, right } => vec![
                Self::Range {
                    left: left.clone(),
                    right: max_val.clone(),
                },
                Self::Range {
                    left: min_val.clone(),
                    right: right.clone(),
                },
            ],
            Self::Bounds { left, right } => vec![
                Self::Bounds {
                    left: left.clone(),
                    right: max_val.clone(),
                },
                Self::Bounds {
                    left: min_val.clone(),
                    right: right.clone(),
                },
            ],
            Self::IncludingExcluding { left, right } => vec![
                Self::IncludingExcluding {
                    left: left.clone(),
                    right: max_val.clone(),
                },
                Self::IncludingExcluding {
                    left: min_val.clone(),
                    right: right.clone(),
                },
            ],
            Self::Excluding { left, right } => vec![
                Self::Excluding {
                    left: left.clone(),
                    right: max_val.clone(),
                },
                Self::Excluding {
                    left: min_val.clone(),
                    right: right.clone(),
                },
            ],
        }
    }

    /// Subtract `other` from `self`, returning the remaining portions.
    ///
    /// Returns a list of bounds representing `self - other`. Both ranges
    /// must be non-wrapping for correct results.
    pub fn subtract(&self, other: &Self) -> Vec<Self> {
        if !self.intersects(other) {
            return vec![self.clone()];
        }
        let mut result = Vec::new();
        // Left remainder: portion of self that is before other
        if self.left() < other.left()
            || (self.left() == other.left() && self.includes_left() && !other.includes_left())
        {
            let new_right = other.left().clone();
            let includes_new_right = !other.includes_left();
            let left_part = if self.includes_left() && includes_new_right {
                Self::Bounds {
                    left: self.left().clone(),
                    right: new_right,
                }
            } else if self.includes_left() {
                Self::IncludingExcluding {
                    left: self.left().clone(),
                    right: new_right,
                }
            } else if includes_new_right {
                Self::Range {
                    left: self.left().clone(),
                    right: new_right,
                }
            } else {
                Self::Excluding {
                    left: self.left().clone(),
                    right: new_right,
                }
            };
            result.push(left_part);
        }
        // Right remainder: portion of self that is after other
        if self.right() > other.right()
            || (self.right() == other.right()
                && self.includes_right()
                && !other.includes_right())
        {
            let new_left = other.right().clone();
            let includes_new_left = !other.includes_right();
            let right_part = if includes_new_left && self.includes_right() {
                Self::Bounds {
                    left: new_left,
                    right: self.right().clone(),
                }
            } else if includes_new_left {
                Self::IncludingExcluding {
                    left: new_left,
                    right: self.right().clone(),
                }
            } else if self.includes_right() {
                Self::Range {
                    left: new_left,
                    right: self.right().clone(),
                }
            } else {
                Self::Excluding {
                    left: new_left,
                    right: self.right().clone(),
                }
            };
            result.push(right_part);
        }
        result
    }
}

impl<T: Ord + Clone + fmt::Display> fmt::Display for AbstractBounds<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (l, r) = match self {
            Self::Range { .. } => ("(", "]"),
            Self::Bounds { .. } => ("[", "]"),
            Self::IncludingExcluding { .. } => ("[", ")"),
            Self::Excluding { .. } => ("(", ")"),
        };
        write!(f, "{}{}, {}{}", l, self.left(), self.right(), r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_contains() {
        let r = AbstractBounds::Range { left: 10, right: 20 };
        assert!(!r.contains(&10)); // exclusive start
        assert!(r.contains(&11));
        assert!(r.contains(&20)); // inclusive end
        assert!(!r.contains(&21));
    }

    #[test]
    fn bounds_contains() {
        let b = AbstractBounds::Bounds { left: 10, right: 20 };
        assert!(b.contains(&10)); // inclusive start
        assert!(b.contains(&20)); // inclusive end
        assert!(!b.contains(&9));
        assert!(!b.contains(&21));
    }

    #[test]
    fn including_excluding_contains() {
        let ie = AbstractBounds::IncludingExcluding { left: 10, right: 20 };
        assert!(ie.contains(&10));  // inclusive start
        assert!(!ie.contains(&20)); // exclusive end
        assert!(ie.contains(&19));
    }

    #[test]
    fn excluding_contains() {
        let e = AbstractBounds::Excluding { left: 10, right: 20 };
        assert!(!e.contains(&10)); // exclusive start
        assert!(!e.contains(&20)); // exclusive end
        assert!(e.contains(&15));
    }

    #[test]
    fn intersects_overlapping() {
        let a = AbstractBounds::Range { left: 10, right: 30 };
        let b = AbstractBounds::Range { left: 20, right: 40 };
        assert!(a.intersects(&b));
        assert!(b.intersects(&a));
    }

    #[test]
    fn intersects_disjoint() {
        let a = AbstractBounds::Range { left: 10, right: 20 };
        let b = AbstractBounds::Range { left: 30, right: 40 };
        assert!(!a.intersects(&b));
    }

    #[test]
    fn intersects_adjacent_range() {
        // (10, 20] and (20, 30] — share point 20
        let a = AbstractBounds::Range { left: 10, right: 20 };
        let b = AbstractBounds::Range { left: 20, right: 30 };
        // a includes 20, b excludes 20 — no shared point
        assert!(!a.intersects(&b));
    }

    #[test]
    fn intersects_adjacent_bounds() {
        // [10, 20] and [20, 30] — share point 20
        let a = AbstractBounds::Bounds { left: 10, right: 20 };
        let b = AbstractBounds::Bounds { left: 20, right: 30 };
        assert!(a.intersects(&b));
    }

    #[test]
    fn unwrap_non_wrapping() {
        let r = AbstractBounds::Range { left: 10, right: 20 };
        let parts = r.unwrap(&0, &100);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], r);
    }

    #[test]
    fn unwrap_wrapping() {
        let r = AbstractBounds::Range { left: 80, right: 20 };
        let parts = r.unwrap(&0, &100);
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0],
            AbstractBounds::Range { left: 80, right: 100 }
        );
        assert_eq!(
            parts[1],
            AbstractBounds::Range { left: 0, right: 20 }
        );
    }

    #[test]
    fn subtract_no_overlap() {
        let a = AbstractBounds::Range { left: 10, right: 20 };
        let b = AbstractBounds::Range { left: 30, right: 40 };
        let result = a.subtract(&b);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], a);
    }

    #[test]
    fn subtract_full_overlap() {
        let a = AbstractBounds::Range { left: 10, right: 20 };
        let b = AbstractBounds::Range { left: 5, right: 25 };
        let result = a.subtract(&b);
        assert!(result.is_empty());
    }

    #[test]
    fn subtract_partial_left() {
        let a = AbstractBounds::Range { left: 10, right: 30 };
        let b = AbstractBounds::Range { left: 5, right: 20 };
        let result = a.subtract(&b);
        assert_eq!(result.len(), 1);
        // Remaining should be (20, 30]
        assert!(result[0].contains(&25));
        assert!(!result[0].contains(&15));
    }

    #[test]
    fn subtract_middle() {
        let a = AbstractBounds::Range { left: 10, right: 40 };
        let b = AbstractBounds::Range { left: 20, right: 30 };
        let result = a.subtract(&b);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn display() {
        let r = AbstractBounds::Range { left: 10, right: 20 };
        assert_eq!(format!("{}", r), "(10, 20]");
        let b = AbstractBounds::Bounds { left: 10, right: 20 };
        assert_eq!(format!("{}", b), "[10, 20]");
        let ie = AbstractBounds::IncludingExcluding { left: 10, right: 20 };
        assert_eq!(format!("{}", ie), "[10, 20)");
        let e = AbstractBounds::Excluding { left: 10, right: 20 };
        assert_eq!(format!("{}", e), "(10, 20)");
    }

    #[test]
    fn accessors() {
        let r = AbstractBounds::Range { left: 10, right: 20 };
        assert_eq!(*r.left(), 10);
        assert_eq!(*r.right(), 20);
        assert!(!r.includes_left());
        assert!(r.includes_right());
    }
}
