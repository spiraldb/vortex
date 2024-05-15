use vortex_dtype::field_paths::FieldPath;

use crate::expressions::{Predicate, Value};
use crate::operators::Operator;

pub trait FieldPathOperations {
    fn eq(self, other: Value) -> Predicate;
    fn not_eq(self, other: Value) -> Predicate;
    fn gt(self, other: Value) -> Predicate;
    fn gte(self, other: Value) -> Predicate;
    fn lt(self, other: Value) -> Predicate;
    fn lte(self, other: Value) -> Predicate;
}

impl FieldPathOperations for FieldPath {
    // comparisons
    fn eq(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::EqualTo,
            right: other,
        }
    }

    fn not_eq(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::NotEqualTo,
            right: other,
        }
    }

    fn gt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::GreaterThan,
            right: other,
        }
    }

    fn gte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::GreaterThanOrEqualTo,
            right: other,
        }
    }

    fn lt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::LessThan,
            right: other,
        }
    }

    fn lte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::LessThanOrEqualTo,
            right: other,
        }
    }
}
