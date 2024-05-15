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
            field: self,
            op: Operator::EqualTo,
            value: other,
        }
    }

    fn not_eq(self, other: Value) -> Predicate {
        Predicate {
            field: self,
            op: Operator::NotEqualTo,
            value: other,
        }
    }

    fn gt(self, other: Value) -> Predicate {
        Predicate {
            field: self,
            op: Operator::GreaterThan,
            value: other,
        }
    }

    fn gte(self, other: Value) -> Predicate {
        Predicate {
            field: self,
            op: Operator::GreaterThanOrEqualTo,
            value: other,
        }
    }

    fn lt(self, other: Value) -> Predicate {
        Predicate {
            field: self,
            op: Operator::LessThan,
            value: other,
        }
    }

    fn lte(self, other: Value) -> Predicate {
        Predicate {
            field: self,
            op: Operator::LessThanOrEqualTo,
            value: other,
        }
    }
}
