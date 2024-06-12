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
            op: Operator::Eq,
            right: other,
        }
    }

    fn not_eq(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::NotEq,
            right: other,
        }
    }

    fn gt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::Gt,
            right: other,
        }
    }

    fn gte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::Gte,
            right: other,
        }
    }

    fn lt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::Lt,
            right: other,
        }
    }

    fn lte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::Lte,
            right: other,
        }
    }
}
