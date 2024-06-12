use vortex_dtype::field::FieldPath;

use crate::expressions::{Predicate, Value};
use crate::operators::Operator;

pub trait FieldPathOperations {
    fn equal(&self, other: Value) -> Predicate;
    fn not_equal(&self, other: Value) -> Predicate;
    fn gt(&self, other: Value) -> Predicate;
    fn gte(&self, other: Value) -> Predicate;
    fn lt(&self, other: Value) -> Predicate;
    fn lte(&self, other: Value) -> Predicate;
}

impl FieldPathOperations for FieldPath {
    // comparisons
    fn equal(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::Eq,
            rhs: other,
        }
    }

    fn not_equal(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::NotEq,
            rhs: other,
        }
    }

    fn gt(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::Gt,
            rhs: other,
        }
    }

    fn gte(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::Gte,
            rhs: other,
        }
    }

    fn lt(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::Lt,
            rhs: other,
        }
    }

    fn lte(&self, other: Value) -> Predicate {
        Predicate {
            lhs: self.clone(),
            op: Operator::Lte,
            rhs: other,
        }
    }
}
