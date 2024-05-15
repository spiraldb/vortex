use serde::{Deserialize, Serialize};
use vortex_dtype::FieldName;
use vortex_scalar::Scalar;

use crate::expression_fns::predicate;
use crate::literal::lit;
use crate::operators::Operator;

#[derive(Deserialize, Serialize)]
pub struct DNFExpr {
    pub conjunctions: Vec<ConjunctionExpr>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ConjunctionExpr {
    pub predicates: Vec<PredicateExpr>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum Value {
    /// A named reference to a qualified field in a dtype.
    Field(FieldExpr),
    /// A constant scalar value.
    Literal(Scalar),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(FieldExpr),
}

impl Value {
    // comparisons
    pub fn eq(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::EqualTo, other)
    }

    pub fn not_eq(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::NotEqualTo, other)
    }

    pub fn gt(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::GreaterThan, other)
    }

    pub fn gte(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::GreaterThanOrEqualTo, other)
    }

    pub fn lt(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::LessThan, other)
    }

    pub fn lte(self, other: Value) -> PredicateExpr {
        predicate(self, Operator::LessThanOrEqualTo, other)
    }
}

pub enum Predicate {
    Expression,
    Not,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PredicateExpr {
    pub left: Value,
    pub op: Operator,
    pub right: Value,
}

impl PredicateExpr {
    pub fn new(left: Value, op: Operator, right: Value) -> Self {
        Self { left, op, right }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct FieldExpr {
    pub field_name: FieldName,
}

impl FieldExpr {
    pub fn is_null(self) -> PredicateExpr {
        predicate(Value::IsNull(self), Operator::EqualTo, lit(true))
    }

    pub fn new(field_name: impl Into<FieldName>) -> Self {
        Self { field_name: field_name.into() }
    }
}