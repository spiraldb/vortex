#![allow(dead_code)]

use vortex_dtype::FieldName;

use crate::expressions::{FieldExpr, PredicateExpr, Value};
use crate::expressions::Value::Field;
use crate::operators::Operator;

pub fn predicate(left: Value, op: Operator, right: Value) -> PredicateExpr {
    PredicateExpr::new(left, op, right)
}

/// Create a field expression based on a qualified field name.
pub fn field(field_name: impl Into<FieldName>) -> Value {
    Field(FieldExpr::new(field_name))
}

pub fn equals(left: Value, right: Value) -> PredicateExpr {
    predicate(
        left,
        Operator::EqualTo,
        right,
    )
}