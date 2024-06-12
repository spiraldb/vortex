#![cfg(feature = "datafusion")]
use datafusion_common::Column;
use datafusion_expr::{BinaryExpr, Expr};
use vortex_dtype::field_paths::{FieldIdentifier, FieldPath};
use vortex_scalar::Scalar;

use crate::expressions::{Predicate, Value};
use crate::operators::Operator;

impl From<Predicate> for Expr {
    fn from(value: Predicate) -> Self {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(FieldPathWrapper(value.left).into()),
            value.op.into(),
            Box::new(value.right.into()),
        ))
    }
}

impl From<Operator> for datafusion_expr::Operator {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Eq => datafusion_expr::Operator::Eq,
            Operator::NotEq => datafusion_expr::Operator::NotEq,
            Operator::Gt => datafusion_expr::Operator::Gt,
            Operator::Gte => datafusion_expr::Operator::GtEq,
            Operator::Lt => datafusion_expr::Operator::Lt,
            Operator::Lte => datafusion_expr::Operator::LtEq,
        }
    }
}

impl From<Value> for Expr {
    fn from(value: Value) -> Self {
        match value {
            Value::Field(field_path) => FieldPathWrapper(field_path).into(),
            Value::Literal(literal) => ScalarWrapper(literal).into(),
        }
    }
}

struct FieldPathWrapper(FieldPath);
impl From<FieldPathWrapper> for Expr {
    fn from(value: FieldPathWrapper) -> Self {
        let mut field = String::new();
        for part in value.0.parts() {
            match part {
                // TODO(ngates): escape quotes?
                FieldIdentifier::Name(identifier) => field.push_str(&format!("\"{}\"", identifier)),
                FieldIdentifier::ListIndex(idx) => field.push_str(&format!("[{}]", idx)),
            }
        }

        Expr::Column(Column::from(field))
    }
}

struct ScalarWrapper(Scalar);
impl From<ScalarWrapper> for Expr {
    fn from(value: ScalarWrapper) -> Self {
        Expr::Literal(value.0.into())
    }
}
