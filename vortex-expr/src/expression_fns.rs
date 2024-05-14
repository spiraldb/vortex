use vortex_dtype::FieldName;

use crate::expressions::BinaryExpr;
use crate::expressions::Expr;
use crate::operators::Operator;

#[allow(dead_code)]
pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// Create a field expression based on a qualified field name.
#[allow(dead_code)]
pub fn field(field: impl Into<FieldName>) -> Expr {
    Expr::Field(field.into())
}

#[allow(dead_code)]
pub fn equals(left: Expr, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr::new(
        Box::new(left),
        Operator::EqualTo,
        Box::new(right),
    ))
}

#[allow(dead_code)]
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr::new(
        Box::new(left),
        Operator::And,
        Box::new(right),
    ))
}

#[allow(dead_code)]
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr::new(
        Box::new(left),
        Operator::Or,
        Box::new(right),
    ))
}
