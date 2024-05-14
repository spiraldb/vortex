use serde::{Deserialize, Serialize};
use vortex_dtype::FieldName;
use vortex_scalar::Scalar;

use crate::expression_fns::binary_expr;
use crate::operators::Operator;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum Expr {
    /// A binary expression such as "duration_seconds == 100"
    Binary(BinaryExpr),

    /// A named reference to a qualified field in a dtype.
    Field(FieldName),

    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>),

    /// A constant scalar value.
    Literal(Scalar),

    /// Negation of an expression. The expression's type must be a boolean.
    Not(Box<Expr>),
}

impl Expr {
    // binary logic

    pub fn and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::And, other)
    }

    pub fn or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Or, other)
    }

    // comparisons

    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::EqualTo, other)
    }

    pub fn not_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::NotEqualTo, other)
    }

    pub fn gt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GreaterThan, other)
    }

    pub fn gte(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GreaterThanOrEqualTo, other)
    }

    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LessThan, other)
    }

    pub fn lte(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LessThanOrEqualTo, other)
    }

    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}
