use vortex_dtype::FieldName;
use vortex_scalar::Scalar;

use crate::expression_fns::binary_expr;
use crate::operators::Operator;

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// A binary expression such as "duration_seconds == 100"
    BinaryExpr(BinaryExpr),

    /// A named reference to a qualified field in a dtype.
    Field(FieldName),

    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>),

    /// A constant scalar value.
    Literal(Scalar),

    /// Additive inverse of an expression. The expression's type must be numeric.
    Minus(Box<Expr>),

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

    pub fn gt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GreaterThanOrEqualTo, other)
    }

    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LessThan, other)
    }

    pub fn lt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LessThanOrEqualTo, other)
    }

    // misc
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    pub fn minus(self) -> Self {
        Expr::Minus(Box::new(self))
    }
}

#[derive(Clone, Debug, PartialEq)]
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
