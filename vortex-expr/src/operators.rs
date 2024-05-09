use std::ops;

use crate::expression_fns::binary_expr;
use crate::expressions::Expr;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Operator {
    // arithmetic
    Plus,
    Minus,
    UnaryMinus,
    Multiplication,
    Division,
    Modulo,
    // binary logic
    And,
    Or,
    // comparison
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

#[derive(PartialEq)]
pub enum Associativity {
    Left,
    Right,
    Neither,
}

/// Magic numbers from postgres docs:
/// <https://www.postgresql.org/docs/7.0/operators.htm#AEN2026>
impl Operator {
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 1,
            Operator::And => 2,
            Operator::NotEqualTo
            | Operator::EqualTo
            | Operator::LessThan
            | Operator::LessThanOrEqualTo
            | Operator::GreaterThan
            | Operator::GreaterThanOrEqualTo => 4,
            Operator::Plus | Operator::Minus => 13,
            Operator::Multiplication | Operator::Division | Operator::Modulo => 14,
            Operator::UnaryMinus => 17,
        }
    }

    pub fn associativity(&self) -> Associativity {
        match self {
            Operator::Or
            | Operator::And
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiplication
            | Operator::Division
            | Operator::Modulo => Associativity::Left,
            Operator::LessThanOrEqualTo
            | Operator::GreaterThan
            | Operator::GreaterThanOrEqualTo => Associativity::Neither,
            Operator::NotEqualTo
            | Operator::EqualTo
            | Operator::LessThan
            | Operator::UnaryMinus => Associativity::Right,
        }
    }
}

/// Various operator support
impl ops::Add for Expr {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Plus, rhs)
    }
}

impl ops::Sub for Expr {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Minus, rhs)
    }
}

impl ops::Mul for Expr {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Multiplication, rhs)
    }
}

impl ops::Div for Expr {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Division, rhs)
    }
}

impl ops::Rem for Expr {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Modulo, rhs)
    }
}

impl ops::Neg for Expr {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Expr::Minus(Box::new(self))
    }
}

impl ops::Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Expr::Not(Box::new(self))
    }
}
