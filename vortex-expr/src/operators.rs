use std::ops;

use crate::expressions::Expr;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Operator {
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
        }
    }

    pub fn associativity(&self) -> Associativity {
        match self {
            Operator::Or | Operator::And => Associativity::Left,
            Operator::LessThanOrEqualTo
            | Operator::GreaterThan
            | Operator::GreaterThanOrEqualTo => Associativity::Neither,
            Operator::NotEqualTo | Operator::EqualTo | Operator::LessThan => Associativity::Right,
        }
    }
}

/// Various operator support
impl ops::Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Expr::Not(Box::new(self))
    }
}
