use std::ops;

use crate::expressions::Predicate;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Operator {
    // comparison
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

impl ops::Not for Predicate {
    type Output = Self;

    fn not(self) -> Self::Output {
        let inverse_op = match self.op {
            Operator::EqualTo => Operator::NotEqualTo,
            Operator::NotEqualTo => Operator::EqualTo,
            Operator::GreaterThan => Operator::LessThanOrEqualTo,
            Operator::GreaterThanOrEqualTo => Operator::LessThan,
            Operator::LessThan => Operator::GreaterThanOrEqualTo,
            Operator::LessThanOrEqualTo => Operator::GreaterThan,
        };
        Predicate {
            left: self.left,
            op: inverse_op,
            right: self.right,
        }
    }
}

impl Operator {
    pub fn inverse(self) -> Self {
        match self {
            Operator::EqualTo => Operator::NotEqualTo,
            Operator::NotEqualTo => Operator::EqualTo,
            Operator::GreaterThan => Operator::LessThanOrEqualTo,
            Operator::GreaterThanOrEqualTo => Operator::LessThan,
            Operator::LessThan => Operator::GreaterThanOrEqualTo,
            Operator::LessThanOrEqualTo => Operator::GreaterThan,
        }
    }
}
