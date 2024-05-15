use std::ops;

use serde::{Deserialize, Serialize};

use crate::expression_fns::predicate;
use crate::expressions::PredicateExpr;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum Operator {
    // comparison
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

impl ops::Not for PredicateExpr {
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
        predicate(self.left, inverse_op, self.right)
    }
}
