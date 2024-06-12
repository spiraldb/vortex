use core::fmt;
use std::fmt::{Display, Formatter};
use std::ops;

use vortex_dtype::NativePType;

use crate::expressions::Predicate;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Operator {
    // comparison
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
        };
        write!(f, "{display}")
    }
}

impl ops::Not for Predicate {
    type Output = Self;

    fn not(self) -> Self::Output {
        let inverse_op = match self.op {
            Operator::Eq => Operator::NotEq,
            Operator::NotEq => Operator::Eq,
            Operator::Gt => Operator::Lte,
            Operator::Gte => Operator::Lt,
            Operator::Lt => Operator::Gte,
            Operator::Lte => Operator::Gt,
        };
        Predicate {
            lhs: self.lhs,
            op: inverse_op,
            rhs: self.rhs,
        }
    }
}

impl Operator {
    pub fn inverse(self) -> Self {
        match self {
            Operator::Eq => Operator::NotEq,
            Operator::NotEq => Operator::Eq,
            Operator::Gt => Operator::Lte,
            Operator::Gte => Operator::Lt,
            Operator::Lt => Operator::Gte,
            Operator::Lte => Operator::Gt,
        }
    }

    pub fn to_predicate<T: NativePType>(&self) -> fn(&T, &T) -> bool {
        match self {
            Operator::Eq => PartialEq::eq,
            Operator::NotEq => PartialEq::ne,
            Operator::Gt => PartialOrd::gt,
            Operator::Gte => PartialOrd::ge,
            Operator::Lt => PartialOrd::lt,
            Operator::Lte => PartialOrd::le,
        }
    }
}
