use core::fmt;
use std::fmt::{Display, Formatter};

use crate::expressions::{Predicate, Value};

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.lhs, self.op, self.rhs)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Field(field_path) => Display::fmt(field_path, f),
            Value::Literal(scalar) => Display::fmt(&scalar, f),
        }
    }
}
