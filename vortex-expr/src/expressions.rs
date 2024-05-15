use vortex_dtype::FieldName;
use vortex_scalar::Scalar;

use crate::expressions::Value::Field;
use crate::operators::Operator;

#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde("transparent")
)]
pub struct Disjunction {
    pub conjunctions: Vec<Conjunction>,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde("transparent")
)]
pub struct Conjunction {
    pub predicates: Vec<Predicate>,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    /// A named reference to a qualified field in a dtype.
    Field(FieldName),
    /// A constant scalar value.
    Literal(Scalar),
}

impl Value {
    pub fn field(field_name: impl Into<FieldName>) -> Value {
        Field(field_name.into())
    }
    // comparisons
    pub fn eq(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::EqualTo,
            right: other,
        }
    }

    pub fn not_eq(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::NotEqualTo,
            right: other,
        }
    }

    pub fn gt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::GreaterThan,
            right: other,
        }
    }

    pub fn gte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::GreaterThanOrEqualTo,
            right: other,
        }
    }

    pub fn lt(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::LessThan,
            right: other,
        }
    }

    pub fn lte(self, other: Value) -> Predicate {
        Predicate {
            left: self,
            op: Operator::LessThanOrEqualTo,
            right: other,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Predicate {
    pub left: Value,
    pub op: Operator,
    pub right: Value,
}

pub fn lit<T: Into<Scalar>>(n: T) -> Value {
    Value::Literal(n.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lit() {
        let scalar: Scalar = 1.into();
        let rhs: Value = lit(scalar);
        let expr = Value::field("id").eq(rhs);
        assert_eq!(format!("{}", expr), "(id = 1)");
    }
}
