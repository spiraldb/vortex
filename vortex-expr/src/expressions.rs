use vortex_dtype::field_paths::FieldPath;
use vortex_scalar::Scalar;

use crate::operators::Operator;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Disjunction {
    pub conjunctions: Vec<Conjunction>,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Conjunction {
    pub predicates: Vec<Predicate>,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    /// A named reference to a qualified field in a dtype.
    Field(FieldPath),
    /// A constant scalar value.
    Literal(Scalar),
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Predicate {
    pub left: FieldPath,
    pub op: Operator,
    pub right: Value,
}

pub fn lit<T: Into<Scalar>>(n: T) -> Value {
    Value::Literal(n.into())
}

impl Value {
    // NB: We rewrite predicates to be Field-op-predicate, so these methods all must
    // use the inverse operator.
    pub fn eq(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::EqualTo,
            right: self,
        }
    }

    pub fn not_eq(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::NotEqualTo.inverse(),
            right: self,
        }
    }

    pub fn gt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::GreaterThan.inverse(),
            right: self,
        }
    }

    pub fn gte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::GreaterThanOrEqualTo.inverse(),
            right: self,
        }
    }

    pub fn lt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::LessThan.inverse(),
            right: self,
        }
    }

    pub fn lte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            left: field.into(),
            op: Operator::LessThanOrEqualTo.inverse(),
            right: self,
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::field_paths::field;

    use super::*;

    #[test]
    fn test_lit() {
        let scalar: Scalar = 1.into();
        let value: Value = lit(scalar);
        let field = field("id");
        let expr = Predicate {
            left: field,
            op: Operator::EqualTo,
            right: value,
        };
        assert_eq!(format!("{}", expr), "($id = 1)");
    }
}
