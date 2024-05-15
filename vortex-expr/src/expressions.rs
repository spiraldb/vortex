use vortex_dtype::field_paths::FieldPath;
use vortex_scalar::Scalar;

use crate::operators::Operator;

#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Disjunction {
    pub conjunctions: Vec<Conjunction>,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Conjunction {
    pub predicates: Vec<Predicate>,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    /// A named reference to a qualified field in a dtype.
    Field(FieldPath),
    /// A constant scalar value.
    Literal(Scalar),
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Predicate {
    pub field: FieldPath,
    pub op: Operator,
    pub value: Value,
}

pub fn lit<T: Into<Scalar>>(n: T) -> Value {
    Value::Literal(n.into())
}

impl Value {
    // comparisons
    pub fn eq(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::EqualTo,
            value: self,
        }
    }

    pub fn not_eq(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::NotEqualTo,
            value: self,
        }
    }

    pub fn gt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::GreaterThan,
            value: self,
        }
    }

    pub fn gte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::GreaterThanOrEqualTo,
            value: self,
        }
    }

    pub fn lt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::LessThan,
            value: self,
        }
    }

    pub fn lte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            field: field.into(),
            op: Operator::LessThanOrEqualTo,
            value: self,
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
            field,
            op: Operator::EqualTo,
            value,
        };
        assert_eq!(format!("{}", expr), "($id = 1)");
    }
}
