use core::fmt;
use std::fmt::{Display, Formatter};

use vortex_dtype::field::FieldPath;
use vortex_scalar::Scalar;

use crate::operators::Operator;

#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Disjunction {
    conjunctions: Vec<Conjunction>,
}

impl Disjunction {
    pub fn iter(&self) -> impl Iterator<Item = &Conjunction> {
        self.conjunctions.iter()
    }
}

impl From<Conjunction> for Disjunction {
    fn from(value: Conjunction) -> Self {
        Self {
            conjunctions: vec![value],
        }
    }
}

impl FromIterator<Predicate> for Disjunction {
    fn from_iter<T: IntoIterator<Item = Predicate>>(iter: T) -> Self {
        Self {
            conjunctions: iter
                .into_iter()
                .map(|predicate| Conjunction::from_iter([predicate]))
                .collect(),
        }
    }
}

impl FromIterator<Conjunction> for Disjunction {
    fn from_iter<T: IntoIterator<Item = Conjunction>>(iter: T) -> Self {
        Self {
            conjunctions: iter.into_iter().collect(),
        }
    }
}

impl Display for Disjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.conjunctions
            .iter()
            .map(|v| format!("{}", v))
            .intersperse("\nOR \n".to_string())
            .try_for_each(|s| write!(f, "{}", s))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct Conjunction {
    predicates: Vec<Predicate>,
}

impl Conjunction {
    pub fn iter(&self) -> impl Iterator<Item = &Predicate> {
        self.predicates.iter()
    }
}

impl From<Predicate> for Conjunction {
    fn from(value: Predicate) -> Self {
        Self {
            predicates: vec![value],
        }
    }
}

impl FromIterator<Predicate> for Conjunction {
    fn from_iter<T: IntoIterator<Item = Predicate>>(iter: T) -> Self {
        Self {
            predicates: iter.into_iter().collect(),
        }
    }
}

impl Display for Conjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.predicates
            .iter()
            .map(|v| format!("{}", v))
            .intersperse(" AND ".to_string())
            .try_for_each(|s| write!(f, "{}", s))
    }
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
    pub lhs: FieldPath,
    pub op: Operator,
    pub rhs: Value,
}

pub fn lit<T: Into<Scalar>>(n: T) -> Value {
    Value::Literal(n.into())
}

impl Value {
    // NB: We rewrite predicates to be Field-op-predicate, so these methods all must
    // use the inverse operator.
    pub fn equals(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::Eq,
            rhs: self,
        }
    }

    pub fn not_equals(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::NotEq.inverse(),
            rhs: self,
        }
    }

    pub fn gt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::Gt.inverse(),
            rhs: self,
        }
    }

    pub fn gte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::Gte.inverse(),
            rhs: self,
        }
    }

    pub fn lt(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::Lt.inverse(),
            rhs: self,
        }
    }

    pub fn lte(self, field: impl Into<FieldPath>) -> Predicate {
        Predicate {
            lhs: field.into(),
            op: Operator::Lte.inverse(),
            rhs: self,
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::field::Field;

    use super::*;

    #[test]
    fn test_lit() {
        let scalar: Scalar = 1.into();
        let value: Value = lit(scalar);
        let field = Field::from("id");
        let expr = Predicate {
            lhs: FieldPath::from_iter([field]),
            op: Operator::Eq,
            rhs: value,
        };
        assert_eq!(format!("{}", expr), "($id = 1)");
    }
}
