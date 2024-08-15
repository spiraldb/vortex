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

use std::fmt::Debug;
use std::sync::Arc;

use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::Operator;

pub trait VortexPhysicalExpr: Debug + Send + Sync {
    fn evaluate(&self, array: &Array) -> VortexResult<Array>;
}

#[derive(Debug)]
pub struct NoOp;

#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn VortexPhysicalExpr>,
    right: Arc<dyn VortexPhysicalExpr>,
    operator: DFOperator,
}

#[derive(Debug)]
pub struct Column {
    name: String,
    index: usize,
}

impl VortexPhysicalExpr for Column {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        let s = StructArray::try_from(array)?;

        let column = s.field_by_name(&self.name).ok_or(vortex_err!(
            "Array doesn't contain child array of name {}",
            self.name
        ))?;

        Ok(column)
    }
}

#[derive(Debug)]
pub struct Literal {
    scalar_value: Scalar,
}

impl VortexPhysicalExpr for Literal {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        Ok(ConstantArray::new(self.scalar_value.clone(), array.len()).into_array())
    }
}

impl VortexPhysicalExpr for BinaryExpr {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.left.evaluate(array)?;
        let rhs = self.right.evaluate(array)?;

        let array = match self.operator {
            DFOperator::Eq => compare(&lhs, &rhs, Operator::Eq)?,
            DFOperator::NotEq => compare(&lhs, &rhs, Operator::NotEq)?,
            DFOperator::Lt => compare(&lhs, &rhs, Operator::Lt)?,
            DFOperator::LtEq => compare(&lhs, &rhs, Operator::Lte)?,
            DFOperator::Gt => compare(&lhs, &rhs, Operator::Gt)?,
            DFOperator::GtEq => compare(&lhs, &rhs, Operator::Gte)?,
            DFOperator::And => vortex::compute::and(&lhs, &rhs)?,
            DFOperator::Or => vortex::compute::or(&lhs, &rhs)?,
            _ => vortex_bail!("{} is not a supported DF operator in Vortex", self.operator),
        };

        Ok(array)
    }
}

impl VortexPhysicalExpr for NoOp {
    fn evaluate(&self, _array: &Array) -> VortexResult<Array> {
        vortex_bail!("NoOp::evaluate() should not be called")
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
