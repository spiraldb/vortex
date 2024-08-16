use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::{ConstantArray, StructArray};
use vortex::compute::{compare, Operator as ArrayOperator};
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::Operator;

pub trait VortexExpr: Debug + Send + Sync {
    fn evaluate(&self, array: &Array) -> VortexResult<Array>;

    fn references(&self) -> HashSet<Field>;
}

#[derive(Debug)]
pub struct NoOp;

#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn VortexExpr>,
    right: Arc<dyn VortexExpr>,
    operator: Operator,
}

impl BinaryExpr {
    pub fn new(left: Arc<dyn VortexExpr>, operator: Operator, right: Arc<dyn VortexExpr>) -> Self {
        Self {
            left,
            right,
            operator,
        }
    }
}

#[derive(Debug)]
pub struct Column {
    field: FieldPath,
}

impl Column {
    pub fn new(field: String) -> Self {
        Self {
            field: FieldPath::from_name(field),
        }
    }
}

impl VortexExpr for Column {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        let s = StructArray::try_from(array)?;

        if let Some(first) = self.field.path().get(0) {
            let column = match first {
                Field::Name(n) => s.field_by_name(n),
                Field::Index(i) => s.field(*i),
            }
            .ok_or_else(|| vortex_err!("Array doesn't contain child array of name {first}"))?;
            Ok(column)
        } else {
            vortex_bail!("Empty column reference")
        }
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::from([self.field.path()[0].clone()])
    }
}

#[derive(Debug)]
pub struct Literal {
    value: Scalar,
}

impl Literal {
    pub fn new(value: Scalar) -> Self {
        Self { value }
    }
}

impl VortexExpr for Literal {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        Ok(ConstantArray::new(self.value.clone(), array.len()).into_array())
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }
}

impl VortexExpr for BinaryExpr {
    fn evaluate(&self, array: &Array) -> VortexResult<Array> {
        let lhs = self.left.evaluate(array)?;
        let rhs = self.right.evaluate(array)?;

        let array = match self.operator {
            Operator::Eq => compare(&lhs, &rhs, ArrayOperator::Eq)?,
            Operator::NotEq => compare(&lhs, &rhs, ArrayOperator::NotEq)?,
            Operator::Lt => compare(&lhs, &rhs, ArrayOperator::Lt)?,
            Operator::Lte => compare(&lhs, &rhs, ArrayOperator::Lte)?,
            Operator::Gt => compare(&lhs, &rhs, ArrayOperator::Gt)?,
            Operator::Gte => compare(&lhs, &rhs, ArrayOperator::Gte)?,
            Operator::And => vortex::compute::and(&lhs, &rhs)?,
            Operator::Or => vortex::compute::or(&lhs, &rhs)?,
        };

        Ok(array)
    }

    fn references(&self) -> HashSet<Field> {
        let mut res = self.left.references();
        res.extend(self.right.references());
        res
    }
}

impl VortexExpr for NoOp {
    fn evaluate(&self, _array: &Array) -> VortexResult<Array> {
        vortex_bail!("NoOp::evaluate() should not be called")
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }
}
