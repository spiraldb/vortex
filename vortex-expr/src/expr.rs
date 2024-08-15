#![allow(dead_code)]

use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::{ConstantArray, StructArray};
use vortex::compute::{compare, Operator as ArrayOperator};
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::Operator;

pub trait VortexExpr: Debug + Send + Sync {
    fn evaluate(&self, array: &Array) -> VortexResult<Array>;
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
    name: String,
}

impl Column {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl VortexExpr for Column {
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
}

impl VortexExpr for NoOp {
    fn evaluate(&self, _array: &Array) -> VortexResult<Array> {
        vortex_bail!("NoOp::evaluate() should not be called")
    }
}
