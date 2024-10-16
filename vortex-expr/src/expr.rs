use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::{ConstantArray, StructArray};
use vortex::compute::{compare, Operator as ArrayOperator};
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_dtype::field::Field;
use vortex_error::{vortex_err, VortexExpect as _, VortexResult};
use vortex_scalar::Scalar;

use crate::Operator;

pub trait VortexExpr: Debug + Send + Sync + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, batch: &Array) -> VortexResult<Array>;

    fn references(&self) -> HashSet<Field>;
}

// Taken from apache-datafusion, necessary since you can't require VortexExpr implement PartialEq<dyn VortexExpr>
fn unbox_any(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn VortexExpr>>() {
        any.downcast_ref::<Arc<dyn VortexExpr>>()
            .vortex_expect("any.is::<Arc<dyn VortexExpr>> returned true but downcast_ref failed")
            .as_any()
    } else if any.is::<Box<dyn VortexExpr>>() {
        any.downcast_ref::<Box<dyn VortexExpr>>()
            .vortex_expect("any.is::<Box<dyn VortexExpr>> returned true but downcast_ref failed")
            .as_any()
    } else {
        any
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    lhs: Arc<dyn VortexExpr>,
    operator: Operator,
    rhs: Arc<dyn VortexExpr>,
}

impl BinaryExpr {
    pub fn new(lhs: Arc<dyn VortexExpr>, operator: Operator, rhs: Arc<dyn VortexExpr>) -> Self {
        Self { lhs, operator, rhs }
    }

    pub fn lhs(&self) -> &Arc<dyn VortexExpr> {
        &self.lhs
    }

    pub fn rhs(&self) -> &Arc<dyn VortexExpr> {
        &self.rhs
    }

    pub fn op(&self) -> Operator {
        self.operator
    }
}

#[derive(Debug, PartialEq, Hash, Clone, Eq)]
pub struct Column {
    field: Field,
}

impl Column {
    pub fn new(field: Field) -> Self {
        Self { field }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

impl From<String> for Column {
    fn from(value: String) -> Self {
        Column::new(value.into())
    }
}

impl From<usize> for Column {
    fn from(value: usize) -> Self {
        Column::new(value.into())
    }
}

impl VortexExpr for Column {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let s = StructArray::try_from(batch)?;

        let column = match &self.field {
            Field::Name(n) => s.field_by_name(n),
            Field::Index(i) => s.field(*i),
        }
        .ok_or_else(|| vortex_err!("Array doesn't contain child array {}", self.field))?;
        Ok(column)
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::from([self.field.clone()])
    }
}

impl PartialEq<dyn Any> for Column {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == self)
            .unwrap_or(false)
    }
}

#[derive(Debug, PartialEq)]
pub struct Literal {
    value: Scalar,
}

impl Literal {
    pub fn new(value: Scalar) -> Self {
        Self { value }
    }
}

impl VortexExpr for Literal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        Ok(ConstantArray::new(self.value.clone(), batch.len()).into_array())
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }
}

impl PartialEq<dyn Any> for Literal {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == self)
            .unwrap_or(false)
    }
}

impl VortexExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let lhs = self.lhs.evaluate(batch)?;
        let rhs = self.rhs.evaluate(batch)?;

        let array = match self.operator {
            Operator::Eq => compare(lhs, rhs, ArrayOperator::Eq)?,
            Operator::NotEq => compare(lhs, rhs, ArrayOperator::NotEq)?,
            Operator::Lt => compare(lhs, rhs, ArrayOperator::Lt)?,
            Operator::Lte => compare(lhs, rhs, ArrayOperator::Lte)?,
            Operator::Gt => compare(lhs, rhs, ArrayOperator::Gt)?,
            Operator::Gte => compare(lhs, rhs, ArrayOperator::Gte)?,
            Operator::And => vortex::compute::and(lhs, rhs)?,
            Operator::Or => vortex::compute::or(lhs, rhs)?,
        };

        Ok(array)
    }

    fn references(&self) -> HashSet<Field> {
        let mut res = self.lhs.references();
        res.extend(self.rhs.references());
        res
    }
}

impl PartialEq<dyn Any> for BinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x.operator == self.operator && x.lhs.eq(&self.lhs) && x.rhs.eq(&self.rhs))
            .unwrap_or(false)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Identity;

impl VortexExpr for Identity {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        Ok(batch.clone())
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }
}

impl PartialEq<dyn Any> for Identity {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == other)
            .unwrap_or(false)
    }
}
