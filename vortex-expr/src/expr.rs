use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::{ConstantArray, StructArray};
use vortex::compute::{compare, Operator as ArrayOperator};
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_dtype::field::Field;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexExpect as _, VortexResult, VortexUnwrap};
use vortex_scalar::Scalar;
use vortex_schema::Schema;

use crate::Operator;

const NON_PRIMITIVE_COST_ESTIMATE: usize = 64;
const COLUMN_COST_MULTIPLIER: usize = 1024;

pub trait VortexExpr: Debug + Send + Sync + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, batch: &Array) -> VortexResult<Array>;

    fn references(&self) -> HashSet<Field>;

    fn estimate_cost(&self, schema: &Schema) -> usize;
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

#[derive(Debug, PartialEq, Hash, Clone, Eq)]
pub struct NoOp;

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

    fn estimate_cost(&self, schema: &Schema) -> usize {
        let field_dtype = schema.field_type(self.field()).vortex_unwrap();

        dtype_cost_estimate(&field_dtype) * COLUMN_COST_MULTIPLIER
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

    fn estimate_cost(&self, _schema: &Schema) -> usize {
        dtype_cost_estimate(self.value.dtype())
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

    fn estimate_cost(&self, schema: &Schema) -> usize {
        self.lhs.estimate_cost(schema) + self.rhs.estimate_cost(schema)
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

impl VortexExpr for NoOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, _array: &Array) -> VortexResult<Array> {
        vortex_bail!("NoOp::evaluate() should not be called")
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }

    fn estimate_cost(&self, _schema: &Schema) -> usize {
        0
    }
}

impl PartialEq<dyn Any> for NoOp {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other).downcast_ref::<Self>().is_some()
    }
}

fn dtype_cost_estimate(dtype: &DType) -> usize {
    match dtype {
        vortex_dtype::DType::Null => 0,
        vortex_dtype::DType::Bool(_) => 1,
        vortex_dtype::DType::Primitive(p, _) => p.byte_width(),
        _ => NON_PRIMITIVE_COST_ESTIMATE,
    }
}
