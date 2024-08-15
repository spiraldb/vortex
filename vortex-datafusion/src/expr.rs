#![allow(dead_code)]

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_expr::Operator as DFOperator;
use datafusion_physical_expr::PhysicalExpr;
use vortex::array::{ConstantArray, StructArray};
use vortex::compute::compare;
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::scalar::dfvalue_to_scalar;

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

pub fn convert_expr_to_vortex(
    physical_expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> VortexResult<Arc<dyn VortexPhysicalExpr>> {
    if physical_expr.data_type(input_schema).unwrap().is_temporal() {
        vortex_bail!("Doesn't support evaluating operations over temporal values");
    }
    if let Some(binary_expr) = physical_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::BinaryExpr>()
    {
        let left = convert_expr_to_vortex(binary_expr.left().clone(), input_schema)?;
        let right = convert_expr_to_vortex(binary_expr.right().clone(), input_schema)?;
        let operator = *binary_expr.op();

        return Ok(Arc::new(BinaryExpr {
            left,
            right,
            operator,
        }) as _);
    }

    if let Some(col_expr) = physical_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::Column>()
    {
        let expr = Column {
            name: col_expr.name().to_owned(),
            index: col_expr.index(),
        };

        return Ok(Arc::new(expr) as _);
    }

    if let Some(lit) = physical_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
    {
        let value = dfvalue_to_scalar(lit.value().clone());
        return Ok(Arc::new(Literal {
            scalar_value: value,
        }) as _);
    }

    if physical_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::NoOp>()
        .is_some()
    {
        return Ok(Arc::new(NoOp));
    }

    vortex_bail!("Couldn't convert DataFusion physical expression to a vortex expression")
}

/// Extract all indexes of all columns referenced by the physical expressions from the schema
pub(crate) fn extract_columns_from_expr(
    expr: Option<&Arc<dyn PhysicalExpr>>,
    schema_ref: SchemaRef,
) -> DFResult<HashSet<usize>> {
    let mut predicate_projection = HashSet::new();

    if let Some(expr) = expr {
        expr.apply(|expr| {
            if let Some(column) = expr
                .as_any()
                .downcast_ref::<datafusion_physical_expr::expressions::Column>()
            {
                match schema_ref.column_with_name(column.name()) {
                    Some(_) => {
                        predicate_projection.insert(column.index());
                    }
                    None => {
                        return Err(DataFusionError::External(
                            format!("Could not find expected column {} in schema", column.name())
                                .into(),
                        ))
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }

    Ok(predicate_projection)
}
