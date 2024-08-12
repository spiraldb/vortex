#![allow(dead_code)]

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{Result as DFResult, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{and, lit, Expr, Operator as DFOperator};
use datafusion_physical_expr::PhysicalExpr;
use vortex::array::{ConstantArray, StructArray};
use vortex::compute::compare;
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::scalar::dfvalue_to_scalar;

/// Convert a set of expressions into a single AND expression.
///
/// # Returns
///
/// If conversion is successful, the result will be a
/// [binary expression node][datafusion_expr::Expr::BinaryExpr] containing the conjunction.
pub(crate) fn make_conjunction(exprs: impl AsRef<[Expr]>) -> DFResult<Expr> {
    Ok(exprs
        .as_ref()
        .iter()
        .fold(lit(true), |conj, elem| and(conj, elem.clone())))
}

/// Simplify an expression using DataFusion's builtin analysis passes.
///
/// This encapsulates common optimizations like constant folding and eliminating redundant
/// expressions, e.g. `value AND true`.
pub(crate) fn simplify_expr(expr: &Expr, schema: SchemaRef) -> DFResult<Expr> {
    let schema = schema.to_dfschema_ref()?;

    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    simplifier.simplify(expr.clone())
}

pub trait VortexPhysicalExpr: Send + Sync {
    fn evaluate(&self, array: &Array) -> VortexResult<Array>;
}

pub struct NoOp;

pub struct BinaryExpr {
    left: Arc<dyn VortexPhysicalExpr>,
    right: Arc<dyn VortexPhysicalExpr>,
    operator: DFOperator,
}

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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::{col, lit};

    use super::*;

    #[test]
    fn test_conjunction_simplify() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("bool_col", DataType::Boolean, false),
        ]));

        let exprs = vec![col("int_col").gt_eq(lit(4)), col("bool_col").is_true()];

        assert_eq!(
            simplify_expr(&make_conjunction(&exprs).unwrap(), schema).unwrap(),
            and(col("int_col").gt_eq(lit(4)), col("bool_col").is_true())
        );
    }
}
