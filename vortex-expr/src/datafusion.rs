#![cfg(feature = "datafusion")]

use std::sync::Arc;

use datafusion_common::arrow::datatypes::Schema;
use datafusion_common::{Column, ExprSchema};
use datafusion_expr::{BinaryExpr, Expr};
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::expressions::{Predicate, Value};
use crate::operators::Operator;
use crate::physical::{Literal, NoOp, VortexPhysicalExpr};

impl From<Predicate> for Expr {
    fn from(value: Predicate) -> Self {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(FieldPathWrapper(value.lhs).into()),
            value.op.into(),
            Box::new(value.rhs.into()),
        ))
    }
}

impl From<Operator> for datafusion_expr::Operator {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Eq => datafusion_expr::Operator::Eq,
            Operator::NotEq => datafusion_expr::Operator::NotEq,
            Operator::Gt => datafusion_expr::Operator::Gt,
            Operator::Gte => datafusion_expr::Operator::GtEq,
            Operator::Lt => datafusion_expr::Operator::Lt,
            Operator::Lte => datafusion_expr::Operator::LtEq,
        }
    }
}

impl From<Value> for Expr {
    fn from(value: Value) -> Self {
        match value {
            Value::Field(field_path) => FieldPathWrapper(field_path).into(),
            Value::Literal(literal) => ScalarWrapper(literal).into(),
        }
    }
}

struct FieldPathWrapper(FieldPath);
impl From<FieldPathWrapper> for Expr {
    fn from(value: FieldPathWrapper) -> Self {
        let mut field = String::new();
        for part in value.0.path() {
            match part {
                // TODO(ngates): escape quotes?
                Field::Name(name) => field.push_str(&format!("\"{}\"", name)),
                Field::Index(idx) => field.push_str(&format!("[{}]", idx)),
            }
        }

        Expr::Column(Column::from(field))
    }
}

struct ScalarWrapper(Scalar);
impl From<ScalarWrapper> for Expr {
    fn from(value: ScalarWrapper) -> Self {
        Expr::Literal(value.0.into())
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

        return Ok(Arc::new(crate::physical::BinaryExpr {
            left,
            right,
            operator,
        }) as _);
    }

    if let Some(col_expr) = physical_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::Column>()
    {
        let expr = crate::physical::Column {
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
