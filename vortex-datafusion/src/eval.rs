use datafusion_expr::{Expr, Operator as DFOperator};
use vortex::{
    array::{bool::BoolArray, constant::ConstantArray},
    compute::compare,
    Array, IntoArray, IntoArrayVariant,
};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::Operator;

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn eval(array: Array, expr: &Expr) -> VortexResult<Array> {
        match expr {
            Expr::BinaryExpr(expr) => {
                let lhs = expr.left.as_ref();
                let rhs = expr.right.as_ref();

                // TODO(adamg): turn and/or into more general compute functions
                match expr.op {
                    DFOperator::And => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), lhs)?.into_bool()?;
                        let rhs = ExpressionEvaluator::eval(array, rhs)?.into_bool()?;
                        let buffer = &lhs.boolean_buffer() & &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Or => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), lhs)?.into_bool()?;
                        let rhs = ExpressionEvaluator::eval(array.clone(), rhs)?.into_bool()?;
                        let buffer = &lhs.boolean_buffer() | &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Eq => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), lhs)?;
                        let rhs = ExpressionEvaluator::eval(array.clone(), rhs)?;
                        compare(&lhs, &rhs, Operator::Eq)
                    }
                    _ => vortex_bail!("{} is an unsupported operator", expr.op),
                }
            }
            Expr::Column(col) => {
                // TODO(adamg): Use variant trait once its merged
                let array = array.clone().into_struct()?;
                let name = col.name();
                array
                    .field_by_name(name)
                    .ok_or(vortex_err!("Missing field {name} in struct"))
            }
            Expr::Literal(lit) => Ok(ConstantArray::new(lit.clone(), array.len()).into_array()),
            _ => unreachable!(),
        }
    }
}
