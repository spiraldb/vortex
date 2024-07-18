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
                let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;

                // TODO(adamg): turn and/or into more general compute functions
                match expr.op {
                    DFOperator::And => {
                        let lhs = lhs.into_bool()?;
                        let rhs = rhs.into_bool()?;
                        let buffer = &lhs.boolean_buffer() & &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Or => {
                        let lhs = lhs.into_bool()?;
                        let rhs = rhs.into_bool()?;
                        let buffer = &lhs.boolean_buffer() | &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Eq => compare(&lhs, &rhs, Operator::Eq),
                    DFOperator::Gt => compare(&lhs, &rhs, Operator::Gt),
                    DFOperator::GtEq => compare(&lhs, &rhs, Operator::Gte),
                    DFOperator::Lt => compare(&lhs, &rhs, Operator::Lt),
                    DFOperator::LtEq => compare(&lhs, &rhs, Operator::Lte),
                    DFOperator::NotEq => compare(&lhs, &rhs, Operator::NotEq),
                    _ => vortex_bail!("{} is an unsupported operator", expr.op),
                }
            }
            Expr::Column(col) => array.with_dyn(|a| {
                let name = col.name();
                a.as_struct_array_unchecked()
                    .field_by_name(name)
                    .ok_or(vortex_err!("Missing field {name} in struct array"))
            }),
            Expr::Literal(lit) => Ok(ConstantArray::new(lit.clone(), array.len()).into_array()),
            _ => unreachable!(),
        }
    }
}
