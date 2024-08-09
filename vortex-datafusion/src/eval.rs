use datafusion_expr::{Expr, Operator as DFOperator};
use vortex::array::ConstantArray;
use vortex::compute::{and, compare, or};
use vortex::{Array, IntoArray};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::Operator;

use crate::can_be_pushed_down;
use crate::scalar::dfvalue_to_scalar;

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn eval(array: Array, expr: &Expr) -> VortexResult<Array> {
        debug_assert!(can_be_pushed_down(expr));

        match expr {
            Expr::BinaryExpr(expr) => {
                let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                // TODO(adamg): turn and/or into more general compute functions
                match expr.op {
                    DFOperator::And => and(&lhs, &rhs),
                    DFOperator::Or => or(&lhs, &rhs),
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
                a.as_struct_array()
                    .and_then(|a| a.field_by_name(name))
                    .ok_or(vortex_err!("Missing field {name} in struct array"))
            }),
            Expr::Literal(lit) => {
                let lit = dfvalue_to_scalar(lit.clone());
                Ok(ConstantArray::new(lit, array.len()).into_array())
            }
            _ => unreachable!(),
        }
    }
}
