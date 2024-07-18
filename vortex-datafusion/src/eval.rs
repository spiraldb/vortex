use datafusion_expr::{Expr, Operator as DFOperator};
use vortex::{
    array::{bool::BoolArray, constant::ConstantArray},
    compute::compare,
    Array, IntoArray, IntoArrayVariant,
};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::Operator;

use crate::can_be_pushed_down;

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn eval(array: Array, expr: &Expr) -> VortexResult<Array> {
        debug_assert!(can_be_pushed_down(expr));
        let original_len = array.len();
        match expr {
            Expr::BinaryExpr(expr) => {
                // TODO(adamg): turn and/or into more general compute functions
                match expr.op {
                    DFOperator::And => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let lhs = lhs.into_bool()?;

                        if lhs.true_count() == 0 {
                            return Ok(ConstantArray::new(false, original_len).into_array());
                        }

                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        let rhs = rhs.into_bool()?;

                        if rhs.true_count() == 0 {
                            return Ok(ConstantArray::new(false, original_len).into_array());
                        }

                        let buffer = &lhs.boolean_buffer() & &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Or => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let lhs = lhs.into_bool()?;

                        if lhs.true_count() == original_len {
                            return Ok(ConstantArray::new(true, original_len).into_array());
                        }

                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        let rhs = rhs.into_bool()?;

                        if lhs.true_count() == original_len {
                            return Ok(ConstantArray::new(true, original_len).into_array());
                        }

                        let buffer = &lhs.boolean_buffer() | &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    DFOperator::Eq => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::Eq)
                    }
                    DFOperator::Gt => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::Gt)
                    }
                    DFOperator::GtEq => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::Gte)
                    }
                    DFOperator::Lt => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::Lt)
                    }
                    DFOperator::LtEq => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::Lte)
                    }
                    DFOperator::NotEq => {
                        let lhs = ExpressionEvaluator::eval(array.clone(), expr.left.as_ref())?;
                        let rhs = ExpressionEvaluator::eval(array, expr.right.as_ref())?;
                        compare(&lhs, &rhs, Operator::NotEq)
                    }
                    _ => vortex_bail!("{} is an unsupported operator", expr.op),
                }
            }
            Expr::Column(col) => array.with_dyn(|a| {
                let name = col.name();
                a.as_struct_array()
                    .and_then(|a| a.field_by_name(name))
                    .ok_or(vortex_err!("Missing field {name} in struct array"))
            }),
            Expr::Literal(lit) => Ok(ConstantArray::new(lit.clone(), array.len()).into_array()),
            _ => unreachable!(),
        }
    }
}
