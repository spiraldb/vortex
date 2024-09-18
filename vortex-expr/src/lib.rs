#![feature(iter_intersperse)]

use std::sync::Arc;

pub mod datafusion;
mod expr;
mod operators;

pub use expr::*;
pub use operators::*;

pub fn split_conjunction(expr: &Arc<dyn VortexExpr>) -> Vec<Arc<dyn VortexExpr>> {
    match expr.as_any().downcast_ref::<BinaryExpr>() {
        Some(bexp) if bexp.op() == Operator::And => {
            let mut lhs = split_conjunction(bexp.lhs());
            lhs.extend_from_slice(&split_conjunction(bexp.rhs()));
            lhs
        }
        Some(_) | None => {
            vec![expr.clone()]
        }
    }
}
