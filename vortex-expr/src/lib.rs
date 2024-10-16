#![feature(iter_intersperse)]

use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

mod binary;
mod column;
pub mod datafusion;
mod identity;
mod literal;
mod operators;
mod select;

pub use binary::*;
pub use column::*;
pub use identity::*;
pub use literal::*;
pub use operators::*;
pub use select::*;
use vortex::Array;
use vortex_dtype::field::Field;
use vortex_error::{VortexExpect, VortexResult};

/// [`VortexExpr`] represents logical operation on [`Array`]s
pub trait VortexExpr: Debug + Send + Sync + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, batch: &Array) -> VortexResult<Array>;

    fn references(&self) -> HashSet<Field>;
}

/// Split expression into conjunctive normal form
pub fn split_conjunction(expr: &Arc<dyn VortexExpr>) -> Vec<Arc<dyn VortexExpr>> {
    split_inner(expr, vec![])
}

fn split_inner(
    expr: &Arc<dyn VortexExpr>,
    mut exprs: Vec<Arc<dyn VortexExpr>>,
) -> Vec<Arc<dyn VortexExpr>> {
    match expr.as_any().downcast_ref::<BinaryExpr>() {
        Some(bexp) if bexp.op() == Operator::And => {
            let split = split_inner(bexp.lhs(), exprs);
            split_inner(bexp.rhs(), split)
        }
        Some(_) | None => {
            exprs.push(expr.clone());
            exprs
        }
    }
}

// Taken from apache-datafusion, necessary since you can't require VortexExpr implement PartialEq<dyn VortexExpr>
pub(crate) fn unbox_any(any: &dyn Any) -> &dyn Any {
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

#[cfg(test)]
mod tests {
    use vortex_dtype::field::Field;

    use super::*;

    #[test]
    fn basic_expr_split_test() {
        let lhs = Arc::new(Column::new(Field::Name("a".to_string()))) as _;
        let rhs = Arc::new(Literal::new(1.into())) as _;
        let expr = Arc::new(BinaryExpr::new(lhs, Operator::Eq, rhs)) as _;
        let conjunction = split_conjunction(&expr);
        assert_eq!(conjunction.len(), 1);
    }

    #[test]
    fn basic_conjunction_split_test() {
        let lhs = Arc::new(Column::new(Field::Name("a".to_string()))) as _;
        let rhs = Arc::new(Literal::new(1.into())) as _;
        let expr = Arc::new(BinaryExpr::new(lhs, Operator::And, rhs)) as _;
        let conjunction = split_conjunction(&expr);
        assert_eq!(conjunction.len(), 2, "Conjunction is {conjunction:?}");
    }
}
