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
            let mut exprs = split_conjunction(bexp.lhs());
            exprs.extend_from_slice(&split_conjunction(bexp.rhs()));
            exprs
        }
        Some(_) | None => vec![expr.clone()],
    }
}

pub fn expr_is_filter(expr: &Arc<dyn VortexExpr>) -> bool {
    expr.as_any().downcast_ref::<BinaryExpr>().is_some()
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
