use std::sync::Arc;

pub mod datafusion;
mod expr;
mod operators;

pub use expr::*;
pub use operators::*;

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
