use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema;
use vortex_dtype::field::FieldPath;
use vortex_expr::{BinaryExpr, VortexExpr};

#[derive(Debug, Clone)]
pub struct RowFilter {
    pub(crate) filter: Arc<dyn VortexExpr>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>) -> Self {
        Self { filter }
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }

    pub fn reorder(mut self, arrow_schema: &Schema) -> RowFilter {
        let expr = reorder_expr_impl(self.filter.clone(), arrow_schema);
        self.filter = expr;
        self
    }
}

fn reorder_expr_impl(expr: Arc<dyn VortexExpr>, schema: &Schema) -> Arc<dyn VortexExpr> {
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let lhs = reorder_expr_impl(binary.lhs().clone(), schema);
        let rhs = reorder_expr_impl(binary.rhs().clone(), schema);
        // We want the cheapest operations "first" (to the left of the expression tree)
        let (lhs, rhs, operator) =
            if binary.lhs().estimate_cost(schema) > binary.rhs().estimate_cost(schema) {
                (rhs, lhs, binary.op().swap())
            } else {
                (lhs, rhs, binary.op())
            };

        Arc::new(BinaryExpr::new(lhs, operator, rhs))
    } else {
        expr
    }
}
