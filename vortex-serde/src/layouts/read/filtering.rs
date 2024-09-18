use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::Array;
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::VortexResult;
use vortex_expr::{BinaryExpr, VortexExpr};

use crate::layouts::Schema;

#[derive(Debug, Clone)]
pub struct RowFilter {
    filter: Arc<dyn VortexExpr>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>) -> Self {
        Self { filter }
    }

    /// Evaluate the underlying filter against a target array, returning a boolean mask
    pub fn evaluate(&self, target: &Array) -> VortexResult<Array> {
        self.filter.evaluate(target)
    }

    /// Returns a set of all referenced fields in the underlying filter
    pub fn references(&self) -> HashSet<Field> {
        self.filter.references()
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }

    /// Re-order the expression so the sub-expressions estimated to be the "cheapest" are first (to the left of the expression)
    pub fn reorder(mut self, schema: &Schema) -> RowFilter {
        let expr = reorder_expr_impl(self.filter.clone(), schema);
        self.filter = expr;
        self
    }
}

fn reorder_expr_impl(expr: Arc<dyn VortexExpr>, schema: &Schema) -> Arc<dyn VortexExpr> {
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let lhs = reorder_expr_impl(binary.lhs().clone(), schema);
        let rhs = reorder_expr_impl(binary.rhs().clone(), schema);

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
