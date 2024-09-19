use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::BoolArray;
use vortex::compute::{and, filter};
use vortex::validity::Validity;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::VortexResult;
use vortex_expr::{expr_is_filter, split_conjunction, BinaryExpr, VortexExpr};

use super::null_as_false;
use crate::layouts::Schema;

#[derive(Debug, Clone)]
pub struct RowFilter {
    conjunction: Vec<Arc<dyn VortexExpr>>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>, schema: Schema) -> Self {
        let mut conjunction = split_conjunction(&filter);
        // Sort in ascending order of cost
        conjunction.sort_by_key(|e| Reverse(e.estimate_cost(&schema)));
        let conjunction = conjunction.into_iter().filter(expr_is_filter).collect();

        Self { conjunction }
    }

    /// Evaluate the underlying filter against a target array, returning a boolean mask
    pub fn apply(&self, target: &Array) -> VortexResult<Array> {
        let mut target = target.clone();
        for expr in self.conjunction.iter() {
            let mask = expr.evaluate(&target)?;
            let mask = null_as_false(mask.into_bool()?)?;
            target = filter(target, mask)?;
        }

        Ok(target)
    }

    pub fn evaluate(&self, target: &Array) -> VortexResult<Array> {
        let mut mask =
            BoolArray::from_vec(vec![true; target.len()], Validity::AllValid).into_array();

        for expr in self.conjunction.iter() {
            let expr_result = expr.evaluate(target)?;
            mask = and(mask, expr_result)?;
        }

        Ok(mask)
    }

    /// Returns a set of all referenced fields in the underlying filter
    pub fn references(&self) -> HashSet<Field> {
        let mut set = HashSet::new();
        for expr in self.conjunction.iter() {
            let references = expr.references();
            set.extend(references.iter().cloned());
        }

        set
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }

    // /// Re-order the expression so the sub-expressions estimated to be the "cheapest" are first (to the left of the expression)
    // pub fn reorder(mut self, schema: &Schema) -> RowFilter {
    //     let expr = reorder_expr_impl(self.filter.clone(), schema);
    //     self.filter = expr;
    //     self
    // }
}

#[allow(dead_code)]
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
