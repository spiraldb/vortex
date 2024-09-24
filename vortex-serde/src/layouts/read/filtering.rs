use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::BoolArray;
use vortex::compute::and;
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::VortexResult;
use vortex_expr::{expr_is_filter, split_conjunction, VortexExpr};

use crate::layouts::Schema;

#[derive(Debug, Clone)]
pub struct RowFilter {
    conjunction: Vec<Arc<dyn VortexExpr>>,
}

impl RowFilter {
    pub fn new(expr: Arc<dyn VortexExpr>) -> Self {
        let conjunction = split_conjunction(&expr)
            .into_iter()
            .filter(expr_is_filter)
            .collect();

        Self { conjunction }
    }

    /// Evaluate the underlying filter against a target array, returning a boolean mask
    pub fn evaluate(&self, target: &Array) -> VortexResult<Array> {
        let mut mask = BoolArray::from(vec![true; target.len()]).into_array();
        for expr in self.conjunction.iter() {
            let new_mask = expr.evaluate(target)?;
            mask = and(new_mask, mask)?;

            if mask.statistics().compute_true_count().unwrap_or_default() == 0 {
                return Ok(
                    BoolArray::from_vec(vec![false; target.len()], Validity::AllValid).into_array(),
                );
            }
        }

        Ok(mask)
    }

    /// Returns a set of all referenced fields in the underlying filter
    pub fn references(&self) -> HashSet<Field> {
        let mut set = HashSet::new();
        for expr in self.conjunction.iter() {
            set.extend(expr.references().iter().cloned());
        }

        set
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }

    /// Re-order the expression so the sub-expressions estimated to be the "cheapest" are first (to the left of the expression)
    pub fn reorder(mut self, schema: &Schema) -> RowFilter {
        // Sort in ascending order of cost
        self.conjunction
            .sort_by_key(|e| Reverse(e.estimate_cost(schema)));

        self
    }
}
