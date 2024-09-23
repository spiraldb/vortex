use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use vortex::array::BoolArray;
use vortex::compute::filter;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::field::{Field, FieldPath};
use vortex_error::{VortexExpect, VortexResult};
use vortex_expr::{split_conjunction, BinaryExpr, VortexExpr};

use super::null_as_false;
use crate::layouts::Schema;

#[derive(Debug, Clone)]
pub struct RowFilter {
    conjunction: Vec<Arc<dyn VortexExpr>>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>) -> Self {
        let conjunction = split_conjunction(&filter);
        Self { conjunction }
    }

    /// Evaluate the underlying filter against a target array, returning a boolean mask
    pub fn evaluate(&self, target: &Array) -> VortexResult<Array> {
        let mut target = target.clone();
        let mut mask = BoolArray::from(vec![true; target.len()]);
        for expr in self.conjunction.iter() {
            let new_mask = expr.evaluate(&target)?;
            let new_mask = null_as_false(new_mask.into_bool()?)?;
            target = filter(target, &new_mask)?;
            mask = bool_array_and_then(mask, new_mask.into_bool()?);

            if target.is_empty() {
                break;
            }
        }

        Ok(mask.into_array())
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

    /// Re-order the expression so the sub-expressions estimated to be the "cheapest" are first (to the left of the expression)
    pub fn reorder(mut self, schema: &Schema) -> RowFilter {
        // Sort in ascending order of cost
        self.conjunction
            .sort_by_key(|e| Reverse(e.estimate_cost(schema)));

        self
    }
}

fn bool_array_and_then(current: BoolArray, next: BoolArray) -> BoolArray {
    assert!(current.len() >= next.len());

    let current = current.boolean_buffer();
    let next = next.boolean_buffer();

    let mut output = BooleanBufferBuilder::new(current.len());
    let mut next_iter = next.iter();

    for c in current.iter() {
        if c {
            output.append(next_iter.next().vortex_expect("Must have a value here"));
        } else {
            output.append(false);
        }
    }

    assert!(next_iter.next().is_none());
    assert_eq!(output.len(), current.len());

    BoolArray::from(output.finish())
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
