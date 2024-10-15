use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_buffer::BooleanBuffer;
use vortex::array::BoolArray;
use vortex::compute::and;
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::field::Field;
use vortex_error::{VortexExpect, VortexResult};
use vortex_expr::{split_conjunction, VortexExpr};

use crate::layouts::null_as_false;
use crate::layouts::read::filter_project::filter_project;

#[derive(Debug, Clone)]
pub struct RowFilter {
    conjunction: Vec<Arc<dyn VortexExpr>>,
}

impl RowFilter {
    pub fn new(expr: Arc<dyn VortexExpr>) -> Self {
        let conjunction = split_conjunction(&expr);
        Self { conjunction }
    }

    pub(crate) fn from_conjunction(conjunction: Vec<Arc<dyn VortexExpr>>) -> Self {
        Self { conjunction }
    }

    /// Evaluate the underlying filter against a target array, returning a boolean mask
    pub fn evaluate(&self, target: &Array) -> VortexResult<Array> {
        let mut filter_iter = self.conjunction.iter();
        let mut mask = filter_iter
            .next()
            .vortex_expect("must have at least one predicate")
            .evaluate(target)?;
        for expr in filter_iter {
            if mask.statistics().compute_true_count().unwrap_or_default() == 0 {
                return BoolArray::try_new(
                    BooleanBuffer::new_unset(target.len()),
                    Validity::AllInvalid,
                )
                .map(IntoArray::into_array);
            }

            let new_mask = expr.evaluate(target)?;
            mask = and(new_mask, mask)?;
        }

        null_as_false(mask.into_bool()?)
    }

    /// Returns a set of all referenced fields in the underlying filter
    pub fn references(&self) -> HashSet<Field> {
        let mut set = HashSet::new();
        for expr in self.conjunction.iter() {
            set.extend(expr.references().iter().cloned());
        }

        set
    }

    pub fn project(&self, fields: &[Field]) -> Option<Self> {
        let conj = self
            .conjunction
            .iter()
            .filter_map(|c| filter_project(c, fields))
            .collect::<Vec<_>>();
        if conj.is_empty() {
            None
        } else {
            Some(Self::from_conjunction(conj))
        }
    }
}
