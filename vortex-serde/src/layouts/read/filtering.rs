use std::fmt::Debug;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use vortex::compute::and;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::Array;
use vortex_dtype::field::Field;
use vortex_error::{VortexExpect, VortexResult};
use vortex_expr::{split_conjunction, VortexExpr};

use crate::layouts::stats::PruningPredicate;

#[derive(Debug, Clone)]
pub struct RowFilter {
    conjunction: Vec<Arc<dyn VortexExpr>>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>) -> Self {
        let conjunction = split_conjunction(&filter);
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
            let new_mask = expr.evaluate(target)?;
            mask = and(new_mask, mask)?;

            if mask.statistics().compute_true_count().unwrap_or_default() == 0 {
                return Ok(mask);
            }
        }

        Ok(mask)
    }

    pub fn to_pruning_filter(&self) -> Option<(RowFilter, HashMap<Field, HashSet<Stat>>)> {
        let mut required_stats = HashMap::new();
        let conjunction: Vec<Arc<dyn VortexExpr>> = self
            .conjunction
            .iter()
            .filter_map(PruningPredicate::try_new)
            .map(|p| {
                p.required_stats().iter().for_each(|(k, stats)| {
                    required_stats
                        .entry(k.clone())
                        .or_insert_with(HashSet::new)
                        .extend(stats);
                });
                p.expr().clone()
            })
            .collect();

        if conjunction.is_empty() {
            None
        } else {
            Some((Self::from_conjunction(conjunction), required_stats))
        }
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
            .filter_map(|c| c.project(fields))
            .collect::<Vec<_>>();
        if conj.is_empty() {
            None
        } else {
            Some(Self::from_conjunction(conj))
        }
    }
}
