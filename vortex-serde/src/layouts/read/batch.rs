use std::mem;
use std::sync::Arc;

use ahash::HashMap;
use vortex::array::StructArray;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};
use vortex_dtype::field::Field;
use vortex_dtype::StructDType;
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};

use crate::layouts::read::selection::RowSelector;
use crate::layouts::read::{LayoutReader, ReadResult};
use crate::layouts::stats::stat_column_name;
use crate::layouts::{PlanResult, PruningScan};

#[derive(Debug)]
pub struct BatchReader {
    names: Arc<[Arc<str>]>,
    children: Vec<Box<dyn LayoutReader>>,
    arrays: Vec<Option<Array>>,
}

impl BatchReader {
    pub fn new(names: Arc<[Arc<str>]>, children: Vec<Box<dyn LayoutReader>>) -> Self {
        let arrays = vec![None; children.len()];
        Self {
            names,
            children,
            arrays,
        }
    }

    pub fn read_more(&mut self) -> VortexResult<Option<ReadResult>> {
        let mut messages = Vec::new();
        for (i, child_array) in self
            .arrays
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].read_next()? {
                Some(rr) => match rr {
                    ReadResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    ReadResult::Batch(a) => *child_array = Some(a),
                },
                None => {
                    debug_assert!(
                        self.arrays.iter().all(Option::is_none),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let child_arrays = mem::replace(&mut self.arrays, vec![None; self.children.len()])
                .into_iter()
                .enumerate()
                .map(|(i, a)| a.ok_or_else(|| vortex_err!("Missing child array at index {i}")))
                .collect::<VortexResult<Vec<_>>>()?;
            let len = child_arrays.first().map(|l| l.len()).unwrap_or(0);
            Ok(Some(ReadResult::Batch(
                StructArray::try_new(self.names.clone(), child_arrays, len, Validity::NonNullable)?
                    .into_array(),
            )))
        } else {
            Ok(Some(ReadResult::ReadMore(messages)))
        }
    }
}

#[derive(Debug)]
pub struct BatchPruner {
    children: Vec<Box<dyn LayoutReader>>,
    selectors: Vec<Option<RowSelector>>,
    projected_scans: Vec<(usize, PruningScan)>,
}

impl BatchPruner {
    pub fn try_new(
        dtype: &StructDType,
        children: Vec<Box<dyn LayoutReader>>,
        scan: PruningScan,
    ) -> VortexResult<Self> {
        let projected_scans = scan
            .stats_projection
            .iter()
            .map(|(f, stats)| {
                let child_idx = match f {
                    Field::Name(n) => dtype
                        .find_name(n.as_ref())
                        .ok_or_else(|| vortex_err!("Invalid projection, trying to select  {n}"))?,
                    Field::Index(i) => *i,
                };

                // TODO(robert): Once we support nested columns we can avoid generating new names here and just project on the field
                let stat_column_projection = stats.iter().map(|s| stat_column_name(f, *s)).collect::<Vec<_>>();
                let projected_filter = scan.filter.as_ref().and_then(|ff| ff.project(&stat_column_projection));

                // Right now we do not support evaluating multicolumn pruning expressions
                // To evaluate multicolumn expressions we need to get statistics from child tables, strip from the predicate
                // all subexpressions that have been already evaluated (by projecting into set of columns that fail this check), align
                // metadata tables (by converting row offsets into run ends)
                if scan.filter.is_some() && projected_filter.is_none() {
                    vortex_bail!("Couldn't project expression {:?} into field {f}, multicolumn expressions are not supported", scan.filter)
                }

                let stats_projection = HashMap::from_iter([(f.clone(), stats.clone())]);
                Ok((
                    child_idx,
                    PruningScan {
                        stats_projection,
                        filter: projected_filter,
                        row_count: scan.row_count,
                    },
                ))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let selectors = vec![None; projected_scans.len()];

        Ok(Self {
            children,
            selectors,
            projected_scans,
        })
    }

    pub fn plan_more(&mut self) -> VortexResult<Option<PlanResult>> {
        let mut messages = Vec::new();
        for (child_selector, (child_idx, pruning_scan)) in self
            .selectors
            .iter_mut()
            .zip(self.projected_scans.iter())
            .filter(|(s, _)| s.is_none())
        {
            match self.children[*child_idx].plan(pruning_scan.clone())? {
                Some(rr) => match rr {
                    PlanResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    PlanResult::Range(s) => *child_selector = Some(s),
                    PlanResult::Batch(_) => {
                        vortex_bail!("Batch results in planning are not supported yet")
                    }
                },
                None => {
                    debug_assert!(
                        self.selectors.iter().all(Option::is_none),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let child_selectors =
                mem::replace(&mut self.selectors, vec![None; self.projected_scans.len()])
                    .into_iter()
                    .enumerate()
                    .map(|(i, a)| a.ok_or_else(|| vortex_err!("Missing child array at index {i}")))
                    .collect::<VortexResult<Vec<_>>>()?;
            Ok(Some(PlanResult::Range(
                child_selectors
                    .into_iter()
                    .reduce(|a, b| a.intersect(&b))
                    .vortex_expect("Pruning on 0 columns"),
            )))
        } else {
            Ok(Some(PlanResult::ReadMore(messages)))
        }
    }
}
