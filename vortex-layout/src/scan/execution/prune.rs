// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Proves a filter's zone maps once over a scan's rows, before any chunk is planned.

use std::ops::Range;
use std::sync::Arc;
use std::vec::IntoIter;

use rustc_hash::FxHashSet;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;
use vortex_session::VortexSession;

use super::PreparedScan;
use super::filter::MaskScan;
use super::filter::MaskStep;
use super::filter::hint;
use super::filter::segments_read;
use crate::plan::physical::PlanRef;
use crate::plan::physical::Zoned;
use crate::plan::physical::pipeline::Split;

/// Whether every zone proof in `plan` has already been proved, so compiling it reads no zones.
fn is_proved(plan: &PlanRef, session: &VortexSession) -> VortexResult<bool> {
    if let Some(zoned) = plan.as_opt::<Zoned>()
        && zoned.is_pruning()
    {
        return zoned.is_proved(session);
    }
    for child in plan.children().iter() {
        if !is_proved(&child?, session)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The planner a prune planner continues into, given the rows the proofs kept.
pub(super) type Next = Box<dyn FnOnce(Mask) -> Box<dyn Planner> + Send>;

/// Runs the filter's zone-map proofs over the scan's rows and emits the planner it was built
/// to continue into, with the rows no proof excludes. The zone maps are the only reads,
/// started together. Rows every proof excludes end the scan here: nothing is emitted.
pub(super) struct PrunePlanner {
    scan: Arc<PreparedScan>,
    rows: Range<u64>,
    /// What to emit once every proof has run, taken when it is.
    next: Option<Next>,
    /// The rows no proof has excluded yet, over `rows`.
    mask: Mask,
    proofs: IntoIter<PlanRef>,
    active: Option<MaskScan>,
    announced: bool,
    /// Ids for hints, counting down from `u32::MAX` so they never meet an adaptor's fetch ids.
    next_hint: u32,
    done: bool,
}

impl PrunePlanner {
    pub(super) fn new(scan: Arc<PreparedScan>, rows: Range<u64>, next: Next) -> Self {
        let proofs: Vec<_> = scan
            .filter
            .iter()
            .flat_map(|filter| filter.proofs().cloned())
            .collect();
        let len = usize::try_from(rows.end - rows.start).unwrap_or(usize::MAX);
        Self {
            scan,
            rows,
            next: Some(next),
            mask: Mask::new_true(len),
            proofs: proofs.into_iter(),
            active: None,
            announced: false,
            next_hint: u32::MAX,
            done: false,
        }
    }
}

impl IoConsumer for PrunePlanner {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some(proof) = &mut self.active {
            proof.set_io_result(request, result);
        }
    }
}

impl Planner for PrunePlanner {
    fn state(&self) -> State {
        if self.done {
            return State::Done;
        }
        match &self.active {
            Some(proof) => proof.state(),
            None => State::NeedsCompute,
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if self.done {
            return Ok(PlannerOutput::Done);
        }
        if !self.announced {
            self.announced = true;
            // A proof whose zones an earlier scan of the file proved reads nothing: a shared
            // plan keeps its proved zones, so only unproved proofs need their zone maps.
            let mut unproved = Vec::new();
            for proof in self.proofs.as_slice() {
                if !is_proved(proof, &self.scan.session)? {
                    unproved.push(proof);
                }
            }
            let segments = segments_read(
                &self.scan.session,
                unproved.into_iter(),
                self.rows.clone(),
                &self.mask,
            )?;
            let mut batch = Vec::new();
            let mut seen = FxHashSet::default();
            hint(
                &self.scan.locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                &segments,
                &mut seen,
                &mut batch,
            );
            if !batch.is_empty() {
                return Ok(PlannerOutput::NeedsIO(batch));
            }
        }
        if let Some(proof) = &mut self.active {
            let excluded = match proof.compute(&self.scan.session)? {
                MaskStep::Running(output) => return Ok(output),
                MaskStep::Finished(excluded) => excluded,
            };
            let input = self.mask.true_count();
            vortex_ensure_eq!(excluded.len(), input);
            self.mask = self.mask.intersect_by_rank(&!excluded);
            tracing::debug!(
                row_start = self.rows.start,
                row_end = self.rows.end,
                input_rows = input,
                output_rows = self.mask.true_count(),
                "v2 zone proof evaluated"
            );
            self.active = None;
        }
        if self.mask.all_false() {
            self.done = true;
            return Ok(PlannerOutput::Done);
        }
        if let Some(proof) = self.proofs.next() {
            self.active = Some(MaskScan::new(
                &self.scan,
                proof,
                Split {
                    rows: self.rows.clone(),
                    mask: self.mask.clone(),
                },
            )?);
            return Ok(PlannerOutput::Continue);
        }
        // Every proof has run: the planner this one continues into gets the kept rows.
        self.done = true;
        tracing::debug!(
            row_start = self.rows.start,
            row_end = self.rows.end,
            kept_rows = self.mask.true_count(),
            "v2 zone proofs finished"
        );
        let next = self
            .next
            .take()
            .ok_or_else(|| vortex_err!("the prune planner has nothing to continue into"))?;
        let kept = std::mem::replace(&mut self.mask, Mask::new_true(0));
        Ok(PlannerOutput::Planner(
            WorkScope {
                file_ordinal: 0,
                rows: self.rows.clone(),
            },
            next(kept),
        ))
    }
}
