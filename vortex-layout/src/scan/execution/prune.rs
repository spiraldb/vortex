// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Zone pruning for a scan's rows, run once before any split is planned.

use std::ops::Range;
use std::sync::Arc;
use std::vec::IntoIter;

use parking_lot::Mutex;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;

use super::ScanPlans;
use super::adaptor::AdaptorStep;
use super::adaptor::ScanAdaptor;
use super::filter::hint;
use super::filter::piece_mask;
use super::filter::segments_read;
use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;

/// Proves every pruning proof over a scan's rows and leaves the rows no proof excluded in
/// `kept`, as a mask over those rows.
///
/// Proofs read only zone maps, started together on the first compute. Each proof runs over
/// the rows the earlier proofs kept, so a scan whose zones all fail its first proof reads
/// nothing more.
pub(in crate::scan) struct PrunePlanner {
    plans: ScanPlans,
    rows: Range<u64>,
    mask: Mask,
    proofs: IntoIter<PlanRef>,
    active: Option<ScanAdaptor>,
    pieces: Vec<Mask>,
    announced: bool,
    next_hint: u32,
    kept: Arc<Mutex<Option<Mask>>>,
    done: bool,
}

impl PrunePlanner {
    pub(in crate::scan) fn new(
        plans: ScanPlans,
        rows: Range<u64>,
        proofs: Vec<PlanRef>,
        kept: Arc<Mutex<Option<Mask>>>,
    ) -> Self {
        let len = usize::try_from(rows.end - rows.start).unwrap_or(usize::MAX);
        Self {
            plans,
            rows,
            mask: Mask::new_true(len),
            proofs: proofs.into_iter(),
            active: None,
            pieces: Vec::new(),
            announced: false,
            next_hint: u32::MAX,
            kept,
            done: false,
        }
    }

    fn finish(&mut self) -> PlannerOutput {
        self.done = true;
        *self.kept.lock() = Some(std::mem::replace(&mut self.mask, Mask::new_true(0)));
        PlannerOutput::Done
    }
}

impl IoConsumer for PrunePlanner {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some(adaptor) = &mut self.active {
            adaptor.set_io_result(request, result);
        }
    }
}

impl Planner for PrunePlanner {
    fn state(&self) -> State {
        if self.done {
            return State::Done;
        }
        match &self.active {
            Some(adaptor) if adaptor.state() != State::Done => adaptor.state(),
            _ => State::NeedsCompute,
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if self.done {
            return Ok(PlannerOutput::Done);
        }
        if !self.announced {
            self.announced = true;
            let proofs = segments_read(self.proofs.as_slice().iter(), self.rows.clone(), None)?;
            let mut batch = Vec::new();
            hint(
                &self.plans.locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                &proofs,
                &mut batch,
            );
            if !batch.is_empty() {
                return Ok(PlannerOutput::NeedsIO(batch));
            }
        }
        if let Some(adaptor) = &mut self.active {
            if adaptor.state() != State::Done {
                return Ok(match adaptor.compute()? {
                    AdaptorStep::Yield => PlannerOutput::Continue,
                    AdaptorStep::NeedsIO(batch) => PlannerOutput::NeedsIO(batch),
                    AdaptorStep::Piece(piece) => {
                        self.pieces.push(piece_mask(&self.plans.session, piece)?);
                        PlannerOutput::Continue
                    }
                });
            }
            let input = self.mask.true_count();
            let excluded = Mask::concat(self.pieces.iter())?;
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
            self.pieces.clear();
        }
        if self.mask.all_false() {
            return Ok(self.finish());
        }
        let Some(proof) = self.proofs.next() else {
            return Ok(self.finish());
        };
        let scan = Scan::try_new(
            self.plans.session.clone(),
            proof,
            vec![Split {
                rows: self.rows.clone(),
                mask: self.mask.clone(),
            }],
        )?
        .with_row_offset(self.plans.row_offset)
        .with_value_cache(self.plans.values.clone());
        self.active = Some(ScanAdaptor::new(scan, Arc::clone(&self.plans.locations)));
        Ok(PlannerOutput::Continue)
    }
}
