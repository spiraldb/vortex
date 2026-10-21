// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::vec::IntoIter;

use vortex_array::ExecutionCtx;
use vortex_array::VortexSessionExecute;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::morsel::Morsel;
use vortex_scan::planning::morsel::MorselOutput;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;

use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::selection::selected;
use crate::scan::execution::ProjectedFilter;
use crate::scan::execution::ScanPlans;
use crate::scan::execution::SelectedRows;
use crate::scan::execution::adaptor::AdaptorStep;
use crate::scan::execution::adaptor::ScanAdaptor;
use crate::scan::execution::batches::Batcher;
use crate::scan::execution::batches::filter_projected;

const MAX_OUTPUT_ROWS: usize = 65_536;

/// Keeps sparse selections together and subdivides dense selections to avoid large copies when
/// converting chunked projection results to Arrow.
pub(super) struct ProjectionPlanner {
    plans: ScanPlans,
    selected: IntoIter<SelectedRows>,
}

impl ProjectionPlanner {
    /// Creates a planner for `selected`.
    pub(super) fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        let selected = if selected.mask.true_count() <= MAX_OUTPUT_ROWS {
            vec![selected]
        } else {
            (0..selected.mask.len())
                .step_by(MAX_OUTPUT_ROWS)
                .filter_map(|start| {
                    let end = (start + MAX_OUTPUT_ROWS).min(selected.mask.len());
                    let mask = selected.mask.slice(start..end);
                    if mask.all_false() {
                        return None;
                    }
                    Some(SelectedRows {
                        scope: WorkScope {
                            file_ordinal: selected.scope.file_ordinal,
                            rows: selected.scope.rows.start + start as u64
                                ..selected.scope.rows.start + end as u64,
                        },
                        mask,
                    })
                })
                .collect()
        };
        Self {
            plans,
            selected: selected.into_iter(),
        }
    }
}

impl IoConsumer for ProjectionPlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for ProjectionPlanner {
    fn state(&self) -> State {
        if !self.selected.as_slice().is_empty() {
            State::NeedsCompute
        } else {
            State::Done
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        let Some(selected) = self.selected.next() else {
            return Ok(PlannerOutput::Done);
        };
        if selected.mask.all_false() {
            return Ok(PlannerOutput::Done);
        }
        Ok(PlannerOutput::Morsel(
            selected.scope.clone(),
            Box::new(ProjectionMorsel::new(self.plans.clone(), selected)),
        ))
    }
}

/// Runs the projection plan over a split's selected rows and returns them in row order, in
/// batches a [`Batcher`] cuts. A selection that projects to no rows finishes without a batch.
struct ProjectionMorsel {
    plans: Option<ScanPlans>,
    filters: Arc<[Arc<dyn ProjectedFilter>]>,
    selected: SelectedRows,
    adaptor: Option<ScanAdaptor>,
    batches: Batcher,
    ctx: ExecutionCtx,
    done: bool,
}

impl ProjectionMorsel {
    /// Creates a morsel projecting `selected`.
    fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        Self {
            batches: Batcher::new(plans.projection.dtype().clone()),
            ctx: plans.session.create_execution_ctx(),
            filters: Arc::clone(&plans.projected_filters),
            plans: Some(plans),
            selected,
            adaptor: None,
            done: false,
        }
    }

    fn finish(&mut self) -> VortexResult<MorselOutput> {
        self.batches.flush()?;
        Ok(match self.batches.pop() {
            Some(array) => MorselOutput::Batch(array),
            None => {
                self.done = true;
                MorselOutput::Done
            }
        })
    }
}

impl IoConsumer for ProjectionMorsel {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some(adaptor) = self.adaptor.as_mut() {
            adaptor.set_io_result(request, result);
        }
    }
}

impl Morsel for ProjectionMorsel {
    fn state(&self) -> State {
        match &self.adaptor {
            _ if self.done => State::Done,
            _ if self.batches.has_ready() => State::NeedsCompute,
            None => State::NeedsCompute,
            Some(adaptor) => match adaptor.state() {
                State::Done => State::NeedsCompute,
                state => state,
            },
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        if self.done {
            vortex_bail!("ProjectionMorsel: compute called after Done");
        }
        let Some(adaptor) = self.adaptor.as_mut() else {
            let plans = self
                .plans
                .take()
                .vortex_expect("an uncompiled projection still has its plans");
            let scan = Scan::try_new(
                plans.session,
                selected(
                    plans.projection,
                    self.selected.scope.rows.clone(),
                    self.selected.mask.clone(),
                ),
                vec![Split {
                    rows: self.selected.scope.rows.clone(),
                    mask: Mask::new_true(self.selected.mask.len()),
                }],
            )?
            .with_row_offset(plans.row_offset)
            .with_value_cache(plans.values);
            self.adaptor = Some(ScanAdaptor::new(scan, plans.locations));
            return Ok(MorselOutput::Continue);
        };
        if let Some(array) = self.batches.pop() {
            return Ok(MorselOutput::Batch(array));
        }
        if adaptor.state() == State::Done {
            return self.finish();
        }
        Ok(match adaptor.compute()? {
            AdaptorStep::Yield => MorselOutput::Continue,
            AdaptorStep::NeedsIO(batch) => MorselOutput::NeedsIO(batch),
            AdaptorStep::Piece(piece) => {
                let piece = if self.filters.is_empty() {
                    Some(piece)
                } else {
                    filter_projected(piece, &self.filters, &mut self.ctx)?
                };
                // A scan of one split produces its pieces in row order.
                if let Some(piece) = piece {
                    self.batches.push(piece, &mut self.ctx)?;
                }
                match self.batches.pop() {
                    Some(array) => MorselOutput::Batch(array),
                    None => MorselOutput::Continue,
                }
            }
        })
    }
}
