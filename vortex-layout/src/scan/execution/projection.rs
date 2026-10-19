// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt;
use futures::channel::oneshot;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_scan::planning::morsel::Morsel;
use vortex_scan::planning::morsel::MorselOutput;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;

use crate::plan::physical::pipeline::SelectedPlan;
use crate::plan::physical::pipeline::Split;
use crate::scan::execution::ScanPlans;
use crate::scan::execution::SelectedRows;
use crate::scan::execution::adaptor::AdaptorStep;
use crate::scan::execution::adaptor::Piece;
use crate::scan::execution::adaptor::ScanAdaptor;
use crate::scan::execution::children::Children;
use crate::scan::execution::children::splits::parallel_splits;

/// Runs the projection plan over a split's selected rows and returns them as one batch, in row
/// order. A selection that projects to no rows finishes without a batch.
pub(super) struct ProjectionPlanner {
    plans: Option<ScanPlans>,
    dtype: DType,
    scope: WorkScope,
    view: Option<SelectedPlan>,
    adaptor: Option<ScanAdaptor>,
    pieces: Vec<Piece>,
    done: bool,
    children: Option<Arc<Children>>,
    pending: Option<(u64, oneshot::Receiver<Vec<ArrayRef>>)>,
}

impl ProjectionPlanner {
    /// Creates a planner projecting `selected`.
    pub(super) fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        let view = SelectedPlan::new(
            plans.projection.clone(),
            vec![Split {
                rows: selected.scope.rows.clone(),
                mask: selected.mask,
            }],
            1,
        );
        Self::from_view(plans, view, selected.scope)
    }

    pub(super) fn from_view(plans: ScanPlans, view: SelectedPlan, scope: WorkScope) -> Self {
        Self {
            dtype: plans.projection.dtype().clone(),
            children: plans.children.clone(),
            pending: None,
            plans: Some(plans),
            scope,
            view: Some(view),
            adaptor: None,
            pieces: Vec::new(),
            done: false,
        }
    }

    fn finish(&mut self) -> VortexResult<PlannerOutput> {
        self.done = true;
        self.pieces
            .sort_by_key(|piece| (piece.split, piece.rows.start));
        let mut arrays: Vec<_> = std::mem::take(&mut self.pieces)
            .into_iter()
            .map(|piece| piece.array)
            .filter(|array| !array.is_empty())
            .collect();
        let array = match arrays.len() {
            0 => return Ok(PlannerOutput::Done),
            1 => arrays.remove(0),
            _ => ChunkedArray::try_new(arrays, self.dtype.clone())?.into_array(),
        };
        Ok(PlannerOutput::Morsel(
            self.scope.clone(),
            Box::new(ProjectedBatch(Some(array))),
        ))
    }
}

impl IoConsumer for ProjectionPlanner {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some(adaptor) = self.adaptor.as_mut() {
            adaptor.set_io_result(request, result);
        }
    }
}

impl Planner for ProjectionPlanner {
    fn state(&self) -> State {
        match &self.adaptor {
            _ if self.done => State::Done,
            _ if self.pending.is_some() => State::NeedsCompute,
            None => State::NeedsCompute,
            Some(adaptor) => match adaptor.state() {
                State::Done => State::NeedsCompute,
                state => state,
            },
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if self.done {
            vortex_bail!("ProjectionPlanner: compute called after Done");
        }
        if let Some((id, mut result)) = self.pending.take() {
            let arrays = result
                .try_recv()
                .map_err(|_| vortex_err!("Child projection was cancelled"))?
                .ok_or_else(|| vortex_err!("Child projection resumed before completion"))?;
            self.adaptor
                .as_mut()
                .ok_or_else(|| vortex_err!("Child completed before projection started"))?
                .complete_child(id, Some(arrays))?;
        }
        let Some(adaptor) = self.adaptor.as_mut() else {
            let plans = self
                .plans
                .take()
                .vortex_expect("an uncompiled projection still has its plans");
            let view = self
                .view
                .take()
                .vortex_expect("an uncompiled projection has a view");
            let scan = view
                .into_scan(plans.session, plans.values)?
                .with_row_offset(plans.row_offset)
                .with_child_planning(self.children.is_some());
            self.adaptor = Some(ScanAdaptor::new(scan, plans.locations));
            return Ok(PlannerOutput::Continue);
        };
        if adaptor.state() == State::Done {
            return self.finish();
        }
        Ok(match adaptor.compute()? {
            AdaptorStep::Yield => PlannerOutput::Continue,
            AdaptorStep::NeedsIO(batch) => PlannerOutput::NeedsIO(batch),
            AdaptorStep::Child(child) => {
                let children = Arc::clone(
                    self.children
                        .as_ref()
                        .ok_or_else(|| vortex_err!("Child projection has no scheduler"))?,
                );
                let work = if children.can_parallelize() {
                    parallel_splits(&child.view.plan, &child.view.splits)?
                } else {
                    None
                };
                if work.is_none() && !child.planner_owned {
                    adaptor.complete_child(child.id, None)?;
                    return Ok(PlannerOutput::Continue);
                }
                let (send, recv) = oneshot::channel();
                self.pending = Some((child.id, recv));
                PlannerOutput::Await(
                    async move {
                        let arrays = match work {
                            Some(work) => {
                                children.execute(child.view.plan, work, child.values).await?
                            }
                            None => children.execute_view(child.view, child.values).await?,
                        };
                        send.send(arrays)
                            .map_err(|_| vortex_err!("Projection planner was cancelled"))
                    }
                    .boxed(),
                )
            }
            AdaptorStep::Piece(piece) => {
                self.pieces.push(piece);
                PlannerOutput::Continue
            }
        })
    }
}

/// Planning resolves the projection; this morsel only publishes the assembled batch.
struct ProjectedBatch(Option<ArrayRef>);

impl IoConsumer for ProjectedBatch {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Morsel for ProjectedBatch {
    fn state(&self) -> State {
        if self.0.is_some() {
            State::NeedsCompute
        } else {
            State::Done
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        Ok(match self.0.take() {
            Some(array) => MorselOutput::Batch(array),
            None => MorselOutput::Done,
        })
    }
}
