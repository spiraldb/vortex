// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_scan::planning::morsel::Morsel;
use vortex_scan::planning::morsel::MorselOutput;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;

use crate::scan::planning::ScanPlans;
use crate::scan::planning::SelectedRows;
use crate::scan::planning::graph::GraphStep;
use crate::scan::planning::graph::Piece;
use crate::scan::planning::graph::ProtocolGraph;
use crate::scan::planning::graph::ScanGraph;
use crate::scan::v2::splits::projection_splits;

/// Turns a split's selected rows into the morsels that project them.
///
/// The rows are cut into projection splits where the projection's chunks start, and each split
/// with a selected row becomes one [`ProjectionMorsel`], so a morsel reads at most one chunk of
/// every column. The planner finishes once it has handed out every morsel.
pub struct ProjectionPlanner {
    plans: Option<ScanPlans>,
    /// The split's selected rows, until the first compute cuts them.
    selected: Option<SelectedRows>,
    /// The projection splits not yet handed out, last first.
    pending: Vec<SelectedRows>,
}

impl ProjectionPlanner {
    /// Creates a planner for `selected`.
    pub fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        Self {
            plans: Some(plans),
            selected: Some(selected),
            pending: Vec::new(),
        }
    }

    /// The projection splits of `selected` that have a selected row, last first.
    fn cut(&self, selected: SelectedRows) -> Vec<SelectedRows> {
        let SelectedRows { scope, mask } = selected;
        let start = scope.rows.start;
        let index = |row: u64| usize::try_from(row - start).vortex_expect("split row fits usize");
        let plans = self.plans.as_ref().vortex_expect("uncut planner has plans");
        let mut splits = projection_splits(&plans.projection_starts, scope.rows.clone())
            .into_iter()
            .filter_map(|rows| {
                let mask = mask.slice(index(rows.start)..index(rows.end));
                (!mask.all_false()).then(|| SelectedRows {
                    scope: WorkScope {
                        file_ordinal: scope.file_ordinal,
                        rows,
                    },
                    mask,
                })
            })
            .collect::<Vec<_>>();
        splits.reverse();
        splits
    }
}

impl IoConsumer for ProjectionPlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for ProjectionPlanner {
    fn state(&self) -> State {
        if self.selected.is_none() && self.pending.is_empty() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if let Some(selected) = self.selected.take() {
            self.pending = self.cut(selected);
        }
        // No projection split has a selected row.
        let Some(selected) = self.pending.pop() else {
            return Ok(PlannerOutput::Done);
        };
        let scope = selected.scope.clone();
        // The driver's continuation follows its children. Give the last morsel ownership so
        // the completed planner does not keep the lowering tree alive while that morsel runs.
        let plans = if self.pending.is_empty() {
            self.plans.take().vortex_expect("planner has plans")
        } else {
            self.plans
                .as_ref()
                .vortex_expect("planner has plans")
                .clone()
        };
        Ok(PlannerOutput::Morsel(
            scope,
            Box::new(ProjectionMorsel::new(plans, selected)),
        ))
    }
}

/// Runs the projection plan over a split's selected rows and returns them as one batch, in row
/// order. A selection that projects to no rows finishes without a batch.
pub struct ProjectionMorsel {
    plans: Option<ScanPlans>,
    dtype: DType,
    selected: SelectedRows,
    graph: Option<ProtocolGraph>,
    pieces: Vec<Piece>,
    done: bool,
}

impl ProjectionMorsel {
    /// Creates a morsel projecting `selected`.
    pub fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        Self {
            dtype: plans.projection.dtype().clone(),
            plans: Some(plans),
            selected,
            graph: None,
            pieces: Vec::new(),
            done: false,
        }
    }

    fn finish(&mut self) -> VortexResult<MorselOutput> {
        self.done = true;
        self.pieces.sort_by_key(|piece| piece.rows.start);
        let mut arrays: Vec<_> = std::mem::take(&mut self.pieces)
            .into_iter()
            .map(|piece| piece.array)
            .filter(|array| !array.is_empty())
            .collect();
        let array = match arrays.len() {
            0 => return Ok(MorselOutput::Done),
            1 => arrays.remove(0),
            _ => ChunkedArray::try_new(arrays, self.dtype.clone())?.into_array(),
        };
        Ok(MorselOutput::Batch(array))
    }
}

impl IoConsumer for ProjectionMorsel {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some(graph) = self.graph.as_mut() {
            graph.set_io_result(request, result);
        }
    }
}

impl Morsel for ProjectionMorsel {
    fn state(&self) -> State {
        match &self.graph {
            _ if self.done => State::Done,
            None => State::NeedsCompute,
            Some(graph) => match graph.state() {
                State::Done => State::NeedsCompute,
                state => state,
            },
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        if self.done {
            vortex_bail!("ProjectionMorsel: compute called after Done");
        }
        let Some(graph) = self.graph.as_mut() else {
            let plans = self
                .plans
                .take()
                .vortex_expect("an uncompiled projection still has its plans");
            let graph = ScanGraph::try_new(
                plans.session,
                &plans.projection,
                self.selected.scope.rows.clone(),
                self.selected.mask.clone(),
                plans.row_offset,
                plans.values,
            )?;
            self.graph = Some(ProtocolGraph::new(graph, plans.locations, 0));
            return Ok(MorselOutput::Continue);
        };
        if graph.state() == State::Done {
            return self.finish();
        }
        Ok(match graph.compute()? {
            GraphStep::Yield => MorselOutput::Continue,
            GraphStep::NeedsIO(batch) => MorselOutput::NeedsIO(batch),
            GraphStep::Piece(piece) => {
                self.pieces.push(piece);
                MorselOutput::Continue
            }
        })
    }
}
