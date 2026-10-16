// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::vec;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;

use super::ScanPlans;
use super::boundaries::work_boundaries;
use super::plan_projection;

/// Owns the file's remaining rows and emits a child planner for each selected work range.
pub(in crate::scan) struct FilePlanner {
    plans: ScanPlans,
    remaining: Range<u64>,
    boundaries: Option<vec::IntoIter<u64>>,
    selection: Selection,
}

impl FilePlanner {
    pub(in crate::scan) fn new(plans: ScanPlans, rows: Range<u64>, selection: Selection) -> Self {
        Self {
            plans,
            remaining: rows,
            boundaries: None,
            selection,
        }
    }
}

impl IoConsumer for FilePlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for FilePlanner {
    fn state(&self) -> State {
        if self.remaining.is_empty() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if self.remaining.is_empty() {
            return Ok(PlannerOutput::Done);
        }
        if self.boundaries.is_none() {
            let mut boundaries =
                work_boundaries(&self.plans.projection, self.remaining.clone())?.into_iter();
            boundaries.next();
            self.boundaries = Some(boundaries);
        }
        let start = self.remaining.start;
        let end = self
            .boundaries
            .as_mut()
            .and_then(Iterator::next)
            .vortex_expect("unfinished file has another work boundary");
        self.remaining.start = end;
        let selected = self.selection.row_mask(&(start..end));
        if selected.mask().all_false() {
            return Ok(PlannerOutput::Continue);
        }
        let scope = WorkScope {
            file_ordinal: 0,
            rows: selected.row_range(),
        };
        Ok(PlannerOutput::Planner(
            scope.clone(),
            plan_projection(self.plans.clone(), scope, selected.mask().clone()),
        ))
    }
}
