// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

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
use super::plan_projection;

/// The default writer's row block size.
const ROW_BLOCK_SIZE: u64 = 8192;
const MAX_BLOCKS_PER_CHUNK: u64 = 8;

/// Aim for at least two tasks per CPU, keeping every boundary aligned to writer blocks.
fn chunk_size(row_count: u64, parallelism: usize) -> u64 {
    let blocks = row_count / parallelism.max(1) as u64 / 2 / ROW_BLOCK_SIZE;
    blocks.clamp(1, MAX_BLOCKS_PER_CHUNK) * ROW_BLOCK_SIZE
}

/// Owns the file's remaining rows and emits a child planner for each selected work range.
pub(in crate::scan) struct FilePlanner {
    plans: ScanPlans,
    remaining: Range<u64>,
    rows_per_chunk: u64,
    selection: Selection,
}

impl FilePlanner {
    pub(in crate::scan) fn new(
        plans: ScanPlans,
        rows: Range<u64>,
        selection: Selection,
        parallelism: usize,
    ) -> Self {
        Self {
            plans,
            rows_per_chunk: chunk_size(rows.end.saturating_sub(rows.start), parallelism),
            remaining: rows,
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
        let start = self.remaining.start;
        let end = (start / self.rows_per_chunk * self.rows_per_chunk)
            .saturating_add(self.rows_per_chunk)
            .min(self.remaining.end);
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::chunk_size;

    #[rstest]
    #[case::empty(0, 14, 8192)]
    #[case::small(1000, 14, 8192)]
    #[case::hashtags_local(511_511, 14, 16_384)]
    #[case::hashtags_ci(511_511, 94, 8192)]
    #[case::large(6_000_000, 14, 65_536)]
    #[case::overflow(u64::MAX, 94, 65_536)]
    #[case::no_parallelism(1000, 0, 8192)]
    fn work_size(#[case] rows: u64, #[case] cpus: usize, #[case] expected: u64) {
        assert_eq!(chunk_size(rows, cpus), expected);
    }
}
