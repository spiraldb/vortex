// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_io::request::IoService;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::WorkScope;

/// One split of a scan, ready to be admitted to a planning run.
///
/// The split's planner projects the rows of `scope`. Each morsel emits its selected batches
/// in row order, and morsels run in whatever order their reads arrive.
pub(super) struct SplitPlan {
    /// The rows of the scan's root row domain the split covers.
    pub(super) scope: WorkScope,
    /// The split's first planner.
    pub(super) root: Box<dyn Planner>,
}

impl SplitPlan {
    /// Runs the split on a planning run of its own, reading through a session of `io`, and
    /// maps each batch as it arrives before returning the results in row order. Mapping here
    /// lets a decoder release the batch's compressed buffers while later batches still read.
    ///
    /// The run advances inside this future, on whichever thread polls it, and the future awaits
    /// its reads between steps. Nothing is handed to another thread.
    pub(super) async fn run<A>(
        self,
        io: Arc<dyn IoService>,
        map: impl Fn(ArrayRef) -> VortexResult<A>,
    ) -> VortexResult<Vec<A>> {
        let mut run = Run::new();
        run.admit(self.root, self.scope, io.session());
        let mut batches = Vec::new();
        loop {
            match run.advance()? {
                Progress::Batch(batch) => {
                    batches.push((batch.scope.rows.start, map(batch.array)?));
                }
                Progress::RootDone(_) | Progress::Idle => break,
                Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
            }
        }
        // Morsels finish in whatever order their reads arrive.
        batches.sort_by_key(|(start, _)| *start);
        Ok(batches.into_iter().map(|(_, batch)| batch).collect())
    }
}
