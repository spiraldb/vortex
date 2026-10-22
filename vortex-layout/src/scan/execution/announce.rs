// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Announces every segment a scan of a file will read, once per file.

use std::sync::Arc;

use rustc_hash::FxHashSet;
use vortex_error::VortexResult;
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

use super::PreparedScan;
use super::filter::hint;
use super::filter::segments_read;

/// What the file announcer continues into.
pub type AnnounceNext = Box<dyn FnOnce() -> Box<dyn Planner> + Send>;

/// Announces, once per file and filter and projection, every segment the scan's partitions
/// will read, so a coalescing source folds the reads of one partition with its neighbours'
/// whatever order the partitions start in. Announcing starts no read. Then emits the planner it
/// was built to continue into. A later partition of the file finds the file announced and emits
/// its next planner at once.
pub struct FileAnnouncePlanner {
    scan: Arc<PreparedScan>,
    next: Option<AnnounceNext>,
    announced: bool,
    /// Ids for hints, counting down from `u32::MAX`.
    next_hint: u32,
}

impl FileAnnouncePlanner {
    /// The planner announcing `scan`'s file, continuing into `next`.
    pub fn new(scan: Arc<PreparedScan>, next: AnnounceNext) -> Self {
        Self {
            scan,
            next: Some(next),
            announced: false,
            next_hint: u32::MAX,
        }
    }
}

impl IoConsumer for FileAnnouncePlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for FileAnnouncePlanner {
    fn state(&self) -> State {
        if self.next.is_none() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if !self.announced {
            self.announced = true;
            let scan = &self.scan;
            if scan
                .cache
                .announce_once(&scan.projection, scan.filter.as_ref())
            {
                let rows = 0..scan.projection.row_count();
                let len = usize::try_from(rows.end).unwrap_or(usize::MAX);
                let mask = Mask::new_true(len);
                let plans = scan
                    .filter
                    .iter()
                    .flat_map(|filter| filter.predicates.iter())
                    .chain(std::iter::once(&scan.projection));
                let segments =
                    segments_read(&scan.session, plans, rows.clone(), &mask, &scan.values)?;
                let mut batch = Vec::new();
                let mut seen = FxHashSet::default();
                hint(
                    &scan.locations,
                    &mut self.next_hint,
                    IoIntent::Announce,
                    &segments,
                    &mut seen,
                    &mut batch,
                );
                if !batch.is_empty() {
                    scan.metrics.file_announced_requests.add(batch.len() as u64);
                    tracing::debug!(
                        row_end = rows.end,
                        requests = batch.len(),
                        "v2 file announced"
                    );
                    return Ok(PlannerOutput::NeedsIO(batch));
                }
            }
        }
        let next = self
            .next
            .take()
            .ok_or_else(|| vortex_err!("the file announcer has nothing to continue into"))?;
        Ok(PlannerOutput::Planner(
            WorkScope {
                file_ordinal: 0,
                rows: self.scan.rows.clone(),
            },
            next(),
        ))
    }
}
