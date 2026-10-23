// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Evaluates a filter's pruning plans once over a scan's rows, before any chunk is planned.

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use rustc_hash::FxHashSet;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_io::request::IoBatch;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;

use super::PreparedScan;
use super::adaptor::AdaptorStep;
use super::adaptor::ScanAdaptor;
use super::chunk::row_chunks;
use super::filter::hint;
use super::filter::piece_mask;
use super::filter::segments_read;
use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;

/// The planner a prune planner continues into, given the rows pruning kept, or `None` when
/// no pruning plan ran.
pub type PruneNext = Box<dyn FnOnce(Option<Mask>) -> Box<dyn Planner> + Send>;

/// Request ids of the adaptor of pruning plan `i` start at `i << IDS_PER_PLAN`, far below the
/// hint ids counting down from `u32::MAX`.
const IDS_PER_PLAN: u32 = 24;

/// Evaluates the filter's pruning plans over the scan's rows, all at once, and emits the planner
/// it was built to continue into, with the rows no plan pruned. The zone maps are the only
/// reads, published together so the planner waits once for all of them. Rows every plan prunes
/// end the scan here: nothing is emitted. A filter without pruning plans emits the next planner
/// at once.
pub struct PrunePlanner {
    scan: Arc<PreparedScan>,
    rows: Range<u64>,
    /// What to emit once every pruning plan has run, taken when it is.
    next: Option<PruneNext>,
    pruning: Vec<PlanRef>,
    /// One adaptor per pruning plan, each over the whole of `rows`, with the pieces it produced.
    active: Vec<(ScanAdaptor, Vec<Mask>)>,
    started: Option<Instant>,
    /// Ids for hints, counting down from `u32::MAX` so they never meet an adaptor's fetch ids.
    next_hint: u32,
    /// The chunks of `rows`, whose reads are speculated on the first ask.
    ahead: Vec<Range<u64>>,
    speculated: bool,
    done: bool,
}

impl PrunePlanner {
    /// The planner of `scan`'s filter's pruning plans over `rows`, continuing into `next`.
    pub fn new(scan: Arc<PreparedScan>, rows: Range<u64>, next: PruneNext) -> Self {
        let pruning: Vec<_> = scan
            .filter
            .iter()
            .flat_map(|filter| filter.pruning().cloned())
            .collect();
        let ahead = row_chunks(rows.clone(), scan.chunk_rows)
            .into_iter()
            .take(scan.chunks_read_ahead)
            .collect();
        Self {
            scan,
            rows,
            next: Some(next),
            pruning,
            active: Vec::new(),
            started: None,
            next_hint: u32::MAX,
            ahead,
            speculated: false,
            done: false,
        }
    }

    fn len(&self) -> usize {
        usize::try_from(self.rows.end - self.rows.start).unwrap_or(usize::MAX)
    }

    /// Builds every pruning plan's adaptor over the whole of `rows`.
    fn start(&mut self) -> VortexResult<()> {
        let mask = Mask::new_true(self.len());
        for (index, plan) in self.pruning.drain(..).enumerate() {
            let scan = Scan::try_new(
                self.scan.session.clone(),
                plan,
                vec![Split {
                    rows: self.rows.clone(),
                    mask: mask.clone(),
                }],
            )?
            .with_row_offset(self.scan.row_offset)
            .with_value_cache(self.scan.values.clone());
            let first_id = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_shl(IDS_PER_PLAN))
                .ok_or_else(|| vortex_err!("too many pruning plans to number their requests"))?;
            let adaptor =
                ScanAdaptor::numbered_from(scan, Arc::clone(&self.scan.locations), first_id)
                    .urgent();
            self.active.push((adaptor, Vec::new()));
        }
        Ok(())
    }

    /// The rows no plan pruned, once every adaptor is done.
    fn kept(&mut self) -> VortexResult<Mask> {
        let mut kept = Mask::new_true(self.len());
        for (_, pieces) in self.active.drain(..) {
            let excluded = Mask::concat(pieces.iter())?;
            vortex_ensure_eq!(excluded.len(), kept.len());
            kept = &kept & &!excluded;
        }
        Ok(kept)
    }
}

impl IoConsumer for PrunePlanner {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        let index = (request.0 >> IDS_PER_PLAN) as usize;
        if let Some((adaptor, _)) = self.active.get_mut(index) {
            adaptor.set_io_result(request, result);
        }
    }
}

impl Planner for PrunePlanner {
    /// While the zone maps are read, every chunk's predicate columns and projection columns are
    /// read too, the predicates first in the batch, registered together so the partition's reads
    /// coalesce as one scan's. The projection is read, not only announced: in a file whose
    /// columns are written in bursts a wide projection column shares no read with the narrow
    /// predicate beside it, and a read only announced is not started until the projection
    /// fetches it, after the filter, with nothing to overlap. One ask reads it all. A scan
    /// selecting rows by index reads nothing it was not asked for.
    fn speculate(&mut self) -> VortexResult<Option<IoBatch>> {
        if !matches!(self.scan.selection, Selection::All) || self.speculated {
            return Ok(None);
        }
        let Some(filter) = &self.scan.filter else {
            return Ok(None);
        };
        self.speculated = true;
        let session = &self.scan.session;
        let locations = &self.scan.locations;
        let mut batch = Vec::new();
        let mut seen = FxHashSet::default();
        let mut projections = Vec::new();
        for chunk in &self.ahead {
            let len = usize::try_from(chunk.end - chunk.start).unwrap_or(usize::MAX);
            let mask = Mask::new_true(len);
            let predicates = segments_read(
                session,
                filter.predicates.iter(),
                chunk.clone(),
                &mask,
                &self.scan.values,
            )?;
            hint(
                locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                &predicates,
                &mut seen,
                &mut batch,
            );
            projections.push(segments_read(
                session,
                std::iter::once(&self.scan.projection),
                chunk.clone(),
                &mask,
                &self.scan.values,
            )?);
        }
        for projection in &projections {
            hint(
                locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                projection,
                &mut seen,
                &mut batch,
            );
        }
        if batch.is_empty() {
            return Ok(None);
        }
        self.scan
            .metrics
            .speculated_requests
            .add(batch.len() as u64);
        tracing::debug!(
            row_start = self.rows.start,
            row_end = self.rows.end,
            segments = batch.len(),
            "v2 chunk reads speculated"
        );
        Ok(Some(batch))
    }

    fn state(&self) -> State {
        if self.done {
            return State::Done;
        }
        if self.started.is_none() {
            return State::NeedsCompute;
        }
        let mut waiting = false;
        for (adaptor, _) in &self.active {
            match adaptor.state() {
                State::NeedsCompute => return State::NeedsCompute,
                State::Waiting => waiting = true,
                State::Done => {}
            }
        }
        if waiting {
            State::Waiting
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if self.done {
            return Ok(PlannerOutput::Done);
        }
        if self.started.is_none() {
            self.started = Some(Instant::now());
            tracing::debug!(
                row_start = self.rows.start,
                row_end = self.rows.end,
                "v2 pruning started"
            );
            self.start()?;
        }
        // Step every adaptor that can compute, publishing all their reads as one batch.
        let mut batch = Vec::new();
        let mut computed = false;
        for (adaptor, pieces) in &mut self.active {
            while adaptor.state() == State::NeedsCompute {
                computed = true;
                match adaptor.compute()? {
                    AdaptorStep::Yield => {}
                    AdaptorStep::NeedsIO(reads) => {
                        batch.extend(reads);
                        break;
                    }
                    AdaptorStep::Piece(piece) => {
                        pieces.push(piece_mask(&self.scan.session, piece)?);
                    }
                }
            }
        }
        if !batch.is_empty() {
            tracing::debug!(
                row_start = self.rows.start,
                row_end = self.rows.end,
                segments = batch.len(),
                "v2 pruning reads started"
            );
            return Ok(PlannerOutput::NeedsIO(batch));
        }
        if computed
            || self
                .active
                .iter()
                .any(|(adaptor, _)| adaptor.state() != State::Done)
        {
            return Ok(PlannerOutput::Continue);
        }
        // Every pruning plan has run: the planner this one continues into gets the kept rows.
        let pruned = !self.active.is_empty();
        let kept = pruned.then(|| self.kept()).transpose()?;
        self.done = true;
        let metrics = &self.scan.metrics;
        if let Some(started) = self.started {
            metrics.prune_duration.update(started.elapsed());
        }
        if let Some(kept) = &kept {
            metrics.prune_rows_in.add(kept.len() as u64);
            metrics.prune_rows_kept.add(kept.true_count() as u64);
        }
        tracing::debug!(
            row_start = self.rows.start,
            row_end = self.rows.end,
            kept_rows = kept.as_ref().map(Mask::true_count),
            "v2 pruning finished"
        );
        if kept.as_ref().is_some_and(Mask::all_false) {
            return Ok(PlannerOutput::Done);
        }
        let next = self
            .next
            .take()
            .ok_or_else(|| vortex_err!("the prune planner has nothing to continue into"))?;
        Ok(PlannerOutput::Planner(
            WorkScope {
                file_ordinal: 0,
                rows: self.rows.clone(),
            },
            next(kept),
        ))
    }
}
