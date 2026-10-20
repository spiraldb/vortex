// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The planner-driven scan executor.
//!
//! A V2 scan lowers the file's layout once into a physical plan and runs every split of it
//! through the planning protocol: the [`Run`](vortex_scan::planning::driver::Run) driver advances
//! the split's planners and morsels, and every segment read is a protocol request served from the
//! file's [`SegmentSource`]. The layout reader is never asked to evaluate anything.
//!
//! Import this module's [`ScanBuilder`] to select V2 explicitly. It accepts a [`ScanFile`]
//! directly and supports projection, row ranges, and selections. Layouts without a pipeline
//! implementation return an error; filters and limits are not part of this builder's API. Work is
//! partitioned into fixed chunks of up to 65,536 rows, independently of the stored layout's chunk
//! boundaries. Full-file projections use smaller 8,192-row multiples when serialized bytes per row
//! are high. A partial selection is instead planned by the stored segments its rows read, so each
//! segment is read once and rows between selected runs are never visited.

mod io;
mod split;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::SchemaRef;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;
use vortex_io::session::RuntimeSessionExt;
use vortex_mask::AllOr;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::LayoutRef;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::pipeline::Reach;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::arrow::to_record_batch;
use crate::scan::execution::ScanPlans;
pub use crate::scan::execution::SegmentLocation;
use crate::scan::execution::plan_projection;
use crate::scan::v2::io::SegmentRanges;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::split::SplitPlan;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;

const ROW_BLOCK_SIZE: usize = 8192;
const MAX_ROWS_PER_CHUNK: usize = 8 * ROW_BLOCK_SIZE;
const TARGET_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;
/// A chunk selected in more runs than this is planned whole: finding the segments of each run
/// would cost more than the chunks it could skip.
const MAX_RUNS: usize = 64;
/// A planner grows to share stored segments only while it spans at most this many rows, so a
/// segment spanning much of the file cannot join every selected run into one task.
const MAX_PLANNER_ROWS: u64 = 4 * MAX_ROWS_PER_CHUNK as u64;

/// Limit estimated bytes per task without changing the writer-aligned row granularity.
fn rows_per_chunk(row_count: u64, locations: &[SegmentLocation]) -> usize {
    if row_count <= MAX_ROWS_PER_CHUNK as u64 {
        return MAX_ROWS_PER_CHUNK;
    }
    let bytes: u64 = locations
        .iter()
        .map(|location| u64::from(location.length))
        .sum();
    let bytes_per_row = bytes.div_ceil(row_count).max(1);
    let blocks = TARGET_BYTES_PER_CHUNK / bytes_per_row / ROW_BLOCK_SIZE as u64;
    blocks.clamp(1, 8) as usize * ROW_BLOCK_SIZE
}

/// The rows of one planner's pipeline splits, in row order.
type PlannerSplits = Vec<Range<u64>>;

/// Whether two sorted segment lists have a segment in common.
fn shares_any(left: &[SegmentId], right: &[SegmentId]) -> bool {
    let (mut left, mut right) = (left.iter().peekable(), right.iter().peekable());
    while let (Some(l), Some(r)) = (left.peek(), right.peek()) {
        match l.cmp(r) {
            Ordering::Less => _ = left.next(),
            Ordering::Greater => _ = right.next(),
            Ordering::Equal => return true,
        }
    }
    false
}

/// Fixed boundaries in the file's row domain, clipped to the requested range.
fn row_chunks(rows: Range<u64>, rows_per_chunk: usize) -> Vec<Range<u64>> {
    if rows.is_empty() {
        return Vec::new();
    }
    let first = rows.start / rows_per_chunk as u64 * rows_per_chunk as u64;
    (first..rows.end)
        .step_by(rows_per_chunk)
        .map(|start| {
            start.max(rows.start)..start.saturating_add(rows_per_chunk as u64).min(rows.end)
        })
        .collect()
}

/// The parts of a file the planner-driven executor reads through.
#[derive(Clone)]
pub struct ScanFile {
    /// The root layout to scan.
    pub layout: LayoutRef,
    /// The location of every segment, indexed by segment id.
    pub locations: Arc<[SegmentLocation]>,
    /// Fetches segment bytes by id.
    pub segments: Arc<dyn SegmentSource>,
}

/// Builds a projection scan on the planner-driven V2 executor.
///
/// This builder reads the file's layout and segments directly. A layout with no pipeline
/// implementation returns an error when the first split that reads it is planned. Filters and row
/// limits are not supported.
pub struct ScanBuilder<A = ArrayRef> {
    session: VortexSession,
    file: ScanFile,
    projection: BoundExpression,
    ordered: bool,
    row_range: Option<Range<u64>>,
    selection: Selection,
    concurrency: usize,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    row_offset: u64,
}

impl ScanBuilder<ArrayRef> {
    /// Creates a V2 scan over the file's layout and segment source.
    pub fn new(session: VortexSession, file: ScanFile) -> Self {
        Self {
            projection: BoundExpression::new_root(file.layout.dtype().clone()),
            session,
            file,
            ordered: true,
            row_range: None,
            selection: Selection::default(),
            concurrency: 4,
            map_fn: Arc::new(Ok),
            row_offset: 0,
        }
    }

    /// Converts each output batch to Arrow on the split's task.
    pub fn into_record_batch_stream(
        self,
        schema: SchemaRef,
    ) -> VortexResult<impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static> {
        let struct_field = Field::new_struct("", schema.fields().clone(), false);
        let session = self.session.clone();
        Ok(self
            .map(move |chunk| {
                let mut ctx = session.create_execution_ctx();
                to_record_batch(chunk, &struct_field, &mut ctx)
            })
            .into_stream()?
            .map_err(|error| ArrowError::ExternalError(Box::new(error))))
    }
}

impl<A: 'static + Send> ScanBuilder<A> {
    /// The output dtype after projection, before any output mapping.
    pub fn dtype(&self) -> &DType {
        self.projection.dtype()
    }

    /// Sets a projection bound against the file's dtype.
    pub fn with_projection(mut self, projection: BoundExpression) -> Self {
        self.projection = projection;
        self
    }

    /// Configures whether batches are yielded in file order.
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Restricts scanning to a contiguous file-local row range.
    pub fn with_row_range(mut self, row_range: Range<u64>) -> Self {
        self.row_range = Some(row_range);
        self
    }

    /// Applies a row selection within the scan's row range.
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Selects rows by strictly sorted file-local indices.
    pub fn with_row_indices(self, row_indices: StrictSortedBuffer<u64>) -> Self {
        self.with_selection(Selection::IncludeByIndex(row_indices))
    }

    /// Sets the root row offset used by row-index expressions.
    pub fn with_row_offset(mut self, row_offset: u64) -> Self {
        self.row_offset = row_offset;
        self
    }

    /// Sets the number of active splits per worker thread. Must be greater than zero.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Maps each output batch on its split's task.
    fn map<B: 'static>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> ScanBuilder<B> {
        let old_map_fn = self.map_fn;
        ScanBuilder {
            session: self.session,
            file: self.file,
            projection: self.projection,
            ordered: self.ordered,
            row_range: self.row_range,
            selection: self.selection,
            concurrency: self.concurrency,
            row_offset: self.row_offset,
            map_fn: Arc::new(move |array| old_map_fn(array).and_then(&map_fn)),
        }
    }

    /// Runs the scan on the session's runtime, preparing it on the first poll.
    /// Preparation errors are returned as stream items.
    pub fn into_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let handle = self.session.handle();
        Ok(stream::once(async move {
            handle
                .spawn_cpu(move || PreparedScan::try_new(self)?.execute_stream())
                .await
        })
        .try_flatten()
        .boxed())
    }
}

/// A scan planned over its file, ready to run its splits.
struct PreparedScan<A: 'static + Send> {
    session: VortexSession,
    plans: ScanPlans,
    /// Serves the splits' reads.
    io: Arc<SegmentScanIo>,
    /// The scan's splits, over the root row domain, in row order.
    splits: Vec<Range<u64>>,
    selection: Selection,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    ordered: bool,
    concurrency: usize,
}

impl<A: 'static + Send> PreparedScan<A> {
    /// Lowers the file's layout, plans the projection, and partitions the selected range into
    /// fixed-size row chunks.
    fn try_new(builder: ScanBuilder<A>) -> VortexResult<Self> {
        let file = builder.file;
        let root = lower(&file.layout)?;
        let row_count = root.row_count();
        // An identity projection needs no rewrites. Keep the lowered children lazy, including
        // statistics that a projection-only scan never reads.
        let identity = builder.projection.is_root() && builder.projection.dtype() == root.dtype();
        let projection = if identity {
            root
        } else {
            optimize(plan_row_idx_expression(builder.projection, root)?)?
        };
        let chunk_rows = if identity {
            rows_per_chunk(row_count, &file.locations)
        } else {
            MAX_ROWS_PER_CHUNK
        };
        let splits = row_chunks(builder.row_range.unwrap_or(0..row_count), chunk_rows);
        let io = Arc::new(SegmentScanIo::new(
            file.segments,
            SegmentRanges::new(Arc::clone(&file.locations)),
        ));
        Ok(Self {
            session: builder.session.clone(),
            plans: ScanPlans {
                session: builder.session,
                locations: file.locations,
                projection,
                row_offset: builder.row_offset,
                values: Default::default(),
            },
            io,
            splits,
            selection: builder.selection,
            map_fn: builder.map_fn,
            ordered: builder.ordered,
            concurrency: builder.concurrency,
        })
    }

    /// Plans the scan's planners one at a time, in row order.
    fn planners(&self) -> Planners {
        Planners {
            plans: self.plans.clone(),
            selection: self.selection.clone(),
            io: Arc::clone(&self.io),
            chunks: self.splits.clone().into_iter(),
            runs: VecDeque::new(),
            whole: None,
            splits: Vec::new(),
            last: Vec::new(),
            reached: Vec::new(),
        }
    }

    /// Runs every split as a task on the session's runtime and yields their mapped batches.
    ///
    /// Like the default executor, up to `concurrency` splits per thread run at once, and their
    /// batches are yielded in row order when the scan is ordered.
    fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let planners = self.planners();
        let io = self.io;
        let map_fn = self.map_fn;
        // Each planner is spawned once planned, as the buffer pulls it, so at most `concurrency`
        // run at once.
        let mut tasks = planners.map(move |split| -> BoxFuture<'static, VortexResult<Vec<A>>> {
            let split = match split {
                Ok(split) => split,
                Err(error) => return future::ready(Err(error)).boxed(),
            };
            let io = Arc::clone(&io);
            let map_fn = Arc::clone(&map_fn);
            handle
                .spawn(async move {
                    split
                        .run(io)
                        .await?
                        .into_iter()
                        .map(|array| map_fn(array))
                        .collect::<VortexResult<Vec<_>>>()
                })
                .boxed()
        });
        // The first are planned and spawned now, each before the next is planned, so they run
        // while the stream is handed back to its consumer.
        let started: Vec<_> = tasks.by_ref().take(concurrency).collect();
        let tasks = stream::iter(started).chain(stream::iter(tasks));
        let batches = if self.ordered {
            tasks.buffered(concurrency).boxed()
        } else {
            tasks.buffer_unordered(concurrency).boxed()
        };
        Ok(batches
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten()
            .boxed())
    }
}

/// What the next fixed chunk holds for planning.
enum NextRun {
    /// A selected run to group into a planner.
    Run(Range<u64>),
    /// The selected span of a chunk planned whole.
    Whole(Range<u64>),
    /// Every chunk has been visited.
    Done,
}

/// Plans a scan's planners one at a time, in row order, so each can run on a task of its own as
/// soon as it is planned, while the rest are still being planned.
///
/// A fully selected chunk, or one selected in more than [`MAX_RUNS`] runs, is one split of its
/// own planner. Otherwise each selected run joins the planner before it, even across chunk
/// boundaries, while the two read a stored segment in common and the planner spans at most
/// [`MAX_PLANNER_ROWS`], so a stored segment is decoded by one planner unless it spans more rows
/// than that. Within a planner, a run extends the split before it when both read exactly the
/// same segments: every row between them then lies in those segments' chunks, so the split visits
/// no chunk the selection skips.
///
/// A column's segments hold contiguous rows, so a run can only share a segment with an earlier
/// run of its planner if it shares one with the run just before it, and a segment several
/// planners read is read by neighbouring planners. So a planner is complete once the run after it
/// is known, and the segments it shares with that run are all it shares with later planners: the
/// scan's IO reads each of them once for both.
struct Planners {
    plans: ScanPlans,
    selection: Selection,
    io: Arc<SegmentScanIo>,
    /// The fixed chunks not yet visited.
    chunks: std::vec::IntoIter<Range<u64>>,
    /// The selected runs of the last chunk visited, not yet grouped.
    runs: VecDeque<Range<u64>>,
    /// A chunk planned whole, waiting for the planner before it to be handed out.
    whole: Option<Range<u64>>,
    /// The pipeline splits of the planner being grouped.
    splits: PlannerSplits,
    /// The segments the last run of the planner reads, and those all its runs read.
    last: Vec<SegmentId>,
    reached: Vec<SegmentId>,
}

impl Planners {
    /// The next planner, ready to be admitted to a planning run.
    fn next_plan(&mut self) -> VortexResult<Option<SplitPlan>> {
        let Some(ranges) = self.next_splits()? else {
            return Ok(None);
        };
        let rows = ranges[0].start..ranges[ranges.len() - 1].end;
        let splits = ranges
            .into_iter()
            .map(|rows| Split {
                mask: self.selection.row_mask(&rows).mask().clone(),
                rows,
            })
            .collect();
        let scope = WorkScope {
            file_ordinal: 0,
            rows,
        };
        Ok(Some(SplitPlan {
            root: plan_projection(self.plans.clone(), scope.clone(), splits),
            scope,
        }))
    }

    /// The pipeline splits of the next planner, in row order.
    fn next_splits(&mut self) -> VortexResult<Option<PlannerSplits>> {
        if let Some(whole) = self.whole.take() {
            return Ok(Some(vec![whole]));
        }
        loop {
            match self.next_run() {
                NextRun::Done => {
                    return Ok((!self.splits.is_empty()).then(|| self.close(&[])));
                }
                NextRun::Whole(span) => {
                    if self.splits.is_empty() {
                        return Ok(Some(vec![span]));
                    }
                    self.whole = Some(span);
                    return Ok(Some(self.close(&[])));
                }
                NextRun::Run(run) => {
                    let mut segments = Vec::new();
                    self.plans.projection.reach(
                        run.clone(),
                        &Reach::Offset(0),
                        &mut |segment, _| segments.push(segment),
                    )?;
                    segments.sort_unstable();
                    segments.dedup();
                    let joins = self.splits.first().is_some_and(|first| {
                        run.end - first.start <= MAX_PLANNER_ROWS
                            && shares_any(&self.last, &segments)
                    });
                    if joins {
                        match self.splits.last_mut() {
                            Some(split) if segments == self.last => split.end = run.end,
                            _ => self.splits.push(run),
                        }
                        self.reached.extend_from_slice(&segments);
                        self.last = segments;
                        continue;
                    }
                    let planner = (!self.splits.is_empty()).then(|| self.close(&segments));
                    self.splits.push(run);
                    self.reached.extend_from_slice(&segments);
                    self.last = segments;
                    if planner.is_some() {
                        return Ok(planner);
                    }
                }
            }
        }
    }

    /// Hands out the planner being grouped. The segments it shares with `next`, those the run
    /// after it reads, are read once for it and the planner that run starts.
    fn close(&mut self, next: &[SegmentId]) -> PlannerSplits {
        self.reached.sort_unstable();
        self.reached.dedup();
        self.io.share_with_next(
            self.reached
                .iter()
                .filter(|segment| next.binary_search(segment).is_ok())
                .copied(),
        );
        self.reached.clear();
        self.last.clear();
        std::mem::take(&mut self.splits)
    }

    fn next_run(&mut self) -> NextRun {
        loop {
            if let Some(run) = self.runs.pop_front() {
                return NextRun::Run(run);
            }
            let Some(rows) = self.chunks.next() else {
                return NextRun::Done;
            };
            let row_mask = self.selection.row_mask(&rows);
            let mask = row_mask.mask();
            match mask.slices() {
                AllOr::None => {}
                AllOr::Some(slices) if slices.len() <= MAX_RUNS => self.runs.extend(
                    slices
                        .iter()
                        .map(|&(start, end)| rows.start + start as u64..rows.start + end as u64),
                ),
                AllOr::All | AllOr::Some(_) => {
                    let span = mask.first().unwrap_or(0)..mask.last().map_or(0, |last| last + 1);
                    return NextRun::Whole(
                        rows.start + span.start as u64..rows.start + span.end as u64,
                    );
                }
            }
        }
    }
}

impl Iterator for Planners {
    type Item = VortexResult<SplitPlan>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_plan().transpose()
    }
}
