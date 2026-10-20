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
use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::SchemaRef;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
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
use vortex_utils::aliases::hash_map::HashMap;
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

    /// The rows each planner projects, as the pipeline splits that produce them, in row order.
    ///
    /// A fully selected chunk, or one selected in more than [`MAX_RUNS`] runs, is one split of
    /// its own planner. Otherwise each selected run joins the planner before it, even across
    /// chunk boundaries, while the two read a stored segment in common and the planner spans at
    /// most [`MAX_PLANNER_ROWS`], so a stored segment is decoded by one planner unless it spans
    /// more rows than that. A segment several planners read is still read once: the scan's IO
    /// shares the read between them. Within a planner, a run extends the split before it when
    /// both read exactly the same segments: every row between them then lies in those segments'
    /// chunks, so the split visits no chunk the selection skips.
    ///
    /// A column's segments hold contiguous rows, so a run can only share a segment with an
    /// earlier run of its planner if it shares one with the run just before it.
    ///
    /// Also returns how many planners read each segment that more than one of them reads.
    fn split_rows(&self) -> VortexResult<(Vec<PlannerSplits>, HashMap<SegmentId, usize>)> {
        let mut planners: Vec<PlannerSplits> = Vec::new();
        // The segments the last selected run reads, or none once its planner is closed.
        let mut previous = Vec::new();
        // The segments the last planner reads, and how many planners read each segment.
        let mut reached: Vec<SegmentId> = Vec::new();
        let mut readers: HashMap<SegmentId, usize> = HashMap::default();
        let mut close = |reached: &mut Vec<SegmentId>| {
            reached.sort_unstable();
            reached.dedup();
            for segment in reached.drain(..) {
                *readers.entry(segment).or_default() += 1;
            }
        };
        for rows in &self.splits {
            let row_mask = self.selection.row_mask(rows);
            let mask = row_mask.mask();
            let slices = match mask.slices() {
                AllOr::None => continue,
                AllOr::Some(slices) if slices.len() <= MAX_RUNS => slices,
                AllOr::All | AllOr::Some(_) => {
                    let span = mask.first().unwrap_or(0)..mask.last().map_or(0, |last| last + 1);
                    let split = rows.start + span.start as u64..rows.start + span.end as u64;
                    planners.push(vec![split]);
                    previous.clear();
                    close(&mut reached);
                    continue;
                }
            };
            for &(start, end) in slices {
                let run = rows.start + start as u64..rows.start + end as u64;
                let mut segments = Vec::new();
                self.plans.projection.reach(
                    run.clone(),
                    &Reach::Offset(0),
                    &mut |segment, _| segments.push(segment),
                )?;
                segments.sort_unstable();
                segments.dedup();
                let joins = planners.last().is_some_and(|splits| {
                    splits
                        .first()
                        .is_some_and(|first| run.end - first.start <= MAX_PLANNER_ROWS)
                        && shares_any(&previous, &segments)
                });
                match planners.last_mut() {
                    Some(splits) if joins && segments == previous => {
                        if let Some(split) = splits.last_mut() {
                            split.end = run.end;
                        }
                    }
                    Some(splits) if joins => splits.push(run),
                    _ => {
                        close(&mut reached);
                        planners.push(vec![run]);
                    }
                }
                reached.extend_from_slice(&segments);
                previous = segments;
            }
        }
        close(&mut reached);
        readers.retain(|_, count| *count > 1);
        Ok((planners, readers))
    }

    /// The planners with selected rows, each ready to be admitted to a planning run.
    fn split_plans(&self) -> VortexResult<Vec<SplitPlan>> {
        let (planners, shared) = self.split_rows()?;
        self.io.share(shared);
        Ok(planners
            .into_iter()
            .filter_map(|ranges| {
                let rows = ranges.first()?.start..ranges.last()?.end;
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
                Some(SplitPlan {
                    root: plan_projection(self.plans.clone(), scope.clone(), splits),
                    scope,
                })
            })
            .collect())
    }

    /// Runs every split as a task on the session's runtime and yields their mapped batches.
    ///
    /// Like the default executor, up to `concurrency` splits per thread run at once, and their
    /// batches are yielded in row order when the scan is ordered.
    fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let split_plans = self.split_plans()?;
        let io = self.io;
        let map_fn = self.map_fn;
        // Spawned as the buffer pulls them, so at most `concurrency` splits read at once.
        let mut tasks = split_plans.into_iter().map(move |split| {
            let io = Arc::clone(&io);
            let map_fn = Arc::clone(&map_fn);
            handle.spawn(async move {
                split
                    .run(io)
                    .await?
                    .into_iter()
                    .map(|array| map_fn(array))
                    .collect::<VortexResult<Vec<_>>>()
            })
        });
        // The first are spawned now, so they run while the stream is handed back to its consumer.
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
