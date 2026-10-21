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
//! directly and supports projection, filters, row ranges, and selections. Unsupported layouts return an
//! error; limits are not part of this builder's API. Work is partitioned into fixed
//! chunks of up to 65,536 rows, or 1,048,576 rows for filtered scans, independently of the stored
//! layout's chunk boundaries.
//! Full-file projections use smaller 8,192-row multiples when serialized bytes per row are high.

mod cache;
mod io;
mod split;
#[cfg(test)]
mod tests;

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::SchemaRef;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

pub use self::cache::PlanCache;
use crate::LayoutRef;
use crate::scan::arrow::to_record_batch;
use crate::scan::execution::ScanPlans;
pub use crate::scan::execution::SegmentLocation;
use crate::scan::execution::plan_selection;
use crate::scan::v2::cache::Planned;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::split::SplitPlan;
use crate::segments::SegmentSource;

const ROW_BLOCK_SIZE: usize = 8192;
const MAX_ROWS_PER_CHUNK: usize = 8 * ROW_BLOCK_SIZE;
const FILTERED_ROWS_PER_CHUNK: usize = 16 * MAX_ROWS_PER_CHUNK;
const TARGET_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;

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
/// This builder reads the file's layout and segments directly. Unsupported layouts return an
/// error when the stream is polled. Row limits are not supported.
pub struct ScanBuilder<A = ArrayRef> {
    session: VortexSession,
    file: ScanFile,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    ordered: bool,
    row_range: Option<Range<u64>>,
    selection: Selection,
    concurrency: usize,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    row_offset: u64,
    plan_cache: Option<PlanCache>,
}

impl ScanBuilder<ArrayRef> {
    /// Creates a V2 scan over the file's layout and segment source.
    pub fn new(session: VortexSession, file: ScanFile) -> Self {
        Self {
            projection: BoundExpression::new_root(file.layout.dtype().clone()),
            filter: None,
            session,
            file,
            ordered: true,
            row_range: None,
            selection: Selection::default(),
            concurrency: 4,
            map_fn: Arc::new(Ok),
            row_offset: 0,
            plan_cache: None,
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

    /// Evaluates a bound predicate before projecting the surviving rows.
    pub fn with_some_filter(mut self, filter: Option<BoundExpression>) -> Self {
        self.filter = filter;
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

    /// Plans through `cache`, sharing plans and whole values with the other scans of the file
    /// given it.
    pub fn with_plan_cache(mut self, cache: PlanCache) -> Self {
        self.plan_cache = Some(cache);
        self
    }

    /// Sets the number of active splits per worker thread. Must be greater than zero.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Maps each output batch on its split's task.
    pub fn map<B: 'static>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> ScanBuilder<B> {
        let old_map_fn = self.map_fn;
        ScanBuilder {
            session: self.session,
            file: self.file,
            projection: self.projection,
            filter: self.filter,
            ordered: self.ordered,
            row_range: self.row_range,
            selection: self.selection,
            concurrency: self.concurrency,
            row_offset: self.row_offset,
            plan_cache: self.plan_cache,
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
    io: Arc<dyn IoService>,
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
        let planned = match &builder.plan_cache {
            Some(cache) => cache.get(
                &file.layout,
                &file.locations,
                builder.projection,
                builder.filter,
                &builder.session,
            )?,
            None => Planned::uncached(
                &file.layout,
                &file.locations,
                builder.projection,
                builder.filter,
                &builder.session,
            )?,
        };
        let row_count = planned.root.row_count();
        let chunk_rows = if planned.filter.is_some() {
            FILTERED_ROWS_PER_CHUNK
        } else if planned.identity {
            rows_per_chunk(row_count, &file.locations)
        } else {
            MAX_ROWS_PER_CHUNK
        };
        let splits = row_chunks(builder.row_range.unwrap_or(0..row_count), chunk_rows);
        let io = Arc::new(SegmentScanIo::new(file.segments, planned.ranges));
        Ok(Self {
            session: builder.session.clone(),
            plans: ScanPlans {
                session: builder.session,
                locations: file.locations,
                projection: planned.projection,
                filter: planned.filter,
                row_offset: builder.row_offset,
                values: planned.values,
            },
            io,
            splits,
            selection: builder.selection,
            map_fn: builder.map_fn,
            ordered: builder.ordered,
            concurrency: builder.concurrency,
        })
    }

    /// The splits with selected rows, each ready to be admitted to a planning run.
    fn split_plans(&self) -> Vec<SplitPlan> {
        self.splits
            .iter()
            .filter_map(|rows| {
                let (scope, root) = plan_selection(
                    &self.plans,
                    WorkScope {
                        file_ordinal: 0,
                        rows: rows.clone(),
                    },
                    &self.selection,
                )?;
                Some(SplitPlan { scope, root })
            })
            .collect()
    }

    /// Runs every split as a task on the session's runtime and yields their mapped batches.
    ///
    /// Like the default executor, up to `concurrency` splits per thread run at once, and their
    /// batches are yielded in row order when the scan is ordered.
    fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let split_plans = self.split_plans();
        let io = self.io;
        let map_fn = self.map_fn;
        // An unordered scan hands each batch on as it is mapped, so a split holds no more than
        // the batch its consumer has not taken.
        if !self.ordered {
            let streams = stream::iter(split_plans).map(move |split| {
                let (send, recv) = mpsc::channel(1);
                let task =
                    handle.spawn(split.run_unordered(Arc::clone(&io), Arc::clone(&map_fn), send));
                let errors = stream::once(task)
                    .filter_map(|result| std::future::ready(result.err().map(Err)));
                stream::select(recv, errors)
            });
            return Ok(streams.flatten_unordered(concurrency).boxed());
        }
        // Spawned as the buffer pulls them, so at most `concurrency` splits read at once.
        let tasks = stream::iter(split_plans).map(move |split| {
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
