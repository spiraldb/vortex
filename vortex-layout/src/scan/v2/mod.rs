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
//! directly and supports projection, row ranges, and selections. Unsupported layouts return an
//! error; filters and limits are not part of this builder's API.

mod io;
mod split;
pub(super) mod splits;
#[cfg(test)]
mod tests;

use std::iter;
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
use vortex_error::vortex_ensure;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::LayoutRef;
use crate::layouts::chunked::Chunked;
use crate::layouts::dict::Dict;
use crate::layouts::flat::Flat;
use crate::layouts::struct_::Struct;
use crate::layouts::zoned::LegacyStats;
use crate::layouts::zoned::Zoned;
use crate::plan::physical::PlanRef;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::arrow::to_record_batch;
use crate::scan::planning::ScanPlans;
pub use crate::scan::planning::SegmentLocation;
use crate::scan::planning::plan_projection;
use crate::scan::split_by::SplitBy;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::io::segment_ranges;
use crate::scan::v2::split::SplitPlan;
use crate::scan::v2::splits::chunk_starts;
use crate::scan::v2::splits::projection_splits;
use crate::segments::SegmentSource;

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
/// error when the stream is polled. Filters and row limits are not supported.
pub struct ScanBuilder<A = ArrayRef> {
    session: VortexSession,
    file: ScanFile,
    projection: BoundExpression,
    ordered: bool,
    row_range: Option<Range<u64>>,
    selection: Selection,
    split_by: SplitBy,
    natural_splits: Option<Arc<[u64]>>,
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
            split_by: SplitBy::default(),
            natural_splits: None,
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

    /// Configures how natural scan work is split for concurrency.
    pub fn with_split_by(mut self, split_by: SplitBy) -> Self {
        self.split_by = split_by;
        self
    }

    /// Supplies strictly increasing full-file boundaries, overriding the split policy.
    /// Boundaries are clamped to the scan's row range.
    pub fn with_natural_splits(mut self, boundaries: Arc<[u64]>) -> Self {
        debug_assert!(
            boundaries.windows(2).all(|w| w[0] < w[1]),
            "natural split boundaries must be strictly increasing"
        );
        self.natural_splits = Some(boundaries);
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
            split_by: self.split_by,
            natural_splits: self.natural_splits,
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

/// Whether every required layout lowers to plans with a pipeline implementation.
///
/// List layouts lower to a list assembly, which cannot compile before its offsets are read.
fn supports_layout(layout: &LayoutRef, identity: bool) -> VortexResult<bool> {
    if !(layout.is::<Flat>()
        || layout.is::<Chunked>()
        || layout.is::<Struct>()
        || layout.is::<Dict>()
        || layout.is::<Zoned>()
        || layout.is::<LegacyStats>())
    {
        return Ok(false);
    }
    // An identity projection cannot use zone statistics. Leave their layouts uninitialized.
    if identity && (layout.is::<Zoned>() || layout.is::<LegacyStats>()) {
        return match layout.slot(0)? {
            Some(data) => supports_layout(&data, identity),
            None => Ok(false),
        };
    }
    for child in layout.children()? {
        if !supports_layout(&child, identity)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A scan planned over its file, ready to run its splits.
struct PreparedScan<A: 'static + Send> {
    session: VortexSession,
    plans: ScanPlans,
    /// Whole dictionary values and the root rows that use them, for an identity projection.
    value_scopes: Option<Vec<(Range<u64>, PlanRef)>>,
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
    /// Lowers the file's layout, plans the projection over it, and cuts the scan's row range
    /// where the projection's chunks start.
    fn try_new(builder: ScanBuilder<A>) -> VortexResult<Self> {
        let file = builder.file;
        vortex_ensure!(
            supports_layout(&file.layout, builder.projection.is_root())?,
            "V2 scans do not support this file's layout"
        );
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
        let mut value_scopes = identity.then(Vec::new);
        let projection_starts: Arc<[u64]> =
            chunk_starts([&projection], value_scopes.as_mut())?.into();
        let rows = builder.row_range.unwrap_or(0..row_count);
        // Whole-column dictionaries already need the scan's full lifetime. Only dictionaries
        // covering part of the scan need separate cache owners for their consuming splits.
        let value_scopes = value_scopes.filter(|values| {
            values
                .iter()
                .any(|(used, _)| used.start > rows.start || used.end < rows.end)
        });
        let splits = if let Some(boundaries) = builder.natural_splits {
            projection_splits(&boundaries, rows)
        } else {
            let natural = projection_splits(&projection_starts, rows.clone());
            let boundaries = natural
                .iter()
                .map(|split| split.start)
                .chain(iter::once(rows.end))
                .collect();
            let boundaries = builder.split_by.split_boundaries(boundaries, &rows)?;
            projection_splits(&boundaries, rows)
        };
        let io = Arc::new(SegmentScanIo::new(
            file.segments,
            segment_ranges(&file.locations),
        ));
        Ok(Self {
            session: builder.session.clone(),
            plans: ScanPlans {
                session: builder.session,
                locations: file.locations,
                projection,
                projection_starts,
                row_offset: builder.row_offset,
                values: Default::default(),
            },
            value_scopes,
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
                let row_mask = self.selection.row_mask(rows);
                if row_mask.mask().all_false() {
                    return None;
                }
                let scope = WorkScope {
                    file_ordinal: 0,
                    rows: row_mask.row_range(),
                };
                let mut plans = self.plans.clone();
                if let Some(values) = &self.value_scopes {
                    plans.values = self.plans.values.for_plans(
                        values
                            .iter()
                            .filter(|(used, _)| used.start < rows.end && rows.start < used.end)
                            .map(|(_, plan)| plan),
                    );
                }
                Some(SplitPlan {
                    root: plan_projection(plans, scope.clone(), row_mask.mask().clone()),
                    scope,
                })
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
