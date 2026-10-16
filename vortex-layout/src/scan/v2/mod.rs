// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The planner-driven scan executor.
//!
//! A V2 scan lowers the file's layout once into a physical plan. A file planner emits child
//! planners for selected row ranges, which run concurrently through the planning protocol.
//! Every segment read is a protocol request served from the file's [`SegmentSource`].
//! The layout reader is never asked to evaluate anything.
//!
//! Import this module's [`ScanBuilder`] to select V2 explicitly. It accepts a [`ScanFile`]
//! directly and supports projection, row ranges, and selections. Unsupported layouts return an
//! error; filters and limits are not part of this builder's API. The file planner partitions work
//! at the projected columns' stored chunk boundaries, subdividing spans larger than 100,000 rows.

mod io;
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
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;

use crate::LayoutRef;
use crate::layouts::chunked::Chunked;
use crate::layouts::dict::Dict;
use crate::layouts::flat::Flat;
use crate::layouts::struct_::Struct;
use crate::layouts::zoned::LegacyStats;
use crate::layouts::zoned::Zoned;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::arrow::to_record_batch;
use crate::scan::execution::FilePlanner;
use crate::scan::execution::ScanPlans;
pub use crate::scan::execution::SegmentLocation;
use crate::scan::execution::execute_file;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::io::segment_ranges;
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

    /// Converts each output batch to Arrow on the child planner's task.
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

    /// Sets the number of active child planners per worker thread. Must be greater than zero.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Maps each output batch on its child planner's task.
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

/// A file planner and the resources needed to execute its children.
struct PreparedScan<A: 'static + Send> {
    session: VortexSession,
    planner: FilePlanner,
    io: Arc<SegmentScanIo>,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    ordered: bool,
    concurrency: usize,
}

impl<A: 'static + Send> PreparedScan<A> {
    /// Lowers the file's layout and creates the planner that owns its work ranges.
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
        let io = Arc::new(SegmentScanIo::new(
            file.segments,
            segment_ranges(&file.locations),
        ));
        Ok(Self {
            session: builder.session.clone(),
            planner: FilePlanner::new(
                ScanPlans {
                    session: builder.session,
                    locations: file.locations,
                    projection,
                    row_offset: builder.row_offset,
                    values: Default::default(),
                },
                builder.row_range.unwrap_or(0..row_count),
                builder.selection,
            ),
            io,
            map_fn: builder.map_fn,
            ordered: builder.ordered,
            concurrency: builder.concurrency,
        })
    }

    fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        Ok(execute_file(
            self.planner,
            self.session,
            self.io,
            self.map_fn,
            self.ordered,
            self.concurrency,
        ))
    }
}
