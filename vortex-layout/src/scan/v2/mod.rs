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
//! error; filters and limits are not part of this builder's API. Work is partitioned into fixed
//! chunks of up to 65,536 rows, independently of the stored layout's chunk boundaries.
//! Full-file projections use smaller 8,192-row multiples when serialized bytes per row are high.
//! When a wide projection has too few row chunks to occupy every thread, each chunk is further
//! divided into groups of columns that decode on their own tasks, and the groups' batches are
//! zipped back into one batch per row chunk.
//!
//! A [`ScanFile`] carrying a [`ScanCache`] shares its lowered plan, segment index, and dictionary
//! values with every other scan of that file, like a cached layout reader does for V1 scans.

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
use arrow_schema::Fields;
use arrow_schema::SchemaRef;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::try_join_all;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::expr::BoundExpression;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::aliases::hash_set::HashSet;
use vortex_utils::parallelism::get_available_parallelism;

pub use self::cache::ScanCache;
use crate::LayoutChildType;
use crate::LayoutRef;
use crate::plan::physical::Pack;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::arrow::to_record_batch;
use crate::scan::execution::ScanPlans;
pub use crate::scan::execution::SegmentLocation;
use crate::scan::execution::plan_selection;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::io::segment_ranges;
use crate::scan::v2::split::SplitPlan;
use crate::segments::SegmentSource;

const ROW_BLOCK_SIZE: usize = 8192;
const MAX_ROWS_PER_CHUNK: usize = 8 * ROW_BLOCK_SIZE;
const TARGET_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;
/// The fewest fields a column group holds. Narrower groups cost more in per-task planning than
/// they gain in parallelism.
const MIN_FIELDS_PER_GROUP: usize = 16;
/// The fewest fields a column group of a sparse selection holds. Such a scan decodes only a few
/// rows of each chunk it reads, so planning a group costs little next to reading its segments.
const SPARSE_MIN_FIELDS_PER_GROUP: usize = 4;
/// Tasks per thread a scan aims for when it divides row chunks into column groups, so that
/// uneven columns still leave every thread busy.
const TASKS_PER_THREAD: usize = 2;

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

/// The number of column groups each of `splits` row chunks of a projection with `nfields` fields
/// divides into, so the scan has at least [`TASKS_PER_THREAD`] tasks per thread, with at least
/// `min_fields` fields in each group.
fn column_group_count(
    nfields: usize,
    splits: usize,
    parallelism: usize,
    min_fields: usize,
) -> usize {
    let target = TASKS_PER_THREAD * parallelism;
    if splits == 0 || splits >= target {
        return 1;
    }
    target
        .div_ceil(splits)
        .min(nfields / min_fields)
        .max(1)
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

/// The rows at which `layout` starts a new chunk, ascending and unique, from `0` to the layout's
/// row count.
///
/// Fields and transparent children are followed down to the first chunked layout on each path.
/// Its chunks are not opened: their offsets come from the chunked layout itself, and a reopened
/// file would otherwise deserialize every chunk of every column for each scan. Chunks nested
/// inside a chunk are rare, and missing them only leaves a split wider than it could be.
fn layout_boundaries(layout: &LayoutRef) -> VortexResult<Vec<u64>> {
    fn collect(layout: &LayoutRef, base: u64, out: &mut Vec<u64>) -> VortexResult<()> {
        for slot in 0..layout.nslots() {
            match layout.slot_type(slot) {
                Some(LayoutChildType::Chunk((_, offset))) => out.push(base + offset),
                Some(LayoutChildType::Field(_) | LayoutChildType::Transparent(_)) => {
                    if let Some(child) = layout.slot(slot)? {
                        collect(&child, base, out)?;
                    }
                }
                Some(LayoutChildType::Auxiliary(_)) | None => {}
            }
        }
        Ok(())
    }

    let mut boundaries = vec![0, layout.row_count()];
    collect(layout, 0, &mut boundaries)?;
    boundaries.sort_unstable();
    boundaries.dedup();
    Ok(boundaries)
}

/// Spans of `rows` that start and end on `boundaries`, each covering consecutive spans between
/// boundaries until it holds at least `min_rows` rows.
///
/// A stored chunk larger than `min_rows` gets a span of its own, so no two spans read it. Smaller
/// stored chunks are grouped, so that a file of many small chunks is not cut into as many spans.
fn boundary_chunks(rows: Range<u64>, boundaries: &[u64], min_rows: u64) -> Vec<Range<u64>> {
    let mut spans = Vec::new();
    let mut start = rows.start;
    for &boundary in boundaries {
        if boundary <= start {
            continue;
        }
        if boundary >= rows.end {
            break;
        }
        if boundary - start >= min_rows {
            spans.push(start..boundary);
            start = boundary;
        }
    }
    if start < rows.end {
        spans.push(start..rows.end);
    }
    spans
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
    /// State shared by every scan of this file, or `None` to rebuild it for each scan.
    pub cache: Option<ScanCache>,
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
    /// Zips the mapped batches of a row chunk's column groups, in field order, into one batch.
    zip_fn: ZipFn<A>,
    row_offset: u64,
}

/// Zips column-group batches covering the same rows into one batch.
type ZipFn<A> = Arc<dyn Fn(Vec<A>) -> VortexResult<A> + Send + Sync>;

impl ScanBuilder<ArrayRef> {
    /// Creates a V2 scan over the file's layout and segment source.
    pub fn new(session: VortexSession, file: ScanFile) -> Self {
        let zip_session = session.clone();
        Self {
            projection: BoundExpression::new_root(file.layout.dtype().clone()),
            session,
            file,
            ordered: true,
            row_range: None,
            selection: Selection::default(),
            concurrency: 4,
            map_fn: Arc::new(Ok),
            zip_fn: Arc::new(move |parts| zip_struct_fields(parts, &zip_session)),
            row_offset: 0,
        }
    }

    /// Converts each output batch to Arrow on the split's task.
    pub fn into_record_batch_stream(
        self,
        schema: SchemaRef,
    ) -> VortexResult<impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static> {
        let struct_field = Field::new_struct("", schema.fields().clone(), false);
        let field_index: HashMap<String, usize> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().clone(), index))
            .collect();
        let session = self.session.clone();
        let zip_schema = Arc::clone(&schema);
        Ok(self
            .map(
                move |chunk| {
                    let mut ctx = session.create_execution_ctx();
                    // A column group's batch converts with the schema of its own fields.
                    let group_field = match chunk.dtype().as_struct_fields_opt() {
                        Some(fields) if fields.nfields() != schema.fields().len() => {
                            Some(group_struct_field(&schema, &field_index, fields.names())?)
                        }
                        _ => None,
                    };
                    to_record_batch(
                        chunk,
                        group_field.as_ref().unwrap_or(&struct_field),
                        &mut ctx,
                    )
                },
                move |parts| {
                    let columns = parts
                        .iter()
                        .flat_map(|batch| batch.columns().iter().cloned())
                        .collect();
                    Ok(RecordBatch::try_new(Arc::clone(&zip_schema), columns)?)
                },
            )
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

    /// Maps each output batch on its split's task. A batch of a column group is mapped on its own,
    /// and `zip_fn` then joins the mapped batches of a row chunk's groups.
    fn map<B: 'static>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
        zip_fn: impl Fn(Vec<B>) -> VortexResult<B> + 'static + Send + Sync,
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
            zip_fn: Arc::new(zip_fn),
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
    /// Plans of disjoint column groups that together make up the projection, in field order.
    /// Empty unless the scan divides its row chunks by column.
    column_groups: Vec<ScanPlans>,
    selection: Selection,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    zip_fn: ZipFn<A>,
    ordered: bool,
    concurrency: usize,
}

impl<A: 'static + Send> PreparedScan<A> {
    /// Lowers the file's layout, plans the projection, and partitions the selected range into
    /// fixed-size row chunks.
    fn try_new(builder: ScanBuilder<A>) -> VortexResult<Self> {
        let file = builder.file;
        let cache = file.cache.unwrap_or_default();
        let root = match cache.root() {
            Some(root) => root,
            None => cache.insert_root(lower(&file.layout)?),
        };
        let row_count = root.row_count();
        // An identity projection needs no rewrites. Keep the lowered children lazy, including
        // statistics that a projection-only scan never reads.
        let identity = builder.projection.is_root() && builder.projection.dtype() == root.dtype();
        let projection = if identity {
            root
        } else {
            match cache.projection(&builder.projection) {
                Some(plan) => plan,
                None => {
                    let plan =
                        optimize(plan_row_idx_expression(builder.projection.clone(), root)?)?;
                    cache.insert_projection(builder.projection, plan)
                }
            }
        };
        let chunk_rows = if identity {
            rows_per_chunk(row_count, &file.locations)
        } else {
            MAX_ROWS_PER_CHUNK
        };
        let rows = builder.row_range.unwrap_or(0..row_count);
        // A sparse selection reads few rows from each stored chunk it touches. Splitting at the
        // stored chunk boundaries reads and decodes each of those chunks once, where fixed row
        // chunks narrower than the stored ones would each read and decode the whole chunk again.
        // Stored chunks narrower than a work chunk are grouped up to its size.
        let sparse = matches!(builder.selection, Selection::IncludeByIndex(_));
        let splits = if sparse {
            let boundaries = match cache.boundaries() {
                Some(boundaries) => boundaries,
                None => cache.insert_boundaries(layout_boundaries(&file.layout)?.into()),
            };
            boundary_chunks(rows, &boundaries, chunk_rows as u64)
        } else {
            row_chunks(rows, chunk_rows)
        };
        // A wide struct projection with too few row chunks for every thread also splits by
        // column. The groups' plans share the projection's children, so planning them is cheap.
        // Arrow batches of the groups are matched to the schema by field name, so names must be
        // unique.
        let group_projections = match projection.as_opt::<Pack>() {
            Some(pack)
                if !pack.dtype().is_nullable() && has_unique_names(pack.fields().names()) =>
            {
                let nfields = pack.nfields();
                let groups = column_group_count(
                    nfields,
                    splits.len(),
                    get_available_parallelism().unwrap_or(1),
                    if sparse {
                        SPARSE_MIN_FIELDS_PER_GROUP
                    } else {
                        MIN_FIELDS_PER_GROUP
                    },
                );
                if groups > 1 {
                    (0..groups)
                        .map(|group| {
                            pack.project_range(
                                group * nfields / groups..(group + 1) * nfields / groups,
                            )
                        })
                        .collect::<VortexResult<Vec<_>>>()?
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        let ranges = match cache.ranges() {
            Some(ranges) => ranges,
            None => cache.insert_ranges(segment_ranges(&file.locations)),
        };
        let io = Arc::new(SegmentScanIo::new(file.segments, ranges));
        let plans = ScanPlans {
            session: builder.session,
            locations: file.locations,
            projection,
            row_offset: builder.row_offset,
            values: cache.values(),
        };
        let column_groups = group_projections
            .into_iter()
            .map(|projection| ScanPlans {
                projection,
                ..plans.clone()
            })
            .collect();
        Ok(Self {
            session: plans.session.clone(),
            plans,
            io,
            splits,
            column_groups,
            selection: builder.selection,
            map_fn: builder.map_fn,
            zip_fn: builder.zip_fn,
            ordered: builder.ordered,
            concurrency: builder.concurrency,
        })
    }

    /// The splits with selected rows, each ready to be admitted to a planning run: one per row
    /// chunk, or one per column group of each row chunk.
    fn split_plans(&self) -> Vec<Vec<SplitPlan>> {
        let groups = if self.column_groups.is_empty() {
            std::slice::from_ref(&self.plans)
        } else {
            self.column_groups.as_slice()
        };
        self.splits
            .iter()
            .filter_map(|rows| {
                let scope = WorkScope {
                    file_ordinal: 0,
                    rows: rows.clone(),
                };
                groups
                    .iter()
                    .map(|plans| {
                        let (scope, root) = plan_selection(plans, scope.clone(), &self.selection)?;
                        Some(SplitPlan { scope, root })
                    })
                    .collect()
            })
            .collect()
    }

    /// Runs every split as a task on the session's runtime and yields their mapped batches.
    ///
    /// Like the default executor, up to `concurrency` splits per thread run at once, and their
    /// batches are yielded in row order when the scan is ordered.
    fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let groups = self.column_groups.len().max(1);
        let concurrency = (self.concurrency * get_available_parallelism().unwrap_or(1))
            .div_ceil(groups)
            .max(1);
        let handle = self.session.handle();
        let split_plans = self.split_plans();
        let io = self.io;
        let map_fn = self.map_fn;
        let zip_fn = self.zip_fn;
        // Spawned as the buffer pulls them, so at most `concurrency` row chunks read at once.
        let tasks = stream::iter(split_plans).map(move |groups| {
            let parts = groups
                .into_iter()
                .map(|split| {
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
                })
                .collect::<Vec<_>>();
            let zip_fn = Arc::clone(&zip_fn);
            async move { zip_column_groups(try_join_all(parts).await?, zip_fn.as_ref()) }
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

fn has_unique_names(names: &FieldNames) -> bool {
    let mut seen = HashSet::with_capacity(names.len());
    names.iter().all(|name| seen.insert(name))
}

/// Zips the batches of a row chunk's column groups, batch by batch. Every group of a chunk
/// projects the same rows, so each yields the same batches.
fn zip_column_groups<A>(
    mut groups: Vec<Vec<A>>,
    zip_fn: &(dyn Fn(Vec<A>) -> VortexResult<A> + Send + Sync),
) -> VortexResult<Vec<A>> {
    if groups.len() == 1 {
        return Ok(groups.remove(0));
    }
    let batches = groups.first().map_or(0, Vec::len);
    for group in &groups {
        vortex_ensure_eq!(
            group.len(),
            batches,
            "column groups of one row chunk produced different numbers of batches"
        );
    }
    let mut groups = groups.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
    (0..batches)
        .map(|_| zip_fn(groups.iter_mut().filter_map(Iterator::next).collect()))
        .collect()
}

/// Joins struct arrays over the same rows into one struct array of all their fields, in order.
fn zip_struct_fields(parts: Vec<ArrayRef>, session: &VortexSession) -> VortexResult<ArrayRef> {
    let mut ctx = session.create_execution_ctx();
    let len = parts.first().map_or(0, |part| part.len());
    let mut names = Vec::new();
    let mut fields = Vec::new();
    for part in parts {
        vortex_ensure_eq!(part.len(), len, "column groups cover different rows");
        // Groups chunked by the stored layout pack into one struct of chunked fields.
        let part = part.execute::<StructArray>(&mut ctx)?;
        names.extend(part.names().iter().cloned());
        fields.extend(part.iter_unmasked_fields().cloned());
    }
    Ok(
        StructArray::try_new(FieldNames::from(names), fields, len, Validity::NonNullable)?
            .into_array(),
    )
}

/// The Arrow struct field for a column group's batch: the fields of `schema` named `names`.
fn group_struct_field(
    schema: &SchemaRef,
    field_index: &HashMap<String, usize>,
    names: &FieldNames,
) -> VortexResult<Field> {
    let fields = names
        .iter()
        .map(|name| {
            field_index
                .get(name.as_ref())
                .map(|&index| Arc::clone(&schema.fields()[index]))
                .ok_or_else(|| vortex_err!("column {name} is not in the Arrow schema"))
        })
        .collect::<VortexResult<Fields>>()?;
    Ok(Field::new_struct("", fields, false))
}
