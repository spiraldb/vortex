// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The planner-driven scan executor.
//!
//! A V2 scan lowers the file's layout once into a physical plan and runs every split of it
//! through the planning protocol: the [`Run`](vortex_scan::planning::driver::Run) driver advances
//! the split's planners and morsels, and every segment read is a protocol request served from the
//! file's [`SegmentSource`]. The layout reader is never asked to evaluate anything.
//!
//! `select` is the one place a streaming [`ScanBuilder`] picks between this executor and the
//! default one. It picks this one when `VORTEX_SCAN_V2=1`, the builder carries a [`ScanFile`],
//! and the scan only uses what this executor implements so far: no filter, no limit, and a layout
//! that lowers to plans with a pipeline implementation. Every other scan runs on the default
//! executor, unchanged.

mod io;
mod split;
pub(crate) mod splits;
#[cfg(test)]
mod tests;

use std::env;
use std::future;
use std::iter;
use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::next::next_fn;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::planning::selection::SelectedRows;
use vortex_scan::planning::selection::SelectionPlanner;
use vortex_scan::selection::Selection;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::parallelism::get_available_parallelism;

use crate::LayoutRef;
use crate::layouts::chunked::Chunked;
use crate::layouts::dict::Dict;
use crate::layouts::flat::Flat;
use crate::layouts::list::List;
use crate::layouts::struct_::Struct;
use crate::layouts::zoned::LegacyStats;
use crate::layouts::zoned::Zoned;
use crate::plan::physical::PlanRef;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::pipeline::Reach;
use crate::plan::physical::pipeline::ValueCache;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::planning::ProjectionPlanner;
use crate::scan::planning::ScanPlans;
pub use crate::scan::planning::SegmentLocation;
use crate::scan::scan_builder::ScanBuilder;
use crate::scan::v2::io::SegmentRanges;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::io::register_segments;
use crate::scan::v2::io::segment_ranges;
use crate::scan::v2::split::SplitPlan;
use crate::scan::v2::splits::LIST_WORK_BYTES;
use crate::scan::v2::splits::chunk_starts;
use crate::scan::v2::splits::projection_splits;
use crate::scan::v2::splits::selection_splits;
use crate::scan::v2::splits::sparse_indices;
use crate::segments::SegmentSource;

/// Set to `1` to run supported scans on the planner-driven executor.
pub const ENV_VAR: &str = "VORTEX_SCAN_V2";

/// Whether supported scans run on the planner-driven executor.
pub fn enabled() -> bool {
    static ENABLED: LazyLock<bool> = LazyLock::new(|| env::var(ENV_VAR).is_ok_and(|v| v == "1"));
    *ENABLED
}

/// The parts of a file the planner-driven executor reads through.
#[derive(Clone)]
pub struct ScanFile {
    /// The root layout the scan builder's reader was opened over.
    pub layout: LayoutRef,
    /// The location of every segment, indexed by segment id.
    pub locations: Arc<[SegmentLocation]>,
    /// Fetches segment bytes by id.
    pub segments: Arc<dyn SegmentSource>,
    preparation: Option<Arc<FilePreparation>>,
}

struct FilePreparation {
    layout: LayoutRef,
    locations: Arc<[SegmentLocation]>,
    segments: Arc<dyn SegmentSource>,
    segment_bytes: u64,
    projection_supported: OnceCell<bool>,
    identity_supported: OnceCell<bool>,
    whole_identity_support: WholeSupport,
    root: OnceCell<PlanRef>,
    ranges: OnceCell<SegmentRanges>,
    values: OnceCell<ValueCache>,
}

// Retain each keyed layout so its address cannot be reused for a different support answer.
type WholeSupport = Mutex<HashMap<usize, (LayoutRef, bool)>>;

impl ScanFile {
    /// Creates a file description without retaining preparation between scans.
    pub fn new(
        layout: LayoutRef,
        locations: Arc<[SegmentLocation]>,
        segments: Arc<dyn SegmentSource>,
    ) -> Self {
        Self {
            layout,
            locations,
            segments,
            preparation: None,
        }
    }

    /// Shares immutable layout support checks and plans, segment locations, and whole dictionary
    /// values between this file's scans. Only identity projections retain dictionary values;
    /// selections and expression results remain owned by each scan.
    pub fn with_caching(mut self) -> Self {
        self.preparation = Some(Arc::new(FilePreparation {
            layout: Arc::clone(&self.layout),
            locations: Arc::clone(&self.locations),
            segments: Arc::clone(&self.segments),
            segment_bytes: self
                .locations
                .iter()
                .map(|location| u64::from(location.length))
                .sum(),
            projection_supported: OnceCell::new(),
            identity_supported: OnceCell::new(),
            whole_identity_support: Mutex::default(),
            root: OnceCell::new(),
            ranges: OnceCell::new(),
            values: OnceCell::new(),
        }));
        self
    }

    fn layout_preparation(&self) -> Option<&FilePreparation> {
        self.preparation
            .as_deref()
            .filter(|cache| Arc::ptr_eq(&cache.layout, &self.layout))
    }

    fn supports(&self, identity: bool) -> VortexResult<bool> {
        let Some(cache) = self.layout_preparation() else {
            return supports_layout(&self.layout, identity);
        };
        let supported = if identity {
            &cache.identity_supported
        } else {
            &cache.projection_supported
        };
        supported
            .get_or_try_init(|| supports_layout(&self.layout, identity))
            .copied()
    }

    fn supports_scan(
        &self,
        identity: bool,
        selection: &Selection,
        rows: Option<&Range<u64>>,
    ) -> VortexResult<bool> {
        if identity {
            if self
                .layout_preparation()
                .and_then(|cache| cache.identity_supported.get())
                .is_some_and(|supported| *supported)
            {
                return Ok(true);
            }
            let rows = rows.cloned().unwrap_or(0..self.layout.row_count());
            if let Some(indices) = sparse_indices(selection, &rows) {
                // A selection-specific answer cannot be cached as support for the whole file.
                return supports_selected_layout(
                    &self.layout,
                    &indices,
                    0,
                    self.layout_preparation()
                        .map(|cache| &cache.whole_identity_support),
                );
            }
        }
        self.supports(identity)
    }

    fn lower(&self) -> VortexResult<PlanRef> {
        match self.layout_preparation() {
            Some(cache) => cache.root.get_or_try_init(|| lower(&self.layout)).cloned(),
            None => lower(&self.layout),
        }
    }

    fn ranges(&self) -> SegmentRanges {
        match self
            .preparation
            .as_deref()
            .filter(|cache| Arc::ptr_eq(&cache.locations, &self.locations))
        {
            Some(cache) => Arc::clone(cache.ranges.get_or_init(|| segment_ranges(&self.locations))),
            None => segment_ranges(&self.locations),
        }
    }

    fn values(&self) -> ValueCache {
        self.layout_preparation()
            .filter(|cache| {
                Arc::ptr_eq(&cache.locations, &self.locations)
                    && Arc::ptr_eq(&cache.segments, &self.segments)
            })
            .map(|cache| cache.values.get_or_init(ValueCache::default).clone())
            .unwrap_or_default()
    }

    fn segment_bytes(&self) -> u64 {
        self.preparation
            .as_deref()
            .filter(|cache| Arc::ptr_eq(&cache.locations, &self.locations))
            .map(|cache| cache.segment_bytes)
            .unwrap_or_else(|| {
                self.locations
                    .iter()
                    .map(|location| u64::from(location.length))
                    .sum()
            })
    }
}

/// The executor a streaming scan runs on.
pub(crate) enum Executor<A: 'static + Send> {
    /// The default executor, over the layout reader.
    V1(ScanBuilder<A>),
    /// The planner-driven executor, prepared.
    V2(PreparedScan<A>),
}

/// Picks the executor for `builder` and prepares the scan when it is this one.
pub(crate) fn select<A: 'static + Send>(builder: ScanBuilder<A>) -> VortexResult<Executor<A>> {
    if !enabled() {
        return Ok(Executor::V1(builder));
    }
    select_supported(builder)
}

/// Prepares `builder` on this executor when it supports the scan.
fn select_supported<A: 'static + Send>(builder: ScanBuilder<A>) -> VortexResult<Executor<A>> {
    let declined = if builder.filter.is_some() {
        Some("filter")
    } else if builder.limit.is_some() {
        Some("limit")
    } else {
        match &builder.scan_file {
            None => Some("no scan file"),
            Some(file)
                if !file.supports_scan(
                    builder.projection.is_root(),
                    &builder.selection,
                    builder.row_range.as_ref(),
                )? =>
            {
                Some("layout")
            }
            Some(_) => None,
        }
    };
    if let Some(reason) = declined {
        tracing::debug!(target: "vortex_layout::scan::v2", reason, "scan runs on v1");
        return Ok(Executor::V1(builder));
    }
    tracing::debug!(target: "vortex_layout::scan::v2", "scan runs on v2");
    Ok(Executor::V2(PreparedScan::try_new(builder)?))
}

/// Whether every required layout lowers to plans with a pipeline implementation.
fn supports_layout(layout: &LayoutRef, identity: bool) -> VortexResult<bool> {
    if !(layout.is::<Flat>()
        || layout.is::<Chunked>()
        || layout.is::<Struct>()
        || layout.is::<List>()
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
    if let Some(chunked) = layout.as_opt::<Chunked>() {
        for index in 0..chunked.nchildren() {
            // Lists and dictionaries need a support check over a different row domain. Flat
            // chunks are supported by their registered vtable alone; keep their metadata lazy
            // until the selection actually reaches them.
            if chunked
                .children()
                .child_encoding(index)
                .is_some_and(|encoding| encoding.is::<Flat>())
            {
                continue;
            }
            let child = chunked.children().child(index, chunked.dtype())?;
            if !supports_layout(&child, identity)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    for child in layout.children()? {
        if !supports_layout(&child, identity)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Checks only the row chunks an identity projection can reach. Dictionary values and lists
/// have different row domains, so their required children retain the full support check.
fn supports_selected_layout(
    layout: &LayoutRef,
    indices: &[u64],
    offset: u64,
    whole: Option<&WholeSupport>,
) -> VortexResult<bool> {
    if layout.is::<Flat>() {
        return Ok(true);
    }
    if let Some(chunked) = layout.as_opt::<Chunked>() {
        let offsets = chunked.chunk_offsets();
        let mut first = 0;
        while first < indices.len() {
            let chunk = offsets
                .partition_point(|&start| start <= indices[first] - offset)
                .saturating_sub(1);
            let end = offset
                + offsets
                    .get(chunk + 1)
                    .ok_or_else(|| vortex_err!("Selected row exceeds chunked layout"))?;
            let last = indices.partition_point(|&index| index < end);
            let child = layout
                .slot(chunk)?
                .ok_or_else(|| vortex_err!("Missing selected chunk"))?;
            if !supports_selected_layout(
                &child,
                &indices[first..last],
                offset + offsets[chunk],
                whole,
            )? {
                return Ok(false);
            }
            first = last;
        }
        return Ok(true);
    }
    if layout.is::<Zoned>() || layout.is::<LegacyStats>() {
        return match layout.slot(0)? {
            Some(data) => supports_selected_layout(&data, indices, offset, whole),
            None => Ok(false),
        };
    }
    if layout.is::<Dict>() {
        let values = layout
            .slot(0)?
            .ok_or_else(|| vortex_err!("Missing dictionary values"))?;
        let codes = layout
            .slot(1)?
            .ok_or_else(|| vortex_err!("Missing dictionary codes"))?;
        return Ok(supports_whole_identity(&values, whole)?
            && supports_selected_layout(&codes, indices, offset, whole)?);
    }
    if layout.is::<List>() {
        return supports_whole_identity(layout, whole);
    }
    if layout.is::<Struct>() {
        for child in layout.children()? {
            if !supports_selected_layout(&child, indices, offset, whole)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    Ok(false)
}

/// Unlike a selected-chunk check, this answer is independent of the selected rows.
fn supports_whole_identity(layout: &LayoutRef, cache: Option<&WholeSupport>) -> VortexResult<bool> {
    let Some(cache) = cache else {
        return supports_layout(layout, true);
    };
    let key = Arc::as_ptr(layout).cast::<()>() as usize;
    if let Some((_, supported)) = cache.lock().get(&key) {
        return Ok(*supported);
    }
    let supported = supports_layout(layout, true)?;
    cache.lock().insert(key, (Arc::clone(layout), supported));
    Ok(supported)
}

/// A scan planned over its file, ready to run its splits.
pub(crate) struct PreparedScan<A: 'static + Send> {
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
    /// Conservative task count derived from the largest estimated list-element working set.
    list_concurrency: Option<usize>,
}

impl<A: 'static + Send> PreparedScan<A> {
    /// Plans the projection and groups the selected rows, reusing immutable file preparation
    /// when available. Full scans use projection boundaries; sparse scans visit selected chunks.
    fn try_new(builder: ScanBuilder<A>) -> VortexResult<Self> {
        let file = builder
            .scan_file
            .ok_or_else(|| vortex_err!("a V2 scan needs a scan file"))?;
        let root = file.lower()?;
        let row_count = root.row_count();
        // An identity projection needs no rewrites. Keep the lowered children lazy, including
        // statistics that a projection-only scan never reads.
        let identity = builder.projection.is_root() && builder.projection.dtype() == root.dtype();
        let projection = if identity {
            root
        } else {
            optimize(plan_row_idx_expression(builder.projection, root)?)?
        };
        let rows = builder.row_range.unwrap_or(0..row_count);
        let parallelism = get_available_parallelism().unwrap_or(1);
        let sparse = selection_splits(&builder.selection, &rows, &projection, parallelism)?;
        let list_concurrency = sparse
            .as_ref()
            .filter(|splits| splits.max_list_bytes != 0)
            .map(|splits| (LIST_WORK_BYTES / splits.max_list_bytes).max(1) as usize);
        let sparse_selection = sparse.is_some();
        // Small segments spread across tasks can cause overlapping coalesced reads. Large
        // leaves already amortize read setup without an additional planning pass.
        let segment_bytes = file.segment_bytes();
        let register_reads =
            sparse_selection && segment_bytes / (file.locations.len().max(1) as u64) < 256 * 1024;
        let mut value_scopes = (identity && sparse.is_none()).then(Vec::new);
        // Sparse coordinates already bound the work. The projection compiler descends only
        // into overlapping chunks, so discovering every chunk would defeat lazy lowering.
        let projection_starts: Arc<[u64]> = if sparse.is_some() {
            Arc::from([])
        } else {
            chunk_starts([&projection], value_scopes.as_mut())?.into()
        };
        // Whole-column dictionaries already need the scan's full lifetime. Only dictionaries
        // covering part of the scan need separate cache owners for their consuming splits.
        let value_scopes = value_scopes.filter(|values| {
            values
                .iter()
                .any(|(used, _)| used.start > rows.start || used.end < rows.end)
        });
        let splits = if let Some(sparse) = sparse {
            sparse.ranges
        } else if let Some(boundaries) = builder.natural_splits {
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
        let ranges = file.ranges();
        let values = if identity {
            file.values()
        } else {
            ValueCache::default()
        };
        // On one core, a few sparse tasks otherwise pay separate read setup for neighboring
        // segments. Register them together while keeping compute and buffer lifetimes per task.
        // With more workers, independent requests preserve parallel reads.
        let register_small_scan = parallelism == 1
            && sparse_selection
            && !register_reads
            && (2..=4).contains(&splits.len());
        let segments = if register_reads || register_small_scan {
            let mut segments = Vec::new();
            for rows in &splits {
                let selected = builder.selection.row_mask(rows);
                let reach = Reach::Selected {
                    offset: 0,
                    rows: rows.clone(),
                    mask: selected.mask().clone(),
                };
                projection.reach(rows.clone(), &reach, &mut |id, _| segments.push(id))?;
            }
            segments.sort_unstable();
            segments.dedup();
            let selected_bytes = segments
                .iter()
                .map(|id| {
                    file.locations
                        .get(**id as usize)
                        .map(|location| u64::from(location.length))
                        .ok_or_else(|| vortex_err!("segment {id} has no known location"))
                })
                .sum::<VortexResult<u64>>()?;
            if register_small_scan || selected_bytes >= segment_bytes.div_ceil(2) {
                register_segments(file.segments, segments)
            } else {
                file.segments
            }
        } else {
            file.segments
        };
        let io = Arc::new(SegmentScanIo::new(segments, ranges));
        Ok(Self {
            session: builder.session.clone(),
            plans: ScanPlans {
                session: builder.session,
                locations: file.locations,
                projection,
                projection_starts,
                row_offset: builder.row_offset,
                values,
            },
            value_scopes,
            io,
            splits,
            selection: builder.selection,
            map_fn: builder.map_fn,
            ordered: builder.ordered,
            concurrency: builder.concurrency,
            list_concurrency,
        })
    }

    /// The splits with selected rows, each ready to be admitted to a planning run.
    fn split_plans(&self) -> Vec<SplitPlan> {
        let selection = Arc::new(self.selection.clone());
        let plans = self.plans.clone();
        let project = next_fn(move |selected: SelectedRows| {
            Ok(ProjectionPlanner::new(plans.clone(), selected))
        });
        self.splits
            .iter()
            .filter_map(|rows| {
                if self.selection.row_count_in_range(rows) == 0 {
                    return None;
                }
                let scope = WorkScope {
                    file_ordinal: 0,
                    rows: rows.clone(),
                };
                let project = if let Some(values) = &self.value_scopes {
                    let mut plans = self.plans.clone();
                    plans.values = self.plans.values.for_plans(
                        values
                            .iter()
                            .filter(|(used, _)| used.start < rows.end && rows.start < used.end)
                            .map(|(_, plan)| plan),
                    );
                    next_fn(move |selected: SelectedRows| {
                        Ok(ProjectionPlanner::new(plans.clone(), selected))
                    })
                } else {
                    Arc::clone(&project)
                };
                Some(SplitPlan {
                    root: Box::new(SelectionPlanner::new(
                        scope.clone(),
                        Arc::clone(&selection),
                        project,
                    )),
                    scope,
                })
            })
            .collect()
    }

    /// Runs splits concurrently and yields their mapped batches.
    ///
    /// Like the default executor, up to `concurrency` splits per thread run at once, and their
    /// batches are yielded in row order when the scan is ordered. With multiple cores, a lone
    /// split runs on the consumer's task; a single core keeps compute with the runtime's IO.
    pub(crate) fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let parallelism = get_available_parallelism().unwrap_or(1);
        let concurrency = self.concurrency * parallelism;
        // Retain admission slots until their output is consumed, so buffered ordered results
        // count against the same estimate. A split larger than the budget runs alone.
        let concurrency = self
            .list_concurrency
            .map_or(concurrency, |limit| concurrency.min(limit));
        let mut split_plans = self.split_plans();
        let io = self.io;
        let map_fn = self.map_fn;
        if self.plans.projection_starts.is_empty()
            && split_plans
                .iter()
                .any(|split| split.scope.rows.end - split.scope.rows.start > 1)
        {
            if parallelism > 1 && split_plans.len() == 1 {
                let split = split_plans
                    .pop()
                    .ok_or_else(|| vortex_err!("Missing single split"))?;
                return Ok(split.stream(io, move |array| map_fn(array)));
            }
            let handle = self.session.handle();
            let streams = stream::iter(split_plans).map(move |split| {
                let io = Arc::clone(&io);
                let map_fn = Arc::clone(&map_fn);
                split.spawn_stream(io, move |array| map_fn(array), &handle)
            });
            return Ok(if self.ordered {
                streams
                    .map(|stream| future::ready(VortexResult::Ok(stream)))
                    .buffered(concurrency)
                    .try_flatten()
                    .boxed()
            } else {
                streams.flatten_unordered(concurrency).boxed()
            });
        }
        if parallelism > 1 && split_plans.len() == 1 {
            let split = split_plans
                .pop()
                .ok_or_else(|| vortex_err!("Missing single split"))?;
            return Ok(
                stream::once(async move { split.run(io, |array| map_fn(array)).await })
                    .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
                    .try_flatten()
                    .boxed(),
            );
        }
        let handle = self.session.handle();
        // Spawned as the buffer pulls them, so at most `concurrency` splits read at once.
        let tasks = stream::iter(split_plans).map(move |split| {
            let io = Arc::clone(&io);
            let map_fn = Arc::clone(&map_fn);
            handle.spawn(async move { split.run(io, |array| map_fn(array)).await })
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
