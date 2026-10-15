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
use std::iter;
use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::selection::Selection;
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
use crate::scan::planning::ScanPlans;
pub use crate::scan::planning::SegmentLocation;
use crate::scan::planning::plan_projection;
use crate::scan::scan_builder::ScanBuilder;
use crate::scan::v2::io::SegmentScanIo;
use crate::scan::v2::io::segment_ranges;
use crate::scan::v2::split::SplitPlan;
use crate::scan::v2::splits::chunk_starts;
use crate::scan::v2::splits::projection_splits;
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
            Some(file) if !supports_layout(&file.layout, builder.projection.is_root())? => {
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
}

impl<A: 'static + Send> PreparedScan<A> {
    /// Lowers the file's layout, plans the projection over it, and cuts the scan's row range
    /// where the projection's chunks start.
    fn try_new(builder: ScanBuilder<A>) -> VortexResult<Self> {
        let file = builder
            .scan_file
            .ok_or_else(|| vortex_err!("a V2 scan needs a scan file"))?;
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
    pub(crate) fn execute_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
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
