// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Layout-level planners and morsels for the planning protocol.
//!
//! A split is evaluated over physical plans: a [`ProjectionPlanner`] emits a [`ProjectionMorsel`]
//! per projection split, which runs the projection plan over the split's selected rows through
//! the pipeline [`Scan`](crate::plan::physical::pipeline::Scan). Every segment read is published
//! as a protocol request, so the planning driver decides when IO happens.

pub(crate) mod graph;
mod projection;

use std::sync::Arc;

pub use projection::ProjectionPlanner;
use vortex_buffer::Alignment;
use vortex_io::request::IoTarget;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::WorkScope;
use vortex_session::VortexSession;

use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::ValueCache;

/// Where a segment's bytes live in its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentLocation {
    /// Byte offset of the segment from the start of the source.
    pub offset: u64,
    /// Length of the segment in bytes.
    pub length: u32,
    /// Alignment the segment's bytes must have once delivered.
    pub alignment: Alignment,
}

impl SegmentLocation {
    /// The byte range a read of the segment asks for.
    pub fn target(&self) -> IoTarget {
        IoTarget::Range {
            offset: self.offset,
            len: self.length as usize,
            alignment: self.alignment,
        }
    }
}

/// What every split of a scan shares: the projection plan and where its segments live.
#[derive(Clone)]
pub struct ScanPlans {
    /// The session used for decoding and expression evaluation.
    pub session: VortexSession,
    /// The location of every segment, indexed by segment id.
    pub locations: Arc<[SegmentLocation]>,
    /// The optimized projection plan, over the scan's root row domain.
    pub projection: PlanRef,
    /// Where the chunks the projection reads start, over the root row domain, sorted. A split's
    /// projection is cut at each of these, so every projection split reads at most one chunk of
    /// every column.
    pub projection_starts: Arc<[u64]>,
    /// The global row index of the root row domain's first row.
    pub row_offset: u64,
    /// Whole values, such as dictionary values, shared by every split of the scan, so a
    /// dictionary read by many chunks is decoded once, as the default executor's reader caches
    /// it. Dropped with the scan.
    pub values: ValueCache,
}

/// The rows of a split selected for projection.
pub struct SelectedRows {
    /// The split the rows belong to.
    pub scope: WorkScope,
    /// Which rows of `scope.rows` are selected.
    pub mask: Mask,
}

/// The work for one split: project the rows of `scope` selected by `mask`.
pub fn plan_projection(plans: ScanPlans, scope: WorkScope, mask: Mask) -> Box<dyn Planner> {
    Box::new(ProjectionPlanner::new(plans, SelectedRows { scope, mask }))
}
