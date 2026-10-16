// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Layout-level planners and morsels for the planning protocol.
//!
//! A [`ProjectionPlanner`] turns each split's selected rows into projection morsels. Single-row
//! projections over flat leaves read and assemble those leaves directly; other projections use
//! the pipeline [`Scan`](crate::plan::physical::pipeline::Scan). Every segment read is published
//! as a protocol request, so the planning driver decides when IO happens.

pub(crate) mod graph;
mod point;
mod projection;

use std::sync::Arc;

pub use projection::ProjectionPlanner;
use vortex_buffer::Alignment;
use vortex_io::request::IoTarget;
pub use vortex_scan::planning::selection::SelectedRows;
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
    /// Sorted projection boundaries in the root row domain. Empty when sparse selection has
    /// already grouped the work and unselected chunk plans should remain lazy.
    pub projection_starts: Arc<[u64]>,
    /// The global row index of the root row domain's first row.
    pub row_offset: u64,
    /// Whole values, such as dictionary values, shared by the splits of this scan. Their plans
    /// always read the same complete row domain, independently of a split's selection.
    pub values: ValueCache,
}
