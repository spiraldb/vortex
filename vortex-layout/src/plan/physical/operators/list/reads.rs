// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reads selected list elements straight from their segments, in the list's own pipeline.
//!
//! Element ranges are known only once the offsets arrive, so they cannot be compiled with the
//! rest of the split. When the elements are plain segment chunks, the list source issues their
//! reads itself instead of compiling a nested scan: each touched segment is read once and decoded
//! on arrival, and the ranges it holds are sliced out of it in list order.

use std::ops::Range;

use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_session::VortexSession;

use super::super::scan::decode;
use super::super::scan::select;
use crate::plan::physical::Concat;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::SegmentScanPlan;
use crate::plan::physical::Zoned;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::overlapping;
use crate::segments::SegmentId;

/// The selected element rows of one segment chunk.
struct Piece {
    slice: Option<Range<usize>>,
    filter: Option<Mask>,
}

/// A requested segment and the pieces, by output position, that read it.
struct Pending {
    plan: SegmentScanPlan,
    pieces: Vec<usize>,
}

/// The element reads of one list source, from request to the ordered element chunks.
pub(super) struct ElementReads {
    pieces: Vec<Piece>,
    output: Vec<Option<ArrayRef>>,
    pending: FxHashMap<SegmentId, Pending>,
    /// Segments in first-use order, so reads are published in element order.
    order: Vec<SegmentId>,
}

impl ElementReads {
    /// Plans the reads for `splits` of `elements`, or `None` when any selected range reaches a
    /// plan other than segment chunks, which then needs a nested scan.
    pub(super) fn try_new(elements: &PlanRef, splits: &[Split]) -> VortexResult<Option<Self>> {
        let mut reads = Self {
            pieces: Vec::new(),
            output: Vec::new(),
            pending: FxHashMap::default(),
            order: Vec::new(),
        };
        for split in splits {
            if !reads.collect(elements, split.rows.clone(), &split.mask)? {
                return Ok(None);
            }
        }
        reads.output = (0..reads.pieces.len()).map(|_| None).collect();
        Ok(Some(reads))
    }

    fn collect(&mut self, plan: &PlanRef, rows: Range<u64>, mask: &Mask) -> VortexResult<bool> {
        if mask.all_false() {
            return Ok(true);
        }
        // A filter keeps the rows of the mask it runs with, which this piece applies itself.
        if let Some(filter) = plan.as_opt::<Filter>() {
            return self.collect(&filter.child_plan()?, rows, mask);
        }
        if let Some(zoned) = plan.as_opt::<Zoned>() {
            if zoned.is_pruning() {
                return Ok(false);
            }
            return match zoned.data_plan()? {
                Some(data) => self.collect(&data, rows, mask),
                None => Ok(false),
            };
        }
        if let Some(stats) = plan.as_opt::<LegacyStats>() {
            return self.collect(&stats.data_plan()?, rows, mask);
        }
        if let Some(concat) = plan.as_opt::<Concat>() {
            for (index, start, local) in overlapping(concat, &rows) {
                let mask = mask.slice(
                    usize::try_from(local.start - rows.start)?
                        ..usize::try_from(local.end - rows.start)?,
                );
                if !self.collect(
                    &concat.child_required(index)?,
                    local.start - start..local.end - start,
                    &mask,
                )? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let Some(scan) = plan.as_opt::<SegmentScan>() else {
            return Ok(false);
        };
        let slice = if rows.start == 0 && rows.end == scan.row_count() {
            None
        } else {
            Some(usize::try_from(rows.start)?..usize::try_from(rows.end)?)
        };
        let filter = (!mask.all_true()).then(|| mask.clone());
        let segment = scan.segment_id();
        let piece = self.pieces.len();
        self.pieces.push(Piece { slice, filter });
        self.pending
            .entry(segment)
            .or_insert_with(|| {
                self.order.push(segment);
                Pending {
                    plan: scan.clone(),
                    pieces: Vec::new(),
                }
            })
            .pieces
            .push(piece);
        Ok(true)
    }

    /// Publishes one read per touched segment.
    pub(super) fn request(&mut self, cx: &mut Cx<'_>) {
        for segment in self.order.drain(..) {
            cx.request_read(segment);
        }
    }

    /// Decodes an arrived segment and narrows it to each piece that reads it.
    pub(super) fn deliver(
        &mut self,
        segment: SegmentId,
        bytes: BufferHandle,
        session: &VortexSession,
    ) -> VortexResult<()> {
        let pending = self
            .pending
            .remove(&segment)
            .ok_or_else(|| vortex_err!("Unexpected list element segment {segment}"))?;
        let array = decode(&pending.plan, bytes, session)?;
        for piece in pending.pieces {
            let Piece { slice, filter } = &self.pieces[piece];
            self.output[piece] = Some(select(array.clone(), slice.as_ref(), filter.as_ref())?);
        }
        Ok(())
    }

    /// Whether every requested segment has arrived.
    pub(super) fn is_done(&self) -> bool {
        self.pending.is_empty()
    }

    /// The element chunks in list order.
    pub(super) fn finish(self) -> VortexResult<Vec<ArrayRef>> {
        vortex_ensure!(
            self.is_done(),
            "List elements finished before every read arrived"
        );
        self.output
            .into_iter()
            .map(|array| array.ok_or_else(|| vortex_err!("Missing list element chunk")))
            .collect()
    }
}
