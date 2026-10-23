// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Finding the segments a plan reads without compiling it.
//!
//! [`visit_segments`] walks a plan tree over a row range. Each plan describes how a child's rows
//! relate to its own through [`PlanVTable::child_rows`](super::PlanVTable::child_rows), the
//! [`SegmentScan`] leaf names its segment, and nothing else is operator-specific.

use std::ops::Range;

use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::PlanRef;
use super::SegmentScan;
use super::selection::selected_slice;
use super::selection::selection_mask;
use crate::segments::SegmentId;

/// How a child's rows relate to its parent's rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildRows {
    /// The child holds these rows of the parent: child row `r` is parent row `rows.start + r`.
    Rows(Range<u64>),
    /// The child is read whole whatever rows of the parent are read, as dictionary values are
    /// for their codes.
    Whole,
    /// The child's rows have no row-wise relation to the parent's, so the walk does not enter
    /// it. Its segments are read by each reader alone and never found ahead of compiling.
    Opaque,
}

/// The rows of the scanned plan a visited plan's rows are read for.
#[derive(Clone, Debug)]
enum RootRows {
    /// Row `r` is scanned row `r + offset`.
    Offset(u64),
    /// Every row is read for these scanned rows.
    Whole(Range<u64>),
}

impl RootRows {
    fn root(&self, rows: &Range<u64>) -> Range<u64> {
        match self {
            RootRows::Offset(offset) => rows.start + offset..rows.end + offset,
            RootRows::Whole(root) => root.clone(),
        }
    }

    fn shift(&self, by: u64) -> Self {
        match self {
            RootRows::Offset(offset) => RootRows::Offset(offset + by),
            RootRows::Whole(root) => RootRows::Whole(root.clone()),
        }
    }

    fn whole(&self, rows: &Range<u64>) -> Self {
        RootRows::Whole(self.root(rows))
    }
}

/// Visits every segment compiling `plan` over `rows` could read, with the rows of `plan` it is
/// read for, so a segment more than one reader reads can be decoded once and reads can be
/// announced before any pipeline exists.
///
/// With a `mask` over `rows`, chunks the mask selects nothing of are skipped, as compiling under
/// that mask would skip them. A child read whole is visited in full under any mask. Compiling
/// does no IO and neither does this.
pub fn visit_segments(
    plan: &PlanRef,
    rows: Range<u64>,
    mask: Option<&Mask>,
    visit: &mut dyn FnMut(SegmentId, Range<u64>),
) -> VortexResult<()> {
    walk(plan, rows, mask, &RootRows::Offset(0), visit)
}

fn walk(
    plan: &PlanRef,
    rows: Range<u64>,
    mask: Option<&Mask>,
    at: &RootRows,
    visit: &mut dyn FnMut(SegmentId, Range<u64>),
) -> VortexResult<()> {
    if rows.start >= rows.end || mask.is_some_and(Mask::all_false) {
        return Ok(());
    }
    if let Some(scan) = plan.as_opt::<SegmentScan>() {
        visit(scan.segment_id(), at.root(&rows));
        return Ok(());
    }
    // A selection narrows the mask for everything beneath it.
    let selected = selection_mask(plan, &rows)?.map(|selected| match mask {
        Some(mask) => mask & &selected,
        None => selected,
    });
    let mask = selected.as_ref().or(mask);
    for index in 0..plan.children().len() {
        match plan.child_rows(index) {
            ChildRows::Opaque => {}
            ChildRows::Whole => {
                let child = plan.child_required(index)?;
                walk(&child, 0..child.row_count(), None, &at.whole(&rows), visit)?;
            }
            ChildRows::Rows(span) => {
                let local = rows.start.max(span.start)..rows.end.min(span.end);
                if local.start >= local.end {
                    continue;
                }
                let child_rows = local.start - span.start..local.end - span.start;
                let Some(mask) = mask else {
                    let child = plan.child_required(index)?;
                    walk(&child, child_rows, None, &at.shift(span.start), visit)?;
                    continue;
                };
                let Some((trimmed, mask)) = selected_slice(
                    mask,
                    usize::try_from(local.start - rows.start)?
                        ..usize::try_from(local.end - rows.start)?,
                ) else {
                    continue;
                };
                let child = plan.child_required(index)?;
                let trimmed = rows.start + trimmed.start as u64 - span.start
                    ..rows.start + trimmed.end as u64 - span.start;
                let mask = (!mask.all_true()).then_some(&mask);
                walk(&child, trimmed, mask, &at.shift(span.start), visit)?;
            }
        }
    }
    Ok(())
}
