// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Split selected child projections at stored segment boundaries before admitting them.

use std::ops::Range;

use rustc_hash::FxHashSet;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;

use super::ChildWork;
use crate::plan::physical::Concat;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::Zoned;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::overlapping;
use crate::segments::SegmentId;

const MAX_ELEMENT_BYTES: u64 = 8 * 1024 * 1024;
const CHILD_TARGET_BYTES: u64 = 256 * 1024;

#[cfg(test)]
mod tests;

/// Divide substantial fixed-width selections at stored segment boundaries. Each segment is
/// owned by at most one task, so independent execution cannot duplicate its IO and decoding.
pub(in crate::scan::execution) fn parallel_splits(
    plan: &PlanRef,
    splits: &[Split],
) -> VortexResult<Option<Vec<ChildWork>>> {
    let width = match plan.dtype() {
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Bool(_) => 1,
        _ => return Ok(None),
    };
    let selected = splits.iter().fold(0_u64, |total, split| {
        total.saturating_add((split.mask.true_count() as u64).saturating_mul(width))
    });
    if selected < CHILD_TARGET_BYTES * 2 {
        return Ok(None);
    }
    let mut seen = FxHashSet::default();
    let mut work = Vec::new();
    for split in splits {
        let mut leaves = Vec::new();
        if !selected_leaves(plan, split, 0, &mut seen, &mut leaves)? {
            return Ok(None);
        }
        let mut first = 0;
        while first < leaves.len() {
            let start = leaves[first].0.start;
            let mut end = leaves[first].0.end;
            let mut bytes = leaves[first].1;
            first += 1;
            while bytes < CHILD_TARGET_BYTES
                && first < leaves.len()
                && bytes.saturating_add(leaves[first].1) <= MAX_ELEMENT_BYTES
            {
                end = leaves[first].0.end;
                bytes += leaves[first].1;
                first += 1;
            }
            work.push(ChildWork {
                split: Split {
                    rows: start..end,
                    mask: split.mask.slice(
                        usize::try_from(start - split.rows.start)?
                            ..usize::try_from(end - split.rows.start)?,
                    ),
                },
                bytes,
            });
        }
    }
    Ok((work.len() > 1).then_some(work))
}

fn selected_leaves(
    plan: &PlanRef,
    split: &Split,
    offset: u64,
    seen: &mut FxHashSet<SegmentId>,
    leaves: &mut Vec<(Range<u64>, u64)>,
) -> VortexResult<bool> {
    if split.mask.all_false() {
        return Ok(true);
    }
    if let Some(filter) = plan.as_opt::<Filter>() {
        return selected_leaves(&filter.child_plan()?, split, offset, seen, leaves);
    }
    if let Some(zoned) = plan.as_opt::<Zoned>() {
        return match zoned.data_plan()? {
            Some(data) => selected_leaves(&data, split, offset, seen, leaves),
            None => Ok(false),
        };
    }
    if let Some(stats) = plan.as_opt::<LegacyStats>() {
        return selected_leaves(&stats.data_plan()?, split, offset, seen, leaves);
    }
    if let Some(concat) = plan.as_opt::<Concat>() {
        for (index, start, rows) in overlapping(concat, &split.rows) {
            let mask = split.mask.slice(
                usize::try_from(rows.start - split.rows.start)?
                    ..usize::try_from(rows.end - split.rows.start)?,
            );
            // Do not lower an untouched lazy child, including unknown layouts in holes.
            if mask.all_false() {
                continue;
            }
            let child = Split {
                rows: rows.start - start..rows.end - start,
                mask,
            };
            if !selected_leaves(
                &concat.child_required(index)?,
                &child,
                offset + start,
                seen,
                leaves,
            )? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    let Some(segment) = plan.as_opt::<SegmentScan>() else {
        return Ok(false);
    };
    let width = match plan.dtype() {
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Bool(_) => 1,
        _ => return Ok(false),
    };
    let validity = if plan.dtype().is_nullable() {
        plan.row_count().div_ceil(8)
    } else {
        0
    };
    let bytes = plan
        .row_count()
        .saturating_mul(width)
        .saturating_add(validity);
    if bytes == 0 || bytes > MAX_ELEMENT_BYTES || !seen.insert(segment.segment_id()) {
        return Ok(false);
    }
    leaves.push((offset + split.rows.start..offset + split.rows.end, bytes));
    Ok(true)
}
