// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Selection-specific planning without changing the pipeline or planner contracts.

use std::ops::Range;

use vortex_array::EmptyMetadata;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_mask::Mask;
use vortex_session::registry::CachedId;

use super::Concat;
use super::Eval;
use super::LegacyStats;
use super::Pack;
use super::Plan;
use super::PlanId;
use super::PlanParts;
use super::PlanRef;
use super::PlanVTable;
use super::Take;
use super::Zoned;
use super::pipeline::Chain;
use super::pipeline::Compiler;
use super::pipeline::Reach;
use super::pipeline::overlapping;
use crate::segments::SegmentId;

#[derive(Clone, Debug)]
struct Selected;

#[derive(Clone, Debug)]
struct SelectedData {
    rows: Range<u64>,
    mask: Mask,
}

/// Binds a projection's selection before segment discovery can initialize unselected children.
/// Whole-value dependencies retain their original plans and cache identities.
pub(crate) fn selected(plan: PlanRef, rows: Range<u64>, mask: Mask) -> PlanRef {
    if mask.all_true() {
        return plan;
    }
    PlanParts {
        vtable: Selected,
        dtype: plan.dtype().clone(),
        row_count: plan.row_count(),
        children: vec![plan].into(),
        data: SelectedData { rows, mask },
    }
    .into_typed()
    .into_plan()
}

impl PlanVTable for Selected {
    type PlanData = SelectedData;
    type Metadata = EmptyMetadata;

    fn id(&self) -> PlanId {
        static ID: CachedId = CachedId::new("vortex.plan.selected");
        *ID
    }

    fn metadata(_plan: &Plan<Self>) -> Option<Self::Metadata> {
        None
    }

    fn compile(
        plan: &Plan<Self>,
        rows: Range<u64>,
        mask: &Mask,
        compiler: &mut Compiler<'_>,
    ) -> VortexResult<Option<Chain>> {
        let selected = plan.data().mask(&rows)?;
        let mask = mask & &selected;
        if mask.all_false() {
            return Ok(None);
        }
        compiler.compile(&plan.child_required(0)?, rows, &mask)
    }

    fn reach(
        plan: &Plan<Self>,
        rows: Range<u64>,
        at: &Reach,
        visit: &mut dyn FnMut(SegmentId, Range<u64>),
    ) -> VortexResult<()> {
        let mask = plan.data().mask(&rows)?;
        reach_selected(&plan.child_required(0)?, rows, &mask, at, visit)
    }
}

impl SelectedData {
    fn mask(&self, rows: &Range<u64>) -> VortexResult<Mask> {
        vortex_ensure!(
            rows.start >= self.rows.start && rows.end <= self.rows.end,
            "Selected plan requested outside its bound row range"
        );
        Ok(self.mask.slice(
            usize::try_from(rows.start - self.rows.start)?
                ..usize::try_from(rows.end - self.rows.start)?,
        ))
    }
}

/// Trims a child range to selected rows, without allocating a mask for an empty or contiguous
/// selection. The returned range remains relative to the input mask.
pub(crate) fn selected_slice(mask: &Mask, rows: Range<usize>) -> Option<(Range<usize>, Mask)> {
    if rows.is_empty() {
        return None;
    }
    match mask {
        Mask::AllFalse(_) => None,
        Mask::AllTrue(_) => {
            let len = rows.end - rows.start;
            Some((rows, Mask::new_true(len)))
        }
        Mask::Values(values) => {
            let bits = values.bit_buffer().slice(rows.clone());
            let first = bits.set_indices().next()?;
            let end = bits.last_set_index()? + 1;
            if rows.start + first == 0 && rows.start + end == mask.len() {
                // Keep the derived indices shared across fields when no rows were trimmed.
                return Some((0..mask.len(), mask.clone()));
            }
            let len = end - first;
            let selected = if bits.count_range(first, end) == len {
                Mask::new_true(len)
            } else {
                Mask::from_buffer(bits.slice(first..end))
            };
            Some((rows.start + first..rows.start + end, selected))
        }
    }
}

fn reach_selected(
    plan: &PlanRef,
    rows: Range<u64>,
    mask: &Mask,
    at: &Reach,
    visit: &mut dyn FnMut(SegmentId, Range<u64>),
) -> VortexResult<()> {
    if mask.all_false() || rows.is_empty() {
        return Ok(());
    }
    if mask.all_true() {
        return plan.reach(rows, at, visit);
    }
    if let Some(concat) = plan.as_opt::<Concat>() {
        for (index, start, local) in overlapping(concat, &rows) {
            let Some((span, mask)) = selected_slice(
                mask,
                usize::try_from(local.start - rows.start)?
                    ..usize::try_from(local.end - rows.start)?,
            ) else {
                continue;
            };
            reach_selected(
                &concat.child_required(index)?,
                rows.start + span.start as u64 - start..rows.start + span.end as u64 - start,
                &mask,
                &at.shift(start),
                visit,
            )?;
        }
        return Ok(());
    }
    if plan.is::<Pack>() || plan.is::<Eval>() {
        for child in plan.children().iter() {
            reach_selected(&child?, rows.clone(), mask, at, visit)?;
        }
        return Ok(());
    }
    if let Some(take) = plan.as_opt::<Take>() {
        let values = take.values()?;
        values.reach(0..values.row_count(), &at.fixed(&rows), visit)?;
        return reach_selected(&take.codes()?, rows, mask, at, visit);
    }
    if let Some(zoned) = plan.as_opt::<Zoned>()
        && !zoned.is_pruning()
    {
        return match zoned.data_plan()? {
            Some(data) => reach_selected(&data, rows, mask, at, visit),
            None => Ok(()),
        };
    }
    if let Some(stats) = plan.as_opt::<LegacyStats>() {
        return reach_selected(&stats.data_plan()?, rows, mask, at, visit);
    }
    // Filters over non-segment children consume every input row. Share, pruning, and unknown
    // operators may also read whole dependencies under a sparse mask.
    plan.reach(rows, at, visit)
}
