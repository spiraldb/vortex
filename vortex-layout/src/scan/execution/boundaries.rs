// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Work boundaries in the projection's row domain.

use std::ops::Range;

use vortex_error::VortexResult;

use crate::plan::physical::Concat;
use crate::plan::physical::Eval;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::Pack;
use crate::plan::physical::PlanRef;
use crate::plan::physical::Take;
use crate::plan::physical::Zoned;
use crate::scan::IDEAL_SPLIT_SIZE;
use crate::scan::split_by::subdivide_large_spans;

/// Cut at every projected column's chunk boundaries, then bound unusually large spans.
pub(super) fn work_boundaries(plan: &PlanRef, rows: Range<u64>) -> VortexResult<Vec<u64>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut boundaries = Vec::new();
    collect_starts(plan, 0, &mut boundaries)?;
    boundaries.retain(|&row| rows.start < row && row < rows.end);
    boundaries.extend([rows.start, rows.end]);
    boundaries.sort_unstable();
    boundaries.dedup();
    Ok(subdivide_large_spans(boundaries, IDEAL_SPLIT_SIZE))
}

fn collect_starts(plan: &PlanRef, offset: u64, starts: &mut Vec<u64>) -> VortexResult<()> {
    if let Some(concat) = plan.as_opt::<Concat>() {
        for (index, &child_offset) in concat.row_offsets().iter().enumerate() {
            starts.push(offset + child_offset);
            collect_starts(&plan.child_required(index)?, offset + child_offset, starts)?;
        }
    } else if let Some(zoned) = plan.as_opt::<Zoned>() {
        if let Some(data) = zoned.data_plan()? {
            collect_starts(&data, offset, starts)?;
        }
    } else if let Some(legacy) = plan.as_opt::<LegacyStats>() {
        collect_starts(&legacy.data_plan()?, offset, starts)?;
    } else if plan.is::<Take>() {
        // Dictionary codes have the output row domain; values have their own domain.
        collect_starts(&plan.child_required(0)?, offset, starts)?;
    } else if plan.is::<Pack>() || plan.is::<Filter>() || plan.is::<Eval>() {
        for child in plan.children().iter_refs() {
            collect_starts(child?, offset, starts)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use vortex_array::dtype::DType;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::StructFields;
    use vortex_error::VortexResult;
    use vortex_session::registry::ReadContext;

    use super::work_boundaries;
    use crate::plan::physical::ConcatPlan;
    use crate::plan::physical::PackPlan;
    use crate::plan::physical::PlanRef;
    use crate::plan::physical::SegmentScanPlan;
    use crate::plan::physical::TakePlan;
    use crate::segments::SegmentId;

    fn segment(id: u32, rows: u64) -> PlanRef {
        SegmentScanPlan::new(
            DType::Primitive(PType::I32, false.into()),
            rows,
            SegmentId::from(id),
            ReadContext::new([]),
            None,
        )
        .into_plan()
    }

    fn concat(children: Vec<PlanRef>) -> VortexResult<PlanRef> {
        Ok(ConcatPlan::try_new(children[0].dtype().clone(), children)?.into_plan())
    }

    #[test]
    fn unions_projected_columns_and_nested_offsets() -> VortexResult<()> {
        let left = concat(vec![segment(0, 3), segment(1, 7)])?;
        let right = concat(vec![
            segment(2, 5),
            concat(vec![segment(3, 2), segment(4, 3)])?,
        ])?;
        let fields = StructFields::from_iter([
            ("left", left.dtype().clone()),
            ("right", right.dtype().clone()),
        ]);
        let plan =
            PackPlan::try_new(fields, false.into(), 10, vec![left, right], None)?.into_plan();
        assert_eq!(work_boundaries(&plan, 0..10)?, [0, 3, 5, 7, 10]);
        assert_eq!(work_boundaries(&plan, 4..9)?, [4, 5, 7, 9]);
        assert!(work_boundaries(&plan, 4..4)?.is_empty());
        Ok(())
    }

    #[test]
    fn ignores_dictionary_value_boundaries() -> VortexResult<()> {
        let codes = concat(vec![segment(0, 4), segment(1, 5)])?;
        let values = concat(vec![segment(2, 2), segment(3, 3)])?;
        let plan = TakePlan::new(codes, values).into_plan();
        assert_eq!(work_boundaries(&plan, 0..9)?, [0, 4, 9]);
        Ok(())
    }

    #[test]
    fn evenly_subdivides_large_gaps() -> VortexResult<()> {
        let plan = concat(vec![segment(0, 60_000), segment(1, 250_000)])?;
        assert_eq!(
            work_boundaries(&plan, 0..310_000)?,
            [0, 60_000, 143_334, 226_668, 310_000]
        );
        Ok(())
    }
}
