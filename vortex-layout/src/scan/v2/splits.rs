// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Splits a scan from its plans rather than from the layout reader.
//!
//! A scan is cut where the chunks of the columns its projection reads start, so each split reads
//! at most one chunk of every column.

use std::iter;
use std::ops::Range;

use itertools::Itertools;
use vortex_error::VortexResult;

use crate::plan::physical::Concat;
use crate::plan::physical::Eval;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::Pack;
use crate::plan::physical::PlanRef;
use crate::plan::physical::Take;
use crate::plan::physical::Zoned;

/// The row positions where the chunks of `plans` start, over their shared row domain, sorted and
/// without duplicates.
///
/// This follows plans whose children cover the same rows, or known parts of them, and stops at any
/// other plan: a take's values cover other rows, and zone statistics cover zones.
/// For a lowered identity projection, `values` also records the root rows that need each whole
/// dictionary, so its cached value can be released after those splits finish.
pub(super) fn chunk_starts<'a>(
    plans: impl IntoIterator<Item = &'a PlanRef>,
    mut values: Option<&mut Vec<(Range<u64>, PlanRef)>>,
) -> VortexResult<Vec<u64>> {
    let mut starts = Vec::new();
    for plan in plans {
        collect_starts(plan, 0, &mut starts, values.as_deref_mut())?;
    }
    starts.sort_unstable();
    starts.dedup();
    Ok(starts)
}

fn collect_starts(
    plan: &PlanRef,
    offset: u64,
    starts: &mut Vec<u64>,
    mut values: Option<&mut Vec<(Range<u64>, PlanRef)>>,
) -> VortexResult<()> {
    if let Some(concat) = plan.as_opt::<Concat>() {
        for (index, &child_offset) in concat.row_offsets().iter().enumerate() {
            starts.push(offset + child_offset);
            collect_starts(
                &plan.child_required(index)?,
                offset + child_offset,
                starts,
                values.as_deref_mut(),
            )?;
        }
    } else if let Some(zoned) = plan.as_opt::<Zoned>() {
        if let Some(data) = zoned.data_plan()? {
            collect_starts(&data, offset, starts, values)?;
        }
    } else if let Some(legacy) = plan.as_opt::<LegacyStats>() {
        collect_starts(&legacy.data_plan()?, offset, starts, values)?;
    } else if plan.is::<Take>() {
        if let Some(values) = values.as_deref_mut() {
            let whole = plan.child_required(1)?;
            let rows = offset..offset + plan.row_count();
            values.push((rows.clone(), whole.clone()));
            collect_nested_values(&whole, &rows, values)?;
        }
        collect_starts(&plan.child_required(0)?, offset, starts, values)?;
    } else if plan.is::<Pack>() || plan.is::<Filter>() || plan.is::<Eval>() {
        for child in plan.children().iter_refs() {
            collect_starts(child?, offset, starts, values.as_deref_mut())?;
        }
    }
    Ok(())
}

/// A dictionary's whole values can contain further dictionaries. They are needed over the
/// outer dictionary's row domain, regardless of their own row counts.
fn collect_nested_values(
    plan: &PlanRef,
    rows: &Range<u64>,
    values: &mut Vec<(Range<u64>, PlanRef)>,
) -> VortexResult<()> {
    if let Some(zoned) = plan.as_opt::<Zoned>() {
        if let Some(data) = zoned.data_plan()? {
            collect_nested_values(&data, rows, values)?;
        }
    } else if let Some(legacy) = plan.as_opt::<LegacyStats>() {
        collect_nested_values(&legacy.data_plan()?, rows, values)?;
    } else {
        if plan.is::<Take>() {
            values.push((rows.clone(), plan.child_required(1)?));
        }
        for child in plan.children().iter_refs() {
            collect_nested_values(child?, rows, values)?;
        }
    }
    Ok(())
}

/// The projection splits of `rows`: cut at every one of `starts` inside it.
pub(in crate::scan) fn projection_splits(starts: &[u64], rows: Range<u64>) -> Vec<Range<u64>> {
    if rows.is_empty() {
        return Vec::new();
    }
    let first = starts.partition_point(|&start| start <= rows.start);
    let last = starts.partition_point(|&start| start < rows.end);
    iter::once(rows.start)
        .chain(starts[first..last].iter().copied())
        .chain(iter::once(rows.end))
        .tuple_windows()
        .map(|(start, end)| start..end)
        .filter(|range| !range.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::PType;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::plan::physical::ConcatPlan;
    use crate::plan::physical::SegmentScanPlan;
    use crate::plan::physical::TakePlan;
    use crate::segments::SegmentId;

    #[rstest]
    #[case::whole(&[], 10..20, vec![10..20])]
    #[case::cut(&[0, 12, 15, 30], 10..20, vec![10..12, 12..15, 15..20])]
    #[case::start_on_boundary(&[10, 15], 10..20, vec![10..15, 15..20])]
    #[case::empty(&[10], 5..5, vec![])]
    #[case::empty_on_boundary(&[5], 5..5, vec![])]
    fn projection(
        #[case] starts: &[u64],
        #[case] rows: Range<u64>,
        #[case] expected: Vec<Range<u64>>,
    ) {
        assert_eq!(projection_splits(starts, rows), expected);
    }

    #[test]
    fn dictionary_values_use_the_outer_codes_row_domain() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I32, false.into());
        let segment = |id, rows| {
            SegmentScanPlan::new(
                dtype.clone(),
                rows,
                SegmentId::from(id),
                ReadContext::new([]),
                None,
            )
            .into_plan()
        };
        let inner_values = segment(0_u32, 3);
        let outer_values = TakePlan::new(segment(1, 2), inner_values.clone()).into_plan();
        let dict = TakePlan::new(segment(2, 4), outer_values.clone()).into_plan();
        let plan = ConcatPlan::try_new(dtype.clone(), vec![segment(3, 5), dict])?.into_plan();
        let mut values = Vec::new();
        assert_eq!(chunk_starts([&plan], Some(&mut values))?, vec![0, 5]);
        assert_eq!(values.len(), 2);
        for ((rows, actual), expected) in values.iter().zip([outer_values, inner_values]) {
            assert_eq!(rows, &(5..9));
            assert!(PlanRef::ptr_eq(actual, &expected));
        }
        Ok(())
    }
}
