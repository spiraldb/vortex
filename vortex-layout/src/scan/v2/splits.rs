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
pub(super) fn chunk_starts<'a>(
    plans: impl IntoIterator<Item = &'a PlanRef>,
) -> VortexResult<Vec<u64>> {
    let mut starts = Vec::new();
    for plan in plans {
        collect_starts(plan, 0, &mut starts)?;
    }
    starts.sort_unstable();
    starts.dedup();
    Ok(starts)
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
    } else if let Some(stats) = plan.as_opt::<LegacyStats>() {
        collect_starts(&stats.data_plan()?, offset, starts)?;
    } else if plan.is::<Take>() {
        collect_starts(&plan.child_required(0)?, offset, starts)?;
    } else if plan.is::<Pack>() || plan.is::<Filter>() || plan.is::<Eval>() {
        for child in plan.children().iter() {
            collect_starts(&child?, offset, starts)?;
        }
    }
    Ok(())
}

/// The projection splits of `rows`: cut at every one of `starts` inside it.
pub(crate) fn projection_splits(starts: &[u64], rows: Range<u64>) -> Vec<Range<u64>> {
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

    use super::*;

    #[rstest]
    #[case::whole(&[], 10..20, vec![10..20])]
    #[case::cut(&[0, 12, 15, 30], 10..20, vec![10..12, 12..15, 15..20])]
    #[case::start_on_boundary(&[10, 15], 10..20, vec![10..15, 15..20])]
    #[case::empty(&[10], 5..5, vec![])]
    fn projection(
        #[case] starts: &[u64],
        #[case] rows: Range<u64>,
        #[case] expected: Vec<Range<u64>>,
    ) {
        assert_eq!(projection_splits(starts, rows), expected);
    }
}
