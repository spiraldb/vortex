// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Bounded grouping and admission of list element work.

use std::ops::Range;

use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::plan::physical::Concat;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::Zoned;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::overlapping;

const MAX_ELEMENT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_MASK_FRAGMENT_BYTES: u64 = 32 * 1024;

/// Combine nearby ranges into a mask, keeping holes out of the output without a scan per hole.
/// Only dense fixed-width spans of at most 8 MiB are grouped. Large contiguous fragments keep
/// their slice path: applying a mask costs more than the scan setup it saves for those ranges.
pub(super) fn element_splits(dtype: &DType, ranges: Vec<Range<u64>>) -> VortexResult<Vec<Split>> {
    if ranges.len() < 2 {
        return Ok(ranges.into_iter().map(Split::all).collect());
    }
    let width = match dtype {
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Bool(_) => 1,
        _ => return Ok(ranges.into_iter().map(Split::all).collect()),
    };
    let mut splits = Vec::new();
    let mut first = 0;
    while first < ranges.len() {
        let start = ranges[first].start;
        let mut selected = ranges[first].end - start;
        let mut end = first + 1;
        while let Some(next) = ranges.get(end) {
            let span = next.end - start;
            let validity = if dtype.is_nullable() {
                span.div_ceil(8)
            } else {
                0
            };
            let bytes = span.saturating_mul(width).saturating_add(validity);
            let total = selected + (next.end - next.start);
            if bytes > MAX_ELEMENT_BYTES || total < span.div_ceil(2) {
                break;
            }
            selected = total;
            end += 1;
        }
        if end == first + 1
            || selected
                .saturating_mul(width)
                .div_ceil((end - first) as u64)
                > MAX_MASK_FRAGMENT_BYTES
        {
            splits.extend(ranges[first..end].iter().cloned().map(Split::all));
        } else {
            let rows = start..ranges[end - 1].end;
            let slices = ranges[first..end]
                .iter()
                .map(|range| {
                    Ok((
                        usize::try_from(range.start - start)?,
                        usize::try_from(range.end - start)?,
                    ))
                })
                .collect::<VortexResult<Vec<_>>>()?;
            let mask = Mask::from_slices(usize::try_from(rows.end - start)?, slices);
            splits.push(Split { rows, mask });
        }
        first = end;
    }
    Ok(splits)
}

/// Overlap grouped rows only when all touched chunks fit the decoded-element estimate.
/// This limits additional range concurrency, not total heap use or coalesced IO buffers.
pub(super) fn can_overlap(
    elements: &PlanRef,
    ranges: &[Range<u64>],
    selected: usize,
    parallelism: usize,
) -> VortexResult<bool> {
    if parallelism <= 1 || ranges.len() <= 1 || selected / ranges.len() < 2 {
        return Ok(false);
    }
    let mut bytes = 0_u64;
    for range in ranges {
        let Some(estimate) = range_bytes(elements, range)? else {
            return Ok(false);
        };
        // Charging a shared chunk again is conservative and avoids a per-scan set.
        bytes = bytes.saturating_add(estimate);
        if bytes > MAX_ELEMENT_BYTES {
            return Ok(false);
        }
    }
    Ok(true)
}

fn range_bytes(plan: &PlanRef, rows: &Range<u64>) -> VortexResult<Option<u64>> {
    if let Some(filter) = plan.as_opt::<Filter>() {
        return range_bytes(&filter.child_plan()?, rows);
    }
    if let Some(zoned) = plan.as_opt::<Zoned>() {
        return match zoned.data_plan()? {
            Some(data) => range_bytes(&data, rows),
            None => Ok(None),
        };
    }
    if let Some(stats) = plan.as_opt::<LegacyStats>() {
        return range_bytes(&stats.data_plan()?, rows);
    }
    if let Some(concat) = plan.as_opt::<Concat>() {
        let mut bytes = 0_u64;
        for (index, start, local) in overlapping(concat, rows) {
            let Some(estimate) = range_bytes(
                &concat.child_required(index)?,
                &(local.start - start..local.end - start),
            )?
            else {
                return Ok(None);
            };
            bytes = bytes.saturating_add(estimate);
            if bytes > MAX_ELEMENT_BYTES {
                break;
            }
        }
        return Ok(Some(bytes));
    }
    if !plan.is::<SegmentScan>() {
        return Ok(None);
    }
    let width = match plan.dtype() {
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Bool(_) => 1,
        _ => return Ok(None),
    };
    let validity = if plan.dtype().is_nullable() {
        plan.row_count().div_ceil(8)
    } else {
        0
    };
    Ok(Some(
        plan.row_count()
            .saturating_mul(width)
            .saturating_add(validity),
    ))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::dtype::PType;
    use vortex_mask::AllOr;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::plan::physical::ConcatPlan;
    use crate::plan::physical::LegacyStatsPlan;
    use crate::plan::physical::SegmentScanPlan;
    use crate::segments::SegmentId;

    fn segment(rows: u64, dtype: DType) -> PlanRef {
        SegmentScanPlan::new(dtype, rows, SegmentId::from(0), ReadContext::new([]), None)
            .into_plan()
    }

    #[rstest]
    fn dense_ranges_use_relative_masks(#[values(0, u64::MAX - 20)] base: u64) -> VortexResult<()> {
        let splits = element_splits(
            &DType::Primitive(PType::I64, true.into()),
            vec![base + 1..base + 5, base + 7..base + 9, base + 12..base + 13],
        )?;
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].rows, base + 1..base + 13);
        assert_eq!(
            splits[0].mask.slices(),
            AllOr::Some(&[(0, 4), (6, 8), (11, 12)][..])
        );
        Ok(())
    }

    #[test]
    fn sparse_gaps_and_unknown_widths_stay_separate() -> VortexResult<()> {
        let ranges = vec![0..2, 100..104, 105..109];
        let splits = element_splits(&DType::Bool(false.into()), ranges.clone())?;
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].rows, 0..2);
        assert_eq!(splits[1].rows, 100..109);
        assert_eq!(splits[1].mask.true_count(), 8);
        let unknown = element_splits(&DType::Utf8(false.into()), ranges.clone())?;
        assert_eq!(
            unknown
                .iter()
                .map(|split| split.rows.clone())
                .collect::<Vec<_>>(),
            ranges
        );
        Ok(())
    }

    #[rstest]
    #[case::at_limit(MAX_ELEMENT_BYTES / 8, false, 1)]
    #[case::over_limit(MAX_ELEMENT_BYTES / 8 + 2, false, 2)]
    #[case::validity_counts(MAX_ELEMENT_BYTES / 8, true, 2)]
    fn grouping_bounds_the_mask_span(
        #[case] rows: u64,
        #[case] nullable: bool,
        #[case] count: usize,
    ) -> VortexResult<()> {
        let ranges = (0..rows)
            .step_by(1024)
            .map(|start| start + u64::from(start != 0)..(start + 1024).min(rows))
            .collect::<Vec<_>>();
        let selected = rows - (ranges.len() as u64 - 1);
        let splits = element_splits(&DType::Primitive(PType::I64, nullable.into()), ranges)?;
        assert_eq!(splits.len(), count);
        assert_eq!(
            splits
                .iter()
                .map(|split| split.mask.true_count())
                .sum::<usize>(),
            usize::try_from(selected)?
        );
        Ok(())
    }

    #[test]
    fn large_fragments_keep_contiguous_slices() -> VortexResult<()> {
        let ranges = vec![0..8192, 8193..16384];
        let splits = element_splits(&DType::Primitive(PType::I64, false.into()), ranges.clone())?;
        assert_eq!(
            splits
                .iter()
                .map(|split| split.rows.clone())
                .collect::<Vec<_>>(),
            ranges
        );
        assert!(
            splits
                .iter()
                .all(|split| matches!(split.mask, Mask::AllTrue(_)))
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn contiguous_ranges_need_no_mask_allocation() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I64, false.into());
        assert!(element_splits(&dtype, vec![])?.is_empty());
        let splits = element_splits(&dtype, vec![0..MAX_ELEMENT_BYTES])?;
        assert!(matches!(splits[0].mask, Mask::AllTrue(_)));
        Ok(())
    }

    #[rstest]
    #[case::grouped(4, 30, true)]
    #[case::isolated(2, 30, false)]
    #[case::single_worker(4, 1, false)]
    fn overlap_requires_grouped_parallel_work(
        #[case] selected: usize,
        #[case] workers: usize,
        #[case] expected: bool,
    ) -> VortexResult<()> {
        let elements = segment(1024, DType::Primitive(PType::I64, false.into()));
        assert_eq!(
            can_overlap(&elements, &[0..2, 10..12], selected, workers)?,
            expected
        );
        Ok(())
    }

    #[rstest]
    #[case::within_limit(MAX_ELEMENT_BYTES / 16, false, true)]
    #[case::large_chunk(MAX_ELEMENT_BYTES / 8, false, false)]
    #[case::validity_counts(MAX_ELEMENT_BYTES / 16, true, false)]
    fn charges_whole_chunks_and_validity(
        #[case] rows: u64,
        #[case] nullable: bool,
        #[case] expected: bool,
    ) -> VortexResult<()> {
        let elements = segment(rows, DType::Primitive(PType::I64, nullable.into()));
        assert_eq!(can_overlap(&elements, &[0..2, 10..12], 4, 30)?, expected);
        Ok(())
    }

    #[test]
    fn skips_untouched_chunks_and_zone_data() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I64, false.into());
        let data = ConcatPlan::try_new(
            dtype.clone(),
            vec![
                segment(1024, dtype.clone()),
                segment(10_000_000, dtype.clone()),
            ],
        )?
        .into_plan();
        let elements =
            LegacyStatsPlan::new(data, segment(10_000_000, dtype), 1024, [].into()).into_plan();
        assert!(can_overlap(&elements, &[0..2, 10..12], 4, 30)?);
        assert!(!can_overlap(&elements, &[0..2, 1024..1026], 4, 30)?);
        Ok(())
    }

    #[test]
    fn unknown_width_and_overflow_stay_sequential() -> VortexResult<()> {
        let unknown = segment(10, DType::Utf8(false.into()));
        let overflow = segment(u64::MAX, DType::Primitive(PType::I64, true.into()));
        for plan in [unknown, overflow] {
            assert!(!can_overlap(&plan, &[0..2, 5..7], 4, 30)?);
        }
        Ok(())
    }
}
