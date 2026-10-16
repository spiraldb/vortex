// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Splits a scan from its plans rather than from the layout reader.
//!
//! Full scans use the projected columns' chunk boundaries. Sparse scans group selected rows
//! using a projected column's chunks, leaving the chunk plans lazy.

use std::borrow::Cow;
use std::iter;
use std::ops::Range;

use itertools::Itertools;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_scan::selection::Selection;

use crate::plan::physical::Concat;
use crate::plan::physical::Eval;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::ListPack;
use crate::plan::physical::Pack;
use crate::plan::physical::PlanRef;
use crate::plan::physical::Take;
use crate::plan::physical::Zoned;

struct Partition {
    plan: PlanRef,
    list_elements: bool,
}

/// Sparse outer-row splits and the largest estimated list-element working set of a split.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SparseSplits {
    pub(super) ranges: Vec<Range<u64>>,
    pub(super) max_list_bytes: u64,
}

/// Maximum estimated element work admitted concurrently by one scan.
pub(super) const LIST_WORK_BYTES: u64 = 32 * 1024 * 1024;

/// Groups sparse selections using projected chunk boundaries. Choose the partition closest to
/// four tasks per worker, without visiting the chunk children.
pub(super) fn selection_splits(
    selection: &Selection,
    rows: &Range<u64>,
    projection: &PlanRef,
    parallelism: usize,
) -> VortexResult<Option<SparseSplits>> {
    let Some(indices) = sparse_indices(selection, rows) else {
        return Ok(None);
    };
    if indices.is_empty() {
        return Ok(Some(SparseSplits {
            ranges: Vec::new(),
            max_list_bytes: 0,
        }));
    }
    vortex_ensure!(
        indices
            .last()
            .is_some_and(|&index| index < projection.row_count()),
        "Selected row exceeds the projection row count"
    );
    if let [index] = indices.as_ref() {
        return Ok(Some(SparseSplits {
            ranges: vec![*index..index + 1],
            max_list_bytes: 0,
        }));
    }
    let mut partitions = Vec::new();
    collect_partitions(projection, false, &mut partitions)?;
    let target = parallelism.max(1).saturating_mul(4);
    let mut best = None;
    for partition in &partitions {
        let plan = &partition.plan;
        if plan.row_count() == 0 || (partition.list_elements && parallelism <= 1) {
            continue;
        }
        let ranges = if partition.list_elements {
            selected_list_chunks(&indices, plan, projection.row_count())
        } else {
            selected_chunks(&indices, plan.as_::<Concat>().row_offsets())
        };
        let score = ranges.len().abs_diff(target);
        if best.as_ref().is_none_or(|(previous, _)| score < *previous) {
            best = Some((score, ranges));
        }
    }
    let ranges = best.map_or_else(|| selected_chunks(&indices, &[0]), |(_, ranges)| ranges);
    let max_list_bytes = if partitions.iter().any(|partition| partition.list_elements) {
        ranges
            .iter()
            .map(|range| {
                let start = indices.partition_point(|&index| index < range.start);
                let end = indices.partition_point(|&index| index < range.end);
                partitions
                    .iter()
                    .filter(|partition| partition.list_elements)
                    .map(|partition| {
                        estimate_list_bytes(
                            &indices[start..end],
                            &partition.plan,
                            projection.row_count(),
                        )
                    })
                    .fold(0_u64, u64::saturating_add)
            })
            .max()
            .unwrap_or(0)
    } else {
        0
    };
    Ok(Some(SparseSplits {
        ranges,
        max_list_bytes,
    }))
}

/// Sparse included indices inside the scan's row range, borrowing the sorted buffer when possible.
pub(super) fn sparse_indices<'a>(
    selection: &'a Selection,
    rows: &Range<u64>,
) -> Option<Cow<'a, [u64]>> {
    let count = selection.row_count_in_range(rows);
    if count == 0 {
        return Some(Cow::Borrowed(&[]));
    }
    if count > rows.end.saturating_sub(rows.start) / 16 {
        return None;
    }
    Some(match selection {
        Selection::IncludeByIndex(indices) => {
            let start = indices.partition_point(|&index| index < rows.start);
            let end = indices.partition_point(|&index| index < rows.end);
            Cow::Borrowed(&indices[start..end])
        }
        Selection::IncludeRoaring(indices) => {
            let mut indices = indices.iter();
            indices.advance_to(rows.start);
            Cow::Owned(indices.take_while(|&index| index < rows.end).collect())
        }
        _ => return None,
    })
}

/// Stops at the first concatenation of each projected column. Its children can remain lazy.
fn collect_partitions(
    plan: &PlanRef,
    list_elements: bool,
    partitions: &mut Vec<Partition>,
) -> VortexResult<()> {
    if plan.is::<Concat>() {
        partitions.push(Partition {
            plan: plan.clone(),
            list_elements,
        });
    } else if let Some(zoned) = plan.as_opt::<Zoned>() {
        if let Some(data) = zoned.data_plan()? {
            collect_partitions(&data, list_elements, partitions)?;
        }
    } else if let Some(legacy) = plan.as_opt::<LegacyStats>() {
        collect_partitions(&legacy.data_plan()?, list_elements, partitions)?;
    } else if let Some(take) = plan.as_opt::<Take>() {
        collect_partitions(&take.codes()?, list_elements, partitions)?;
    } else if let Some(list) = plan.as_opt::<ListPack>() {
        collect_partitions(&list.elements()?, true, partitions)?;
    } else if plan.is::<Pack>() || plan.is::<Filter>() || plan.is::<Eval>() {
        for child in plan.children().iter_refs() {
            collect_partitions(child?, list_elements, partitions)?;
        }
    } else if list_elements {
        partitions.push(Partition {
            plan: plan.clone(),
            list_elements,
        });
    }
    Ok(())
}

fn selected_chunks(indices: &[u64], offsets: &[u64]) -> Vec<Range<u64>> {
    let mut ranges: Vec<Range<u64>> = Vec::new();
    let mut previous = None;
    for &index in indices {
        let chunk = offsets
            .partition_point(|&offset| offset <= index)
            .saturating_sub(1);
        if previous == Some(chunk) {
            if let Some(range) = ranges.last_mut() {
                range.end = index + 1;
            }
        } else {
            ranges.push(index..index + 1);
            previous = Some(chunk);
        }
    }
    ranges
}

/// Uses relative element positions as task-sizing hints; execution resolves exact list offsets.
fn selected_list_chunks(indices: &[u64], elements: &PlanRef, row_count: u64) -> Vec<Range<u64>> {
    // Tiny element chunks do not amortize a separate root task and offsets read.
    // Group selected primitive chunks into roughly 2 MiB of uncompressed work.
    let min_rows = match elements.dtype() {
        DType::Primitive(ptype, _) => (2 * 1024 * 1024 / ptype.byte_width()) as u64,
        _ => 0,
    };
    let offsets = elements
        .as_opt::<Concat>()
        .map_or(&[0][..], |concat| concat.row_offsets());
    let mut ranges: Vec<Range<u64>> = Vec::new();
    let mut previous = None;
    let mut element_rows = 0_u64;
    for &index in indices {
        // floor(boundary * row_count / element_count) <= index, inverted so only
        // selected rows need mapping rather than every element chunk boundary.
        let element_index = ((u128::from(index) + 1) * u128::from(elements.row_count()) - 1)
            / u128::from(row_count);
        let chunk = offsets
            .partition_point(|&offset| u128::from(offset) <= element_index)
            .saturating_sub(1);
        if (previous == Some(chunk) || element_rows < min_rows)
            && let Some(last) = ranges.last_mut()
        {
            last.end = index + 1;
        } else {
            ranges.push(index..index + 1);
            element_rows = 0;
        }
        if previous != Some(chunk) {
            let end = offsets
                .get(chunk + 1)
                .copied()
                .unwrap_or(elements.row_count());
            element_rows = element_rows.saturating_add(end - offsets[chunk]);
        }
        previous = Some(chunk);
    }
    ranges
}

fn estimate_list_bytes(indices: &[u64], elements: &PlanRef, row_count: u64) -> u64 {
    if indices.is_empty() || elements.row_count() == 0 {
        return 0;
    }
    let width = match elements.dtype() {
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Bool(_) => 1,
        DType::Null => return 0,
        // Without a fixed-width estimate, admit only one such task at a time.
        _ => return LIST_WORK_BYTES,
    };
    let offsets = elements
        .as_opt::<Concat>()
        .map_or(&[0][..], |concat| concat.row_offsets());
    let mut previous = None;
    let mut bytes = 0_u64;
    for &index in indices {
        let element_index = ((u128::from(index) + 1) * u128::from(elements.row_count()) - 1)
            / u128::from(row_count);
        let chunk = offsets
            .partition_point(|&offset| u128::from(offset) <= element_index)
            .saturating_sub(1);
        if previous != Some(chunk) {
            let end = offsets
                .get(chunk + 1)
                .copied()
                .unwrap_or(elements.row_count());
            bytes = bytes.saturating_add((end - offsets[chunk]).saturating_mul(width));
            previous = Some(chunk);
        }
    }
    bytes
}

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
pub(crate) fn projection_splits(starts: &[u64], rows: Range<u64>) -> Vec<Range<u64>> {
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
    use vortex_buffer::Buffer;
    use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::plan::physical::ConcatPlan;
    use crate::plan::physical::ListPackPlan;
    use crate::plan::physical::SegmentScanPlan;
    use crate::plan::physical::TakePlan;
    use crate::segments::SegmentId;

    #[rstest]
    #[case::nearby_clusters(vec![10, 11, 5010, 5011], 0..100_000, Some(vec![10..12, 5010..5012]))]
    #[case::clipped(vec![10, 11, 5010, 5011], 11..5011, Some(vec![11..12, 5010..5011]))]
    #[case::empty(vec![], 0..100, Some(vec![]))]
    #[case::dense((0..100).collect(), 0..100, None)]
    #[case::last(vec![u64::MAX - 1], u64::MAX - 100..u64::MAX, Some(vec![u64::MAX - 1..u64::MAX]))]
    fn sparse_selection_ranges(
        #[case] indices: Vec<u64>,
        #[case] rows: Range<u64>,
        #[case] expected: Option<Vec<Range<u64>>>,
        #[values(false, true)] roaring: bool,
    ) -> VortexResult<()> {
        let selection = if roaring {
            Selection::IncludeRoaring(indices.into_iter().collect())
        } else {
            Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from(indices))?)
        };
        assert_eq!(
            selection_splits(&selection, &rows, &partition(&[4000, u64::MAX - 4000])?, 32)?
                .map(|splits| splits.ranges),
            expected
        );
        Ok(())
    }

    fn partition(chunks: &[u64]) -> VortexResult<PlanRef> {
        let dtype = DType::Primitive(PType::I32, false.into());
        let plans = chunks
            .iter()
            .enumerate()
            .map(|(id, &rows)| {
                SegmentScanPlan::new(
                    dtype.clone(),
                    rows,
                    SegmentId::from(id as u32),
                    ReadContext::new([]),
                    None,
                )
                .into_plan()
            })
            .collect();
        Ok(ConcatPlan::try_new(dtype, plans)?.into_plan())
    }

    #[test]
    fn rejects_selected_rows_beyond_the_plan() -> VortexResult<()> {
        let selection = Selection::IncludeRoaring([100].into_iter().collect());
        let error = selection_splits(&selection, &(0..200), &partition(&[100])?, 1)
            .expect_err("selected row is outside the plan");
        assert!(error.to_string().contains("projection row count"));
        Ok(())
    }

    #[rstest]
    #[case(1, 2)]
    #[case(32, 10)]
    fn groups_for_available_workers(
        #[case] workers: usize,
        #[case] expected_chunks: usize,
        #[values(0, 1, 2)] list_depth: u32,
    ) -> VortexResult<()> {
        let selected = Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from_iter(
            (0..10).map(|i| i * 1_000_000 + 10),
        ))?);
        let coarse = partition(&[5_000_000; 2])?;
        let mut row_count = 10_000_000 * 4_u64.pow(list_depth);
        let mut fine = partition(&[row_count / 10; 10])?;
        for _ in 0..list_depth {
            row_count /= 4;
            fine = list_partition(fine, row_count)?;
        }
        let fields = vortex_array::dtype::StructFields::from_iter([
            ("coarse", coarse.dtype().clone()),
            ("fine", fine.dtype().clone()),
        ]);
        let plan = crate::plan::physical::PackPlan::try_new(
            fields,
            false.into(),
            10_000_000,
            vec![coarse, fine],
            None,
        )?
        .into_plan();
        let splits = selection_splits(&selected, &(0..10_000_000), &plan, workers)?
            .ok_or_else(|| vortex_error::vortex_err!("expected sparse splits"))?;
        assert_eq!(splits.ranges.len(), expected_chunks);
        assert_eq!(
            splits
                .ranges
                .iter()
                .map(|range| selected.row_count_in_range(range))
                .sum::<u64>(),
            10
        );
        Ok(())
    }

    fn list_partition(elements: PlanRef, row_count: u64) -> VortexResult<PlanRef> {
        Ok(ListPackPlan::try_new(
            false.into(),
            row_count,
            elements,
            partition(&[row_count + 1])?,
            None,
        )?
        .into_plan())
    }

    #[rstest]
    fn small_list_chunks_keep_the_coarser_projection_partition(
        #[values(1, 30)] workers: usize,
    ) -> VortexResult<()> {
        let selected = Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from_iter(
            (0..100).map(|i| i * 10_000),
        ))?);
        let ids = partition(&[125_000; 8])?;
        let lists = list_partition(partition(&[16_384; 640])?, 1_000_000)?;
        let fields = vortex_array::dtype::StructFields::from_iter([
            ("id", ids.dtype().clone()),
            ("list", lists.dtype().clone()),
        ]);
        let expected = selection_splits(&selected, &(0..1_000_000), &ids, workers)?
            .map(|splits| splits.ranges);
        let plan = crate::plan::physical::PackPlan::try_new(
            fields,
            false.into(),
            1_000_000,
            vec![ids, lists],
            None,
        )?
        .into_plan();
        assert_eq!(
            selection_splits(&selected, &(0..1_000_000), &plan, workers)?
                .map(|splits| splits.ranges),
            expected
        );
        Ok(())
    }

    #[rstest]
    #[case::empty_elements(vec![0, 0], 1000, vec![10, 500], vec![10..501])]
    #[case::coincident_boundaries(vec![1, 1, 998], 100, vec![0, 50], vec![0..51])]
    #[case::large_coordinates(
        vec![u64::MAX / 2, u64::MAX - u64::MAX / 2],
        u64::MAX - 1,
        vec![1, u64::MAX / 2],
        vec![1..2, u64::MAX / 2..u64::MAX / 2 + 1],
    )]
    fn list_partitions_cover_selected_rows(
        #[case] chunks: Vec<u64>,
        #[case] row_count: u64,
        #[case] indices: Vec<u64>,
        #[case] expected: Vec<Range<u64>>,
    ) -> VortexResult<()> {
        let plan = list_partition(partition(&chunks)?, row_count)?;
        let selection =
            Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from(indices))?);
        assert_eq!(
            selection_splits(&selection, &(0..row_count), &plan, 30)?.map(|splits| splits.ranges),
            Some(expected)
        );
        Ok(())
    }

    #[test]
    fn list_work_counts_unique_chunks_across_projected_lists() -> VortexResult<()> {
        let elements = partition(&[1_000_000; 2])?;
        let list = list_partition(elements, 1000)?;
        let fields = vortex_array::dtype::StructFields::from_iter([
            ("a", list.dtype().clone()),
            ("b", list.dtype().clone()),
        ]);
        let plan = crate::plan::physical::PackPlan::try_new(
            fields,
            false.into(),
            1000,
            vec![list.clone(), list],
            None,
        )?
        .into_plan();
        let selection = Selection::IncludeRoaring([10, 20, 510].into_iter().collect());
        let splits = selection_splits(&selection, &(0..1000), &plan, 30)?
            .ok_or_else(|| vortex_error::vortex_err!("expected sparse splits"))?;
        assert_eq!(splits.ranges, vec![10..21, 510..511]);
        // Two I32 columns each touch one 4 MB chunk per split, even for two selected rows.
        assert_eq!(splits.max_list_bytes, 8_000_000);
        Ok(())
    }

    #[test]
    fn unchunked_list_elements_have_a_work_estimate() -> VortexResult<()> {
        let elements = partition(&[10_000_000])?.child_required(0)?;
        let list = list_partition(elements, 1000)?;
        let selection = Selection::IncludeRoaring([10, 510].into_iter().collect());
        let splits = selection_splits(&selection, &(0..1000), &list, 30)?
            .ok_or_else(|| vortex_error::vortex_err!("expected sparse splits"))?;
        assert_eq!(splits.ranges, vec![10..511]);
        assert_eq!(splits.max_list_bytes, 40_000_000);
        Ok(())
    }

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
