// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;
use vortex_array::dtype::PType;
use vortex_session::registry::ReadContext;

use super::*;
use crate::plan::physical::ConcatPlan;
use crate::plan::physical::SegmentScanPlan;

fn chunks(count: u32, rows: u64, dtype: DType, duplicate: bool) -> VortexResult<PlanRef> {
    ConcatPlan::try_new(
        dtype.clone(),
        (0..count)
            .map(|id| {
                SegmentScanPlan::new(
                    dtype.clone(),
                    rows,
                    SegmentId::from(if duplicate { 0 } else { id }),
                    ReadContext::new([]),
                    None,
                )
                .into_plan()
            })
            .collect(),
    )
    .map(|plan| plan.into_plan())
}

#[rstest]
fn contiguous_list_row_splits_at_stored_boundaries(
    #[values(false, true)] nullable: bool,
) -> VortexResult<()> {
    let rows = 8192;
    let plan = chunks(
        32,
        rows,
        DType::Primitive(PType::I64, nullable.into()),
        false,
    )?;
    let jobs = parallel_splits(&plan, &[Split::all(100..rows * 32 - 100)])?
        .expect("large row is parallel");
    assert_eq!(jobs.len(), 8);
    assert_eq!(jobs[0].split.rows, 100..rows * 4);
    assert_eq!(jobs[7].split.rows, rows * 28..rows * 32 - 100);
    assert!(jobs.iter().all(|job| job.split.mask.all_true()));
    assert_eq!(
        jobs.iter().map(|job| job.bytes).sum::<u64>(),
        rows * 32 * 8 + if nullable { rows * 4 } else { 0 }
    );
    Ok(())
}

#[test]
fn masks_preserve_holes_and_do_not_charge_unselected_segments() -> VortexResult<()> {
    let rows = 32768;
    let plan = chunks(5, rows, DType::Primitive(PType::I64, false.into()), false)?;
    let mask = Mask::from_slices(
        usize::try_from(rows * 5)?,
        vec![(0, 32768), (65536, 98304), (131072, 163840)],
    );
    let jobs = parallel_splits(
        &plan,
        &[Split {
            rows: 0..rows * 5,
            mask,
        }],
    )?
    .expect("three selected chunks");
    assert_eq!(
        jobs.iter()
            .map(|job| job.split.rows.clone())
            .collect::<Vec<_>>(),
        vec![0..rows, rows * 2..rows * 3, rows * 4..rows * 5]
    );
    assert_eq!(jobs.iter().map(|job| job.bytes).sum::<u64>(), rows * 3 * 8);
    Ok(())
}

#[test]
fn partial_masks_survive_grouping_and_boundary_clipping() -> VortexResult<()> {
    let plan = chunks(32, 8192, DType::Primitive(PType::I64, false.into()), false)?;
    let mask = Mask::from_iter((0..200_000).map(|i| i % 7 != 0));
    let split = Split {
        rows: 999..200_999,
        mask: mask.clone(),
    };
    let jobs = parallel_splits(&plan, &[split])?.expect("large selected payload");
    let actual: Vec<_> = jobs
        .iter()
        .flat_map(|job| {
            job.split
                .mask
                .iter()
                .enumerate()
                .filter_map(|(i, selected)| selected.then_some(job.split.rows.start + i as u64))
        })
        .collect();
    let expected: Vec<_> = mask
        .iter()
        .enumerate()
        .filter_map(|(i, selected)| selected.then_some(999 + i as u64))
        .collect();
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn repeated_segments_and_repeated_ranges_stay_serial() -> VortexResult<()> {
    let dtype = DType::Primitive(PType::I64, false.into());
    let repeated = chunks(2, 65536, dtype.clone(), true)?;
    assert!(parallel_splits(&repeated, &[Split::all(0..131072)])?.is_none());
    let distinct = chunks(2, 65536, dtype, false)?;
    assert!(
        parallel_splits(
            &distinct,
            &[Split::all(0..32000), Split::all(32001..131072)]
        )?
        .is_none()
    );
    Ok(())
}

#[rstest]
#[case::single_segment(1, 131072, DType::Primitive(PType::I64, false.into()))]
#[case::oversized_segment(2, MAX_ELEMENT_BYTES / 8 + 1, DType::Primitive(PType::I64, false.into()))]
#[case::tiny_selection(8, 1024, DType::Primitive(PType::I64, false.into()))]
#[case::variable_width(8, 131072, DType::Utf8(false.into()))]
fn unsuitable_child_work_stays_serial(
    #[case] count: u32,
    #[case] rows: u64,
    #[case] dtype: DType,
) -> VortexResult<()> {
    let plan = chunks(count, rows, dtype, false)?;
    assert!(parallel_splits(&plan, &[Split::all(0..rows * u64::from(count))])?.is_none());
    Ok(())
}
