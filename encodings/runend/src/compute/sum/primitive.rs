// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Weighted primitive reductions over materialized run ends, values, and validity.
//!
//! Each range visits only its valid runs. Signed products use wider arithmetic so that a run
//! can cancel a preceding sum even when its product alone does not fit in the result type.
//! Fixed-size groups share a forward run cursor; list-view groups locate their runs independently.

use std::iter::Peekable;
use std::ops::Range;

use itertools::Either;
use num_traits::AsPrimitive;
use num_traits::ToPrimitive;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::GroupRanges;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::IntegerPType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability::Nullable;
use vortex_array::match_each_native_ptype;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::scalar::PValue;
use vortex_array::scalar::Scalar;
use vortex_buffer::BitBuffer;
use vortex_buffer::BitBufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use crate::RunEnd;
use crate::RunEndArrayExt;
use crate::RunEndArraySlotsExt;

pub(super) struct RunEndSums {
    ends: PrimitiveArray,
    values: PrimitiveArray,
    validity: Mask,
    offset: usize,
    skip_nans: bool,
}

impl RunEndSums {
    pub(super) fn new(
        array: ArrayView<'_, RunEnd>,
        ctx: &mut ExecutionCtx,
        skip_nans: bool,
    ) -> VortexResult<Self> {
        let ends = array.ends().clone().execute::<PrimitiveArray>(ctx)?;
        let values = array.values().clone().execute::<PrimitiveArray>(ctx)?;
        let validity = values.validity()?.execute_mask(values.len(), ctx)?;

        Ok(Self {
            ends,
            values,
            validity,
            offset: array.offset(),
            skip_nans,
        })
    }

    /// Return the widened sum and whether the range contains no valid values.
    pub(super) fn sum(&self, range: Range<usize>) -> (Scalar, bool) {
        let range = self.offset + range.start..self.offset + range.end;
        let valid_runs = self.validity.indices();

        match_each_unsigned_integer_ptype!(self.ends.ptype(), |E| {
            let ends = self.ends.as_slice::<E>();
            match_each_native_ptype!(self.values.ptype(),
                unsigned: |T| {
                    sum_scalar(ends, self.values.as_slice::<T>(), &valid_runs, range, add_unsigned_run)
                },
                signed: |T| {
                    sum_scalar(ends, self.values.as_slice::<T>(), &valid_runs, range, add_signed_run)
                },
                floating: |T| {
                    sum_scalar(ends, self.values.as_slice::<T>(), &valid_runs, range,
                        |sum, value, len| add_float_run(sum, value, len, self.skip_nans))
                }
            )
        })
    }

    /// Sum all groups with one native type dispatch and return their sums and empty flags.
    pub(super) fn grouped_sum(
        &self,
        ranges: &GroupRanges,
        group_validity: &Mask,
    ) -> (PrimitiveArray, BitBuffer) {
        let valid_runs = self.validity.indices();

        match_each_unsigned_integer_ptype!(self.ends.ptype(), |E| {
            let ends = self.ends.as_slice::<E>();
            match_each_native_ptype!(self.values.ptype(),
                unsigned: |T| {
                    collect_sums(ends, self.values.as_slice::<T>(), valid_runs, ranges,
                        group_validity, self.offset, add_unsigned_run)
                },
                signed: |T| {
                    collect_sums(ends, self.values.as_slice::<T>(), valid_runs, ranges,
                        group_validity, self.offset, add_signed_run)
                },
                floating: |T| {
                    collect_sums(ends, self.values.as_slice::<T>(), valid_runs, ranges,
                        group_validity, self.offset,
                        |sum, value, len| add_float_run(sum, value, len, self.skip_nans))
                }
            )
        })
    }
}

fn add_unsigned_run<T: AsPrimitive<u64>>(sum: u64, value: T, len: usize) -> Option<u64> {
    value
        .as_()
        .checked_mul(len as u64)
        .and_then(|product| sum.checked_add(product))
}

fn add_signed_run<T: AsPrimitive<i64>>(sum: i64, value: T, len: usize) -> Option<i64> {
    i64::try_from(i128::from(sum) + i128::from(value.as_()) * len as i128).ok()
}

fn add_float_run<T: NativePType>(sum: f64, value: T, len: usize, skip_nans: bool) -> Option<f64> {
    if skip_nans && value.is_nan() {
        return Some(sum);
    }

    let value = ToPrimitive::to_f64(&value).vortex_expect("Float values fit in f64");
    // Fuse the operations so a finite sum can cancel a product that exceeds f64::MAX.
    Some(value.mul_add(len as f64, sum))
}

fn sum_scalar<E: IntegerPType, T: NativePType, A: NativePType + Into<PValue>>(
    ends: &[E],
    values: &[T],
    validity: &AllOr<&[usize]>,
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Scalar, bool) {
    let (sum, is_empty) = sum_runs(ends, values, validity, range, add_run);
    let sum = match sum {
        Some(sum) => Scalar::primitive(sum, Nullable),
        None => Scalar::null(DType::Primitive(A::PTYPE, Nullable)),
    };

    (sum, is_empty)
}

fn collect_sums<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    validity: AllOr<&[usize]>,
    ranges: &GroupRanges,
    group_validity: &Mask,
    offset: usize,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (PrimitiveArray, BitBuffer) {
    match ranges {
        GroupRanges::FixedSizeList { .. } => {
            let indices = match validity {
                AllOr::All => Either::Left(0..ends.len()),
                AllOr::None => Either::Left(0..0),
                AllOr::Some(indices) => Either::Right(indices.iter().copied()),
            };
            let mut indices = indices.peekable();

            collect_group_sums(ranges, group_validity, offset, |range| {
                sum_consecutive_runs(ends, values, &mut indices, range, &add_run)
            })
        }
        GroupRanges::ListView { .. } => {
            collect_group_sums(ranges, group_validity, offset, |range| {
                sum_runs(ends, values, &validity, range, &add_run)
            })
        }
    }
}

fn collect_group_sums<A: NativePType>(
    ranges: &GroupRanges,
    group_validity: &Mask,
    offset: usize,
    mut sum_group: impl FnMut(Range<usize>) -> (Option<A>, bool),
) -> (PrimitiveArray, BitBuffer) {
    let mut empty_groups = BitBufferMut::new_unset(ranges.len());
    let sums =
        PrimitiveArray::from_option_iter(ranges.iter().zip(group_validity.iter()).enumerate().map(
            |(index, ((start, len), valid))| {
                if !valid {
                    return None;
                }

                let (sum, is_empty) = sum_group(offset + start..offset + start + len);
                empty_groups.set_to(index, is_empty);
                sum
            },
        ));

    (sums, empty_groups.freeze())
}

/// Sum consecutive, non-overlapping ranges while retaining runs that cross a group boundary.
fn sum_consecutive_runs<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    indices: &mut Peekable<impl Iterator<Item = usize>>,
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Option<A>, bool) {
    let mut sum = A::default();
    if range.is_empty() {
        return (Some(sum), true);
    }

    let mut is_empty = true;
    while let Some(&index) = indices.peek() {
        let end = ends[index].as_();
        // Null groups and overflow can leave the cursor behind the next group's start.
        if end <= range.start {
            indices.next();
            continue;
        }

        let start = if index == 0 {
            range.start
        } else {
            ends[index - 1].as_().max(range.start)
        };
        if start >= range.end {
            break;
        }

        is_empty = false;
        let Some(next) = add_run(sum, values[index], end.min(range.end) - start) else {
            return (None, false);
        };
        sum = next;
        if end > range.end {
            break;
        }
        indices.next();
    }

    (Some(sum), is_empty)
}

fn sum_runs<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    validity: &AllOr<&[usize]>,
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Option<A>, bool) {
    let first = ends.partition_point(|end| end.as_() <= range.start);
    let indices = match validity {
        AllOr::All => Either::Left(first..ends.len()),
        AllOr::None => return (Some(A::default()), true),
        AllOr::Some(indices) => {
            let start = indices.partition_point(|&index| index < first);
            Either::Right(indices[start..].iter().copied())
        }
    };

    sum_consecutive_runs(ends, values, &mut indices.peekable(), range, add_run)
}
