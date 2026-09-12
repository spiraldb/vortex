// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Weighted reduction of the valid runs that intersect a logical range.
//!
//! All-valid inputs traverse the end and value slices directly. Partially valid inputs visit only
//! their valid run indices. Both paths clip the boundary runs to the requested range.
//! Signed arithmetic widens the product, and floating-point arithmetic uses fused multiply-add,
//! so a run can cancel a preceding sum even when its product alone exceeds the result type.

use std::iter::Peekable;
use std::ops::Range;

use num_traits::AsPrimitive;
use num_traits::ToPrimitive;
use vortex_array::dtype::IntegerPType;
use vortex_array::dtype::NativePType;
use vortex_error::VortexExpect;

pub(super) fn add_unsigned_run<T: AsPrimitive<u64>>(sum: u64, value: T, len: usize) -> Option<u64> {
    value
        .as_()
        .checked_mul(len as u64)
        .and_then(|product| sum.checked_add(product))
}

pub(super) fn add_signed_run<T: AsPrimitive<i64>>(sum: i64, value: T, len: usize) -> Option<i64> {
    i64::try_from(i128::from(sum) + i128::from(value.as_()) * len as i128).ok()
}

pub(super) fn add_float_run<T: NativePType>(
    sum: f64,
    value: T,
    len: usize,
    skip_nans: bool,
) -> Option<f64> {
    if skip_nans && value.is_nan() {
        return Some(sum);
    }

    let value = ToPrimitive::to_f64(&value).vortex_expect("Float values fit in f64");
    // Fuse the operations so a finite sum can cancel a product that exceeds f64::MAX.
    Some(value.mul_add(len as f64, sum))
}

/// Sum an all-valid range directly from the end and value slices.
///
/// The cursor is a position in the slices, retained for consecutive groups. Ranges must be ordered
/// and non-overlapping. Arbitrary ranges must first position the cursor with a binary search.
pub(super) fn sum_all_valid<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    cursor: &mut usize,
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Option<A>, bool) {
    let mut sum = A::default();
    if range.is_empty() {
        return (Some(sum), true);
    }

    // Skipped null groups or an earlier overflow can leave the cursor behind this range.
    while ends[*cursor].as_() <= range.start {
        *cursor += 1;
    }

    let mut start = range.start;
    for (&end, &value) in ends[*cursor..].iter().zip(&values[*cursor..]) {
        let end = end.as_();
        if end >= range.end {
            *cursor += usize::from(end == range.end);
            return (add_run(sum, value, range.end - start), false);
        }

        *cursor += 1;
        let Some(next) = add_run(sum, value, end - start) else {
            return (None, false);
        };
        sum = next;
        start = end;
    }

    (Some(sum), false)
}

/// Locate the first intersecting valid run before summing an arbitrary range.
pub(super) fn sum_valid_range<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    indices: &[usize],
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Option<A>, bool) {
    let first = ends.partition_point(|end| end.as_() <= range.start);
    let start = indices.partition_point(|&index| index < first);
    let mut indices = indices[start..].iter().copied().peekable();

    sum_next_valid_range(ends, values, &mut indices, range, add_run)
}

/// Sum the next range while retaining a run that crosses its end.
///
/// The caller must supply non-overlapping ranges in increasing order. Skipped null groups and
/// early overflow returns can leave the cursor behind the next range's start.
pub(super) fn sum_next_valid_range<E: IntegerPType, T: NativePType, A: NativePType>(
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
        if end <= range.start {
            indices.next();
            continue;
        }

        let run_start = if index == 0 { 0 } else { ends[index - 1].as_() };
        let start = run_start.max(range.start);
        if start >= range.end {
            break;
        }

        let overlap_len = end.min(range.end) - start;
        is_empty = false;
        let Some(next) = add_run(sum, values[index], overlap_len) else {
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
