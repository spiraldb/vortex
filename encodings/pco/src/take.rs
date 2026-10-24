// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Batched selection using compact request descriptors and one operation-owned page cache.

use std::collections::BTreeSet;
use std::ops::Range;

use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::min_max::MinMaxResult;
use vortex_array::aggregate_fn::fns::min_max::min_max;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PiecewiseSequence;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::arrays::piecewise_sequence::array::PiecewiseSequenceArraySlotsExt;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::half;
use vortex_array::match_each_integer_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::BitBufferMut;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::access::AccessState;
use crate::array::number_type_from_ptype;
use crate::decoder::PageDecoder;

/// Compact descriptor; null indices have no start. Zero-length runs consume no source rows.
struct Run {
    start: Option<usize>,
    length: usize,
    stride: usize,
}

impl TakeExecute for Pco {
    fn take(
        array: ArrayView<'_, Pco>,
        indices: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if indices.is_empty() {
            let dtype = array
                .dtype()
                .clone()
                .union_nullability(indices.dtype().nullability());
            return Ok(Some(Canonical::empty(&dtype).into_array()));
        }
        let compact = indices.as_opt::<PiecewiseSequence>().is_some();
        let mut grouped = None;
        let runs = if let Some(sequence) = indices.as_opt::<PiecewiseSequence>() {
            let starts = sequence.starts().clone().execute::<PrimitiveArray>(ctx)?;
            let lengths = sequence.lengths().clone().execute::<PrimitiveArray>(ctx)?;
            let multipliers = sequence
                .multipliers()
                .clone()
                .execute::<PrimitiveArray>(ctx)?;
            let starts = primitive_usizes(&starts)?;
            let lengths = primitive_usizes(&lengths)?;
            let multipliers = primitive_usizes(&multipliers)?;
            let mut runs = Vec::with_capacity(starts.len());
            for ((start, length), stride) in starts.into_iter().zip(lengths).zip(multipliers) {
                runs.push(Run {
                    start: Some(start),
                    length,
                    stride,
                });
            }
            runs
        } else {
            let indices = indices.clone().execute::<PrimitiveArray>(ctx)?;
            let mask = indices.validity()?.execute_mask(indices.len(), ctx)?;
            // Reuse native logical-null-aware extrema and their immutable-array stats cache.
            // Numeric extrema prove every defined index is representable and in bounds.
            let extrema = min_max(indices.as_array(), ctx, NumericalAggregateOpts::default())?;
            let summary = if let Some(extrema) = extrema {
                match_each_integer_ptype!(indices.ptype(), |I| {
                    validate_extrema(indices.as_slice::<I>(), &mask, &extrema, array.len())?
                })
            } else {
                PlainSummary {
                    defined: 0,
                    contiguous: false,
                }
            };
            if summary.defined == 0 {
                return Ok(Some(
                    ConstantArray::new(Scalar::null(array.dtype().as_nullable()), indices.len())
                        .into_array(),
                ));
            }
            if let Some(result) = zero_page_take(array, indices.len(), ctx)? {
                return Ok(Some(result));
            }
            grouped = (!summary.contiguous).then_some(summary.defined);
            if grouped.is_some() && descriptor_bytes(summary.defined) >= canonical_bytes(array) {
                return Ok(None);
            }
            if grouped.is_some()
                && match_each_integer_ptype!(indices.ptype(), |I| {
                    prefer_canonical_primitive(
                        array,
                        indices.as_slice::<I>(),
                        &mask,
                        summary.defined,
                    )?
                })
            {
                return Ok(None);
            }
            match_each_integer_ptype!(indices.ptype(), |I| {
                primitive_runs(indices.as_slice::<I>(), &mask)?
            })
        };
        validate_runs(&runs, array.len(), indices.len())?;
        if compact {
            if let Some(result) = zero_page_take(array, indices.len(), ctx)? {
                return Ok(Some(result));
            }
            // Compact ranges/repeats remain compact. Only genuine strided requests carry
            // per-position selection work; output cardinality alone is not setup work.
            let random = runs
                .iter()
                .filter(|run| run.stride > 1)
                .map(|run| run.length)
                .sum();
            if descriptor_bytes(random) >= canonical_bytes(array) && random != 0 {
                return Ok(None);
            }
        }
        let nullable = array.dtype().nullability() | indices.dtype().nullability();
        let result = match_number_enum!(number_type_from_ptype(array.dtype().as_ptype()), NumberType<T> => {
            gather::<T>(array, &runs, indices.len(), nullable, grouped, ctx)?
        });
        Ok(Some(result.into_array()))
    }
}

struct PlainSummary {
    defined: usize,
    contiguous: bool,
}

fn validate_extrema<I: NativePType>(
    indices: &[I],
    mask: &Mask,
    extrema: &MinMaxResult,
    source_len: usize,
) -> VortexResult<PlainSummary>
where
    usize: TryFrom<I>,
{
    let min = extrema
        .min
        .as_primitive()
        .try_typed_value::<I>()?
        .ok_or_else(|| vortex_err!("Pco index minimum is null"))?;
    let max = extrema
        .max
        .as_primitive()
        .try_typed_value::<I>()?
        .ok_or_else(|| vortex_err!("Pco index maximum is null"))?;
    checked_index(min)?;
    let max = checked_index(max)?;
    vortex_ensure!(
        max < source_len,
        "Pco take index {max} exceeds array length {source_len}"
    );
    // Bounds/error category is guaranteed above. No contract chooses the first invalid row.
    // Contiguous detection exits at the first mismatch; null physical values are never inspected.
    let contiguous = match mask {
        Mask::AllTrue(_) => indices.windows(2).all(|pair| {
            checked_index(pair[0])
                .ok()
                .and_then(|row| row.checked_add(1))
                == checked_index(pair[1]).ok()
        }),
        _ => false,
    };
    Ok(PlainSummary {
        defined: mask.true_count(),
        contiguous,
    })
}

fn zero_page_take(
    array: ArrayView<'_, Pco>,
    len: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    if !array.page_index()?.pages.is_empty() {
        return Ok(None);
    }
    let mask = array
        .unsliced_validity()
        .execute_mask(array.unsliced_n_rows(), ctx)?;
    vortex_ensure_eq!(
        mask.true_count(),
        0,
        "Pco zero-page source has defined rows"
    );
    Ok(Some(
        ConstantArray::new(Scalar::null(array.dtype().as_nullable()), len).into_array(),
    ))
}

fn canonical_bytes(array: ArrayView<'_, Pco>) -> usize {
    // Fallback canonicalizes the visible array, not its potentially much larger owner.
    array
        .len()
        .saturating_mul(array.dtype().as_ptype().byte_width())
}

fn descriptor_bytes(requests: usize) -> usize {
    // Actual prospective sparse-path storage: coalesced Run upper bound, sorting pair,
    // and resolved (page, compact index, repeated-request range). Output storage is common.
    requests.saturating_mul(
        size_of::<Run>() + size_of::<(usize, usize)>() + size_of::<(usize, usize, Range<usize>)>(),
    )
}

/// Borrow validated primitive inputs during planning. Broad requests stop once selected
/// physical values plus prospective descriptors exceed the visible canonical values buffer.
/// Only the chosen sparse strategy allocates Runs/sorting descriptors; no source-sized bitmap.
fn prefer_canonical_primitive<I: Copy>(
    array: ArrayView<'_, Pco>,
    indices: &[I],
    mask: &Mask,
    requests: usize,
) -> VortexResult<bool>
where
    usize: TryFrom<I>,
{
    let index = array.page_index()?;
    if !index.logical {
        return Ok(requests >= index.pages.len());
    }
    let full_bytes = canonical_bytes(array);
    let setup = descriptor_bytes(requests);
    let width = array.dtype().as_ptype().byte_width();
    let mut touched = BTreeSet::new();
    let mut selected = 0usize;
    for (&row, valid) in indices.iter().zip(mask.iter()) {
        if !valid {
            continue;
        }
        let row = array.slice_start() + checked_index(row)?;
        let page = index
            .pages
            .partition_point(|page| page.rows.as_ref().is_some_and(|rows| rows.end <= row));
        if touched.insert(page) {
            let page = index
                .pages
                .get(page)
                .ok_or_else(|| vortex_err!("Pco selected row has no logical page"))?;
            selected = selected.saturating_add(page.values.len());
            if setup.saturating_add(selected.saturating_mul(width)) >= full_bytes {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn checked_index<I>(value: I) -> VortexResult<usize>
where
    usize: TryFrom<I>,
{
    usize::try_from(value).map_err(|_| vortex_err!("Pco take index is negative or exceeds usize"))
}

fn primitive_runs<I: Copy>(indices: &[I], mask: &Mask) -> VortexResult<Vec<Run>>
where
    usize: TryFrom<I>,
{
    let mut runs: Vec<Run> = Vec::with_capacity(indices.len().min(64));
    for (&value, valid) in indices.iter().zip(mask.iter()) {
        let start = if valid {
            Some(checked_index(value)?)
        } else {
            None
        };
        if let Some(previous) = runs.last_mut() {
            let contiguous = match (previous.start, start) {
                (Some(first), Some(next)) => {
                    previous.stride == 1 && first.checked_add(previous.length) == Some(next)
                }
                (None, None) => true,
                _ => false,
            };
            if contiguous {
                previous.length += 1;
                continue;
            }
        }
        runs.push(Run {
            start,
            length: 1,
            stride: 1,
        });
    }
    Ok(runs)
}

fn primitive_usizes(array: &PrimitiveArray) -> VortexResult<Vec<usize>> {
    match_each_integer_ptype!(array.ptype(), |I| {
        array
            .as_slice::<I>()
            .iter()
            .map(|&value| checked_index(value))
            .collect()
    })
}

fn validate_runs(runs: &[Run], source_len: usize, output_len: usize) -> VortexResult<()> {
    let mut total = 0usize;
    for run in runs {
        total = total
            .checked_add(run.length)
            .ok_or_else(|| vortex_err!("Pco take output length overflows usize"))?;
        if let Some(start) = run.start
            && run.length != 0
        {
            let last = (run.length - 1)
                .checked_mul(run.stride)
                .and_then(|delta| start.checked_add(delta))
                .ok_or_else(|| vortex_err!("Pco take range overflows usize"))?;
            vortex_ensure!(
                last < source_len,
                "Pco take index {last} exceeds array length {source_len}"
            );
        }
    }
    vortex_ensure_eq!(
        total,
        output_len,
        "Pco take expanded length does not match declared length"
    );
    Ok(())
}

fn gather<T: Number + NativePType>(
    array: ArrayView<'_, Pco>,
    runs: &[Run],
    output_len: usize,
    nullability: Nullability,
    grouped: Option<usize>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    if let [
        Run {
            start: Some(start),
            length,
            stride: 1,
        },
    ] = runs
    {
        return stream_range::<T>(array, *start..*start + *length, nullability, ctx);
    }
    if array.page_index()?.pages.is_empty() {
        let mask = array
            .unsliced_validity()
            .execute_mask(array.unsliced_n_rows(), ctx)?;
        vortex_ensure_eq!(
            mask.true_count(),
            0,
            "Pco zero-page source has defined rows"
        );
        return Ok(PrimitiveArray::new(
            BufferMut::<T>::zeroed_in(output_len, ctx.allocator().clone()).freeze(),
            Validity::AllInvalid,
        ));
    }
    if nullability.is_nullable() {
        if let Some(defined) = grouped {
            gather_grouped::<T, true>(array, runs, output_len, defined, ctx)
        } else {
            gather_typed::<T, true>(array, runs, output_len, ctx)
        }
    } else if let Some(defined) = grouped {
        gather_grouped::<T, false>(array, runs, output_len, defined, ctx)
    } else {
        gather_typed::<T, false>(array, runs, output_len, ctx)
    }
}

/// Sort requests once, resolve each repeated logical position once, then invoke the native
/// primitive gather once per physical page. Only the final scatter visits output positions;
/// it does no page-index search, validity rank, decoder lookup, or cache lookup.
fn gather_grouped<T: Number + NativePType, const NULLABLE: bool>(
    array: ArrayView<'_, Pco>,
    runs: &[Run],
    output_len: usize,
    defined: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let mut requests = Vec::with_capacity(defined);
    let mut output = 0usize;
    for run in runs {
        if let Some(start) = run.start {
            for offset in 0..run.length {
                requests.push((
                    array.slice_start() + start + offset * run.stride,
                    output + offset,
                ));
            }
        }
        output += run.length;
    }
    requests.sort_unstable();
    let mut state = AccessState::<true>::default();
    let mut resolved = Vec::with_capacity(requests.len().min(64));
    let mut cursor = 0;
    while cursor < requests.len() {
        let row = requests[cursor].0;
        let end = cursor + requests[cursor..].partition_point(|request| request.0 == row);
        if let Some((page, value)) = state.resolve(array, row, ctx)? {
            resolved.push((page, value, cursor..end));
        }
        cursor = end;
    }
    let mut values = BufferMut::<T>::zeroed_in(output_len, ctx.allocator().clone());
    let mut valid = BitBufferMut::new_unset(if NULLABLE { output_len } else { 0 });
    let mut cursor = 0;
    while cursor < resolved.len() {
        let page = resolved[cursor].0;
        let end = cursor + resolved[cursor..].partition_point(|request| request.0 == page);
        let compact = PrimitiveArray::from_iter(
            resolved[cursor..end]
                .iter()
                .map(|request| {
                    u64::try_from(request.1)
                        .map_err(|_| vortex_err!("Pco compact index exceeds u64"))
                })
                .collect::<VortexResult<Vec<_>>>()?,
        )
        .into_array();
        let decoded = state.values(array, page, ctx)?.clone();
        let selected = decoded.take(compact)?.execute::<PrimitiveArray>(ctx)?;
        for (request, &value) in resolved[cursor..end].iter().zip(selected.as_slice::<T>()) {
            for &(_, output) in &requests[request.2.clone()] {
                values[output] = value;
                if NULLABLE {
                    valid.set(output);
                }
            }
        }
        cursor = end;
    }
    let validity = if NULLABLE {
        Validity::from(valid.freeze())
    } else {
        Validity::NonNullable
    };
    Ok(PrimitiveArray::new(values.freeze(), validity))
}

/// One initialized final output buffer. Nullable outputs leave null positions at the
/// allocator's safe zero initialization instead of eagerly touching every null gap.
struct Output<T: NativePType, const NULLABLE: bool> {
    values: BufferMut<T>,
    len: usize,
}

impl<T: NativePType, const NULLABLE: bool> Output<T, NULLABLE> {
    fn new(len: usize, ctx: &ExecutionCtx) -> Self {
        let values = if NULLABLE {
            BufferMut::zeroed_in(len, ctx.allocator().clone())
        } else {
            BufferMut::with_capacity_in(len, ctx.allocator().clone())
        };
        Self { values, len: 0 }
    }

    fn push(&mut self, value: T) {
        if NULLABLE {
            self.values[self.len] = value;
        } else {
            self.values.push(value);
        }
        self.len += 1;
    }

    fn extend_from_slice(&mut self, values: &[T]) {
        if NULLABLE {
            self.values[self.len..self.len + values.len()].copy_from_slice(values);
        } else {
            self.values.extend_from_slice(values);
        }
        self.len += values.len();
    }

    fn repeat(&mut self, value: T, len: usize) {
        if NULLABLE {
            self.values[self.len..self.len + len].fill(value);
        } else {
            self.values.extend(std::iter::repeat_n(value, len));
        }
        self.len += len;
    }

    fn nulls(&mut self, len: usize) {
        if NULLABLE {
            self.len += len;
        } else {
            self.repeat(T::default(), len);
        }
    }
}

fn gather_typed<T: Number + NativePType, const NULLABLE: bool>(
    array: ArrayView<'_, Pco>,
    runs: &[Run],
    output_len: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let mut state = AccessState::<true>::default();
    let mut values = Output::<T, NULLABLE>::new(output_len, ctx);
    // The non-nullable monomorphization has no bitmap allocation or append work.
    let mut valid = BitBufferMut::with_capacity(if NULLABLE { output_len } else { 0 });
    for run in runs {
        let Some(start) = run.start else {
            if NULLABLE {
                valid.append_n(false, run.length);
            }
            values.nulls(run.length);
            continue;
        };
        if run.stride == 0 && run.length != 0 {
            if let Some((page, value)) = state.resolve(array, array.slice_start() + start, ctx)? {
                let value = state.values(array, page, ctx)?.as_slice::<T>()[value];
                values.repeat(value, run.length);
                if NULLABLE {
                    valid.append_n(true, run.length);
                }
            } else {
                if NULLABLE {
                    valid.append_n(false, run.length);
                }
                values.nulls(run.length);
            }
            continue;
        }
        if run.stride == 1 && run.length != 0 {
            let mut row = array.slice_start() + start;
            let stop = row + run.length;
            while row < stop {
                if let Some(span) = state.span(array, row, stop, ctx)? {
                    match &span.mask {
                        Mask::AllTrue(n) => {
                            values.extend_from_slice(
                                &state.values(array, span.page, ctx)?.as_slice::<T>()[span.values],
                            );
                            if NULLABLE {
                                valid.append_n(true, *n);
                            }
                        }
                        Mask::AllFalse(n) => {
                            values.nulls(*n);
                            if NULLABLE {
                                valid.append_n(false, *n);
                            }
                        }
                        Mask::Values(mask) => {
                            let source = state.values(array, span.page, ctx)?.as_slice::<T>();
                            let source = &source[span.values];
                            let mut logical = 0usize;
                            let mut compact = 0usize;
                            // Copy defined runs straight into the final output. Null gaps are
                            // initialized once; no compact staging output is expanded afterward.
                            for (start, stop) in mask.bit_buffer().set_slices() {
                                values.nulls(start - logical);
                                let count = stop - start;
                                values.extend_from_slice(&source[compact..compact + count]);
                                compact += count;
                                logical = stop;
                            }
                            values.nulls(mask.len() - logical);
                            if NULLABLE {
                                valid.append_buffer(mask.bit_buffer());
                            }
                        }
                    }
                    row = span.stop;
                } else {
                    if let Some((page, value)) = state.resolve(array, row, ctx)? {
                        values.push(state.values(array, page, ctx)?.as_slice::<T>()[value]);
                        if NULLABLE {
                            valid.append_true();
                        }
                    } else {
                        values.nulls(1);
                        if NULLABLE {
                            valid.append_false();
                        }
                    }
                    row += 1;
                }
            }
            continue;
        }
        for offset in 0..run.length {
            // The complete descriptor, including its output length and last element, was
            // checked before allocating output. The source slice itself is already validated.
            let row = array.slice_start() + start + offset * run.stride;
            if let Some((page, value)) = state.resolve(array, row, ctx)? {
                values.push(state.values(array, page, ctx)?.as_slice::<T>()[value]);
                if NULLABLE {
                    valid.append_true();
                }
            } else {
                values.nulls(1);
                if NULLABLE {
                    valid.append_false();
                }
            }
        }
    }
    let validity = if NULLABLE {
        Validity::from(valid.freeze())
    } else {
        Validity::NonNullable
    };
    vortex_ensure_eq!(
        values.len,
        output_len,
        "Pco selected output length mismatch"
    );
    Ok(PrimitiveArray::new(values.values.freeze(), validity))
}

/// Contiguous selections stream pages into the final initialized values buffer and retain
/// exact sliced source validity. No output validity copy or retained decoded-page cache.
fn stream_range<T: Number + NativePType>(
    array: ArrayView<'_, Pco>,
    range: Range<usize>,
    nullability: Nullability,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    vortex_ensure!(
        range.start <= range.end && range.end <= array.len(),
        "Pco contiguous range exceeds visible bounds"
    );
    let global_start = array.slice_start() + range.start;
    let mut values = BufferMut::<T>::zeroed_in(range.len(), ctx.allocator().clone());
    let mut scratch = BufferMut::<T>::with_capacity_in(0, ctx.allocator().clone());
    let mut state = AccessState::<false>::default();
    let mut decoder = PageDecoder::<T>::new(array.metadata.header.as_slice())?;
    let stop = array.slice_start() + range.end;
    let mut row = global_start;
    while row < stop {
        let span = state
            .span(array, row, stop, ctx)?
            .ok_or_else(|| vortex_err!("Pco contiguous page span is missing"))?;
        vortex_ensure!(
            span.stop > row && span.stop <= stop,
            "Pco contiguous span does not advance within bounds"
        );
        vortex_ensure_eq!(
            span.mask.len(),
            span.stop - row,
            "Pco contiguous mask length mismatch"
        );
        vortex_ensure_eq!(
            span.values.len(),
            span.mask.true_count(),
            "Pco contiguous value count mismatch"
        );
        let start = row - global_start;
        let output = values
            .get_mut(start..start + span.mask.len())
            .ok_or_else(|| vortex_err!("Pco contiguous output exceeds visible range"))?;
        if !span.mask.all_false() {
            let index = array.page_index()?;
            let page = index
                .pages
                .get(span.page)
                .ok_or_else(|| vortex_err!("Pco contiguous page is missing"))?;
            let buffer = array
                .pages
                .get(span.page)
                .ok_or_else(|| vortex_err!("Pco compressed page is missing"))?;
            let chunk = array
                .chunk_metas
                .get(page.chunk)
                .ok_or_else(|| vortex_err!("Pco chunk metadata is missing"))?;
            let n = page.values.len();
            if span.mask.all_true() && span.values == (0..n) {
                decoder.read_into(page.chunk, chunk.as_slice(), n, buffer.as_slice(), output)?;
            } else {
                if scratch.len() < n {
                    scratch.push_n(T::default(), n - scratch.len());
                }
                decoder.read_into(
                    page.chunk,
                    chunk.as_slice(),
                    n,
                    buffer.as_slice(),
                    &mut scratch[..n],
                )?;
                let source = scratch
                    .get(span.values)
                    .ok_or_else(|| vortex_err!("Pco compact range exceeds page output"))?;
                match &span.mask {
                    Mask::AllTrue(_) => output.copy_from_slice(source),
                    Mask::AllFalse(_) => {}
                    Mask::Values(mask) => {
                        let mut compact = 0;
                        for (start, end) in mask.bit_buffer().set_slices() {
                            let len = end - start;
                            output[start..end].copy_from_slice(&source[compact..compact + len]);
                            compact += len;
                        }
                    }
                }
            }
        }
        row = span.stop;
    }
    let validity = array
        .unsliced_validity()
        .slice(global_start..stop)?
        .union_nullability(nullability);
    Ok(PrimitiveArray::new(values.freeze(), validity))
}

pub(crate) fn slice(
    array: ArrayView<'_, Pco>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    match_number_enum!(number_type_from_ptype(array.dtype().as_ptype()), NumberType<T> => {
        stream_range::<T>(array, 0..array.len(), array.dtype().nullability(), ctx)
    })
}
