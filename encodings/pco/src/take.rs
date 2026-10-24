// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Batched selection using compact request descriptors and one operation-owned page cache.

use std::collections::BTreeSet;

use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::PiecewiseSequence;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::arrays::piecewise_sequence::array::PiecewiseSequenceArraySlotsExt;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::match_each_integer_ptype;
use vortex_array::validity::Validity;
use vortex_buffer::BitBufferMut;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::access::AccessState;
use crate::array::number_type_from_ptype;

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
            match_each_integer_ptype!(indices.ptype(), |I| {
                primitive_runs(indices.as_slice::<I>(), &mask)?
            })
        };
        validate_runs(&runs, array.len(), indices.len())?;
        // For broad scattered requests the ordinary canonical decoder plus native primitive
        // take has less setup/decoded work than retaining almost every page separately.
        if scattered(&runs) && prefer_canonical(array, &runs)? {
            return Ok(None);
        }
        let nullable = array.dtype().nullability() | indices.dtype().nullability();
        let result = match_number_enum!(number_type_from_ptype(array.dtype().as_ptype()), NumberType<T> => {
            gather::<T>(array, &runs, indices.len(), nullable, ctx)?
        });
        Ok(Some(result.into_array()))
    }
}

fn scattered(runs: &[Run]) -> bool {
    runs.len() > 1 && runs.iter().all(|run| run.length <= 1 || run.stride != 1)
}

/// Conservative work estimate in bytes: selected physical values plus request descriptors
/// versus one full canonical values buffer. Stop as soon as canonical work wins, so broad
/// requests do not first build a complete selected-page set. No source-sized scratch bitmap.
fn prefer_canonical(array: ArrayView<'_, Pco>, runs: &[Run]) -> VortexResult<bool> {
    let index = array.page_index()?;
    let total = index.pages.last().map_or(0, |page| page.values.end);
    let width = array.dtype().as_ptype().byte_width();
    let full_bytes = total.saturating_mul(width);
    let requests = runs.iter().map(|run| run.length).sum::<usize>();
    let setup_bytes = requests.saturating_mul(std::mem::size_of::<(usize, usize)>());
    if setup_bytes >= full_bytes {
        return Ok(true);
    }
    if !index.logical {
        // With no logical page index, page coverage itself requires global validity rank.
        // Use the page-count upper bound rather than spending arbitrary-request setup first.
        return Ok(requests >= index.pages.len());
    }
    let mut touched = BTreeSet::new();
    let mut selected = 0usize;
    for run in runs {
        let Some(start) = run.start else { continue };
        for offset in 0..run.length {
            let row = array.slice_start() + start + offset * run.stride;
            let page = index
                .pages
                .partition_point(|page| page.rows.as_ref().is_some_and(|rows| rows.end <= row));
            if touched.insert(page) {
                let selected_page = index
                    .pages
                    .get(page)
                    .ok_or_else(|| vortex_err!("Pco selected row has no logical page"))?;
                selected = selected.saturating_add(selected_page.values.len());
                if setup_bytes.saturating_add(selected.saturating_mul(width)) >= full_bytes {
                    return Ok(true);
                }
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
    nullability: vortex_array::dtype::Nullability,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
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
        if scattered(runs) {
            gather_grouped::<T, true>(array, runs, output_len, ctx)
        } else {
            gather_typed::<T, true>(array, runs, output_len, ctx)
        }
    } else if scattered(runs) {
        gather_grouped::<T, false>(array, runs, output_len, ctx)
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
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let mut requests = Vec::with_capacity(output_len);
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
    let mut state = AccessState::default();
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
    let mut state = AccessState::default();
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

pub(crate) fn slice(
    array: ArrayView<'_, Pco>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let runs = [Run {
        start: Some(0),
        length: array.len(),
        stride: 1,
    }];
    match_number_enum!(number_type_from_ptype(array.dtype().as_ptype()), NumberType<T> => {
        gather::<T>(array, &runs, array.len(), array.dtype().nullability(), ctx)
    })
}
