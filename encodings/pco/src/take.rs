// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Batched selection using compact request descriptors and one operation-owned page cache.

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
        let nullable = array.dtype().nullability() | indices.dtype().nullability();
        let result = match_number_enum!(number_type_from_ptype(array.dtype().as_ptype()), NumberType<T> => {
            gather::<T>(array, &runs, indices.len(), nullable, ctx)?
        });
        Ok(Some(result.into_array()))
    }
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
    if nullability.is_nullable() {
        gather_typed::<T, true>(array, runs, output_len, ctx)
    } else {
        gather_typed::<T, false>(array, runs, output_len, ctx)
    }
}

fn gather_typed<T: Number + NativePType, const NULLABLE: bool>(
    array: ArrayView<'_, Pco>,
    runs: &[Run],
    output_len: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let mut state = AccessState::default();
    let mut values = BufferMut::<T>::with_capacity_in(output_len, ctx.allocator().clone());
    // The non-nullable monomorphization has no bitmap allocation or append work.
    let mut valid = BitBufferMut::with_capacity(if NULLABLE { output_len } else { 0 });
    for run in runs {
        let Some(start) = run.start else {
            if NULLABLE {
                valid.append_n(false, run.length);
            }
            values.extend(std::iter::repeat_n(T::default(), run.length));
            continue;
        };
        if run.stride == 0 && run.length != 0 {
            if let Some((page, value)) = state.resolve(array, array.slice_start() + start, ctx)? {
                let value = state.values(array, page, ctx)?.as_slice::<T>()[value];
                values.extend(std::iter::repeat_n(value, run.length));
                if NULLABLE {
                    valid.append_n(true, run.length);
                }
            } else {
                if NULLABLE {
                    valid.append_n(false, run.length);
                }
                values.extend(std::iter::repeat_n(T::default(), run.length));
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
                            values.extend(std::iter::repeat_n(T::default(), *n));
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
                                values.extend(std::iter::repeat_n(T::default(), start - logical));
                                let count = stop - start;
                                values.extend_from_slice(&source[compact..compact + count]);
                                compact += count;
                                logical = stop;
                            }
                            values.extend(std::iter::repeat_n(T::default(), mask.len() - logical));
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
                        values.push(T::default());
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
                values.push(T::default());
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
    Ok(PrimitiveArray::new(values.freeze(), validity))
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
