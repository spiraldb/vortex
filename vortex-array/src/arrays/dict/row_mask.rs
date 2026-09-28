// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Expansion of a predicate evaluated over dictionary values into a mask over rows.

use std::ops::BitAnd;

use num_traits::AsPrimitive;
use vortex_buffer::BitBuffer;
use vortex_buffer::BitBufferMut;
use vortex_buffer::BufferMut;
use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::arrays::PrimitiveArray;
use crate::dtype::DType;
use crate::match_each_integer_ptype;

/// Expand a boolean predicate result over dictionary `values` into a [`Mask`] over the rows that
/// `codes` index into it, restricted to `selection`.
///
/// Null values and null codes both map to `false`, matching SQL filter semantics. Compared with
/// `values.take(codes)` followed by a mask conversion, this walks the codes once, writes the row
/// mask directly, and short-circuits when no value or every value matched. When `selection` keeps
/// only a small fraction of the rows, only those rows' codes are fetched and tested.
pub fn dict_row_mask(
    values: ArrayRef,
    codes: ArrayRef,
    selection: &Mask,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Mask> {
    vortex_ensure!(
        matches!(values.dtype(), DType::Bool(_)),
        "dict_row_mask values must be boolean, got {}",
        values.dtype()
    );
    let rows = codes.len();
    vortex_ensure!(
        selection.len() == rows,
        "dict_row_mask selection has {} rows, codes have {rows}",
        selection.len()
    );
    if matches!(selection, Mask::AllFalse(_)) {
        return Ok(Mask::new_false(rows));
    }
    let codes_validity = codes.validity()?.execute_mask(rows, ctx)?;
    if matches!(codes_validity, Mask::AllFalse(_)) {
        return Ok(Mask::new_false(rows));
    }

    let value_mask = values.null_as_false().execute(ctx)?;
    let value_bits = match value_mask.bit_buffer() {
        AllOr::All => return Ok(selection.bitand(&codes_validity)),
        AllOr::None => return Ok(Mask::new_false(rows)),
        AllOr::Some(bits) => bits.clone(),
    };

    let codes = codes.execute::<PrimitiveArray>(ctx)?;

    // Sparse selections: test only the selected rows and emit their hits by index. Unpacking the
    // codes is a fast vectorized pass, so it is cheaper than a random-access take even here.
    if let Some(selected) = selection.values()
        && selected.true_count() * SPARSE_SELECTION_DIVISOR < rows
    {
        let selected_validity = codes_validity.bitand(selection);
        let hit_rows = match_each_integer_ptype!(codes.ptype(), |C| {
            gather_selected(&value_bits, codes.as_slice::<C>(), selected.bit_buffer())?
        });
        return Ok(Mask::from_indices(rows, hit_rows).bitand(&selected_validity));
    }

    let validity_bits = codes_validity.values().map(|v| v.bit_buffer());
    let gathered = match_each_integer_ptype!(codes.ptype(), |C| {
        gather_bits(&value_bits, codes.as_slice::<C>(), validity_bits)?
    });
    Ok(Mask::from_buffer(gathered)
        .bitand(&codes_validity)
        .bitand(selection))
}

/// A selection keeping fewer than one row in this many takes the sparse path.
const SPARSE_SELECTION_DIVISOR: usize = 32;

/// Indices of the selected rows whose code maps to a set bit.
///
/// Codes at unselected rows are never read, so only the selected rows are range checked. A code
/// outside the dictionary at a selected row is an error; the caller has already dropped null
/// codes from the selection's contribution through the validity mask.
fn gather_selected<C: AsPrimitive<usize>>(
    bits: &BitBuffer,
    codes: &[C],
    selected: &BitBuffer,
) -> VortexResult<Vec<usize>> {
    let num_values = bits.len();
    let mut hits = Vec::with_capacity(selected.true_count());
    selected.try_for_each_set_index(|row| {
        let idx = codes[row].as_();
        if idx >= num_values {
            vortex_bail!("dict code {idx} out of range for {num_values} dictionary values");
        }
        if bits.value(idx) {
            hits.push(row);
        }
        Ok::<(), VortexError>(())
    })?;
    Ok(hits)
}

/// Gather `bits[codes[i]]` for every row into a new bitmap.
///
/// The dictionary is expanded once into a byte-per-value table, which stays cache resident, so
/// each row costs a byte load instead of a variable shift. Rows are packed eight at a time through
/// independent accumulators to keep the loop out of a single dependency chain. The code range is
/// checked with one vectorizable pass up front; a code outside the dictionary is an error unless
/// that row's code is null, in which case the caller masks the row out anyway.
fn gather_bits<C: AsPrimitive<usize>>(
    bits: &BitBuffer,
    codes: &[C],
    codes_validity: Option<&BitBuffer>,
) -> VortexResult<BitBuffer> {
    let num_values = bits.len();
    let max_code = codes.iter().map(|code| code.as_()).max().unwrap_or(0);
    if !codes.is_empty() && max_code >= num_values {
        let Some(validity) = codes_validity else {
            vortex_bail!("dict code {max_code} out of range for {num_values} dictionary values");
        };
        for (i, code) in codes.iter().enumerate() {
            if code.as_() >= num_values && validity.value(i) {
                vortex_bail!(
                    "dict code {} out of range for {num_values} dictionary values",
                    code.as_()
                );
            }
        }
        return Ok(BitBuffer::collect_bool(codes.len(), |i| {
            let idx = codes[i].as_();
            idx < num_values && bits.value(idx)
        }));
    }

    let lut = expand_to_bytes(bits);
    let mut words = BufferMut::<u64>::zeroed(codes.len().div_ceil(64));
    let word_slice = words.as_mut_slice();
    let (full, tail) = codes.as_chunks::<64>();
    for (word, chunk) in word_slice.iter_mut().zip(full) {
        let mut acc = 0u64;
        for (k, group) in chunk.as_chunks::<8>().0.iter().enumerate() {
            let mut v = 0u64;
            for (j, code) in group.iter().enumerate() {
                // SAFETY: `max_code < num_values == lut.len()` was checked above.
                v |= u64::from(unsafe { *lut.get_unchecked(code.as_()) }) << j;
            }
            acc |= v << (k * 8);
        }
        *word = acc;
    }
    if !tail.is_empty() {
        let mut acc = 0u64;
        for (j, code) in tail.iter().enumerate() {
            // SAFETY: as above.
            acc |= u64::from(unsafe { *lut.get_unchecked(code.as_()) }) << j;
        }
        word_slice[codes.len() / 64] = acc;
    }
    let mut bytes = words.into_byte_buffer();
    bytes.truncate(codes.len().div_ceil(8));
    Ok(BitBufferMut::from_buffer(bytes, 0, codes.len()).freeze())
}

/// Expand a bitmap into one byte per bit, eight bytes per input byte through a multiply spread.
fn expand_to_bytes(bits: &BitBuffer) -> Vec<u8> {
    let aligned;
    let bytes = match bits.byte_aligned_bytes() {
        Some(bytes) => bytes,
        None => {
            aligned = bits.sliced();
            aligned
                .byte_aligned_bytes()
                .vortex_expect("sliced bit buffer starts at offset zero")
        }
    };
    let mut lut = vec![0u8; bytes.len() * 8];
    for (out, &byte) in lut.as_chunks_mut::<8>().0.iter_mut().zip(bytes) {
        // Place bit j of `byte` at bit 0 of output byte j.
        let isolated = u64::from(byte).wrapping_mul(0x0101_0101_0101_0101) & 0x8040_2010_0804_0201;
        let spread = (isolated.wrapping_add(0x7f7f_7f7f_7f7f_7f7f) >> 7) & 0x0101_0101_0101_0101;
        *out = spread.to_le_bytes();
    }
    lut.truncate(bits.len());
    lut
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use super::*;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::arrays::ConstantArray;
    use crate::arrays::PrimitiveArray;
    use crate::assert_arrays_eq;
    use crate::dtype::Nullability;
    use crate::scalar::Scalar;

    fn check(values: ArrayRef, codes: ArrayRef, expected: &[bool]) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let selection = Mask::new_true(codes.len());
        let mask = dict_row_mask(values, codes, &selection, &mut ctx)?;
        assert_arrays_eq!(
            mask.into_array(),
            BoolArray::from_iter(expected.iter().copied()),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn gathers_value_bits_by_code() -> VortexResult<()> {
        check(
            BoolArray::from_iter([true, false, true]).into_array(),
            PrimitiveArray::from_iter([0u16, 1, 2, 2, 1, 0]).into_array(),
            &[true, false, true, true, false, true],
        )
    }

    #[test]
    fn spans_word_boundaries() -> VortexResult<()> {
        let codes: Vec<u32> = (0..150).map(|i| i % 3).collect();
        let expected: Vec<bool> = codes.iter().map(|c| *c != 1).collect();
        check(
            BoolArray::from_iter([true, false, true]).into_array(),
            PrimitiveArray::from_iter(codes).into_array(),
            &expected,
        )
    }

    #[test]
    fn null_values_are_false() -> VortexResult<()> {
        check(
            BoolArray::from_iter([Some(true), None, Some(false)]).into_array(),
            PrimitiveArray::from_iter([1u8, 0, 2]).into_array(),
            &[false, true, false],
        )
    }

    #[test]
    fn null_codes_are_false() -> VortexResult<()> {
        check(
            BoolArray::from_iter([true, true]).into_array(),
            PrimitiveArray::from_option_iter([Some(0u16), None, Some(1)]).into_array(),
            &[true, false, true],
        )
    }

    #[rstest]
    #[case(Some(true), [true, true, true])]
    #[case(Some(false), [false, false, false])]
    #[case(None, [false, false, false])]
    fn constant_values_short_circuit(
        #[case] value: Option<bool>,
        #[case] expected: [bool; 3],
    ) -> VortexResult<()> {
        let scalar = match value {
            Some(v) => Scalar::bool(v, Nullability::Nullable),
            None => Scalar::null(DType::Bool(Nullability::Nullable)),
        };
        check(
            ConstantArray::new(scalar, 2).into_array(),
            PrimitiveArray::from_iter([0u16, 1, 0]).into_array(),
            &expected,
        )
    }

    #[test]
    fn sparse_selection_tests_only_selected_rows() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let rows = 1_000usize;
        let codes: Vec<u16> = (0..1_000u16).map(|i| i % 3).collect();
        let values = BoolArray::from_iter([true, false, true]).into_array();
        // Keep every 50th row: under the sparse threshold.
        let selection = Mask::from_indices(rows, (0..rows).step_by(50));
        let mask = dict_row_mask(
            values,
            PrimitiveArray::from_iter(codes.clone()).into_array(),
            &selection,
            &mut ctx,
        )?;
        let expected: Vec<bool> = (0..rows).map(|i| i % 50 == 0 && codes[i] != 1).collect();
        assert_arrays_eq!(mask.into_array(), BoolArray::from_iter(expected), &mut ctx);
        Ok(())
    }

    #[test]
    fn dense_selection_is_applied() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let selection = Mask::from_iter([true, false, true, true]);
        let mask = dict_row_mask(
            BoolArray::from_iter([true, true]).into_array(),
            PrimitiveArray::from_iter([0u16, 1, 0, 1]).into_array(),
            &selection,
            &mut ctx,
        )?;
        assert_arrays_eq!(
            mask.into_array(),
            BoolArray::from_iter([true, false, true, true]),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn out_of_range_code_is_an_error() {
        let mut ctx = array_session().create_execution_ctx();
        let result = dict_row_mask(
            BoolArray::from_iter([true, false]).into_array(),
            PrimitiveArray::from_iter([0u16, 5]).into_array(),
            &Mask::new_true(2),
            &mut ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn out_of_range_null_code_is_masked() -> VortexResult<()> {
        check(
            BoolArray::from_iter([true, false]).into_array(),
            PrimitiveArray::from_option_iter([Some(0u16), None]).into_array(),
            &[true, false],
        )
    }
}
