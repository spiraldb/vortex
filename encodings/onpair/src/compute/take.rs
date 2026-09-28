// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Dictionary-aware take: gather rows in the compressed domain.
//!
//! Registered as the `Dict` take kernel. When a dictionary's values are OnPair-encoded and a
//! filter has left fewer rows than the dictionary holds, this copies just the code ranges of
//! those rows so canonicalization decodes only them, instead of decoding every dictionary value
//! and taking views from the result.

use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::List;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::OnPair;
use crate::OnPairArrayExt;
use crate::OnPairArraySlotsExt;
use crate::OnPairIndexChildren;

impl TakeExecute for OnPair {
    fn take(
        array: ArrayView<'_, Self>,
        indices: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        // Taking at least as many rows as the dictionary holds copies more codes than a full
        // decode reads, so leave that to the canonical path.
        if indices.len() >= array.len() {
            return Ok(None);
        }

        let indices_nulls_zeroed = match indices.validity()?.execute_mask(indices.len(), ctx)? {
            Mask::AllTrue(_) => indices.clone(),
            Mask::AllFalse(_) => {
                return Ok(Some(
                    ConstantArray::new(Scalar::null(array.dtype().as_nullable()), indices.len())
                        .into_array(),
                ));
            }
            Mask::Values(_) => indices.fill_null(Scalar::from(0).cast(indices.dtype())?)?,
        };

        let codes = unsafe {
            ListArray::new_unchecked(
                array.codes().clone(),
                array.codes_offsets().clone(),
                Validity::NonNullable,
            )
        };
        let taken_codes = <List as TakeExecute>::take(codes.as_view(), &indices_nulls_zeroed, ctx)?
            .vortex_expect("List take kernel always returns Some")
            .try_downcast::<List>()
            .ok()
            .vortex_expect("List take returns a List");
        let uncompressed_lengths = array.uncompressed_lengths().take(indices_nulls_zeroed)?;
        let validity = array.array_validity().take(indices)?;
        let dtype = array
            .dtype()
            .clone()
            .union_nullability(indices.dtype().nullability());

        Ok(Some(
            unsafe {
                OnPair::new_unchecked(
                    dtype,
                    array.data().clone().without_indexes(),
                    array.dict_offsets().clone(),
                    taken_codes.elements().clone(),
                    taken_codes.offsets().clone(),
                    uncompressed_lengths,
                    validity,
                    OnPairIndexChildren::default(),
                )
            }
            .into_array(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::DictArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::SharedArray;
    use vortex_array::arrays::VarBinArray;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_error::vortex_err;
    use vortex_session::VortexSession;

    use super::*;
    use crate::DEFAULT_CONFIG;
    use crate::OnPairArray;
    use crate::build_token_frequency_index;
    use crate::onpair_compress;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        session
    });

    fn strings(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("https://www.example.com/items/{i:08}"))
            .collect()
    }

    fn encode(values: &[String], nullability: Nullability) -> VortexResult<OnPairArray> {
        let input = VarBinArray::from_iter(
            values.iter().map(|s| Some(s.as_bytes())),
            DType::Utf8(nullability),
        )
        .into_array();
        let mut ctx = SESSION.create_execution_ctx();
        let encoded = onpair_compress(&input, DEFAULT_CONFIG, &mut ctx)?
            .try_downcast::<OnPair>()
            .map_err(|array| vortex_err!("expected OnPair, got {}", array.encoding_id()))?;
        build_token_frequency_index(encoded, &mut ctx)
    }

    #[test]
    fn take_gathers_rows_without_decoding() -> VortexResult<()> {
        let values = strings(2_000);
        let array = encode(&values, Nullability::NonNullable)?;
        let dict_bytes_before = array.dict_bytes().clone();
        let picks = [7u32, 3, 3, 1_999, 0, 42];

        let mut ctx = SESSION.create_execution_ctx();
        let taken = <OnPair as TakeExecute>::take(
            array.as_view(),
            &PrimitiveArray::from_iter(picks).into_array(),
            &mut ctx,
        )?
        .ok_or_else(|| vortex_err!("take should stay in the compressed domain"))?;
        let typed = taken
            .try_downcast::<OnPair>()
            .map_err(|array| vortex_err!("take result was not OnPair: {}", array.encoding_id()))?;
        assert_eq!(typed.dict_bytes().as_slice(), dict_bytes_before.as_slice());
        assert!(typed.token_frequency_index_child().is_none());
        assert_eq!(typed.len(), picks.len());

        let expected =
            VarBinViewArray::from_iter_str(picks.iter().map(|&i| values[i as usize].as_str()));
        assert_arrays_eq!(typed.into_array(), expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn take_with_null_indices_yields_nulls() -> VortexResult<()> {
        let values = strings(100);
        let array = encode(&values, Nullability::NonNullable)?;
        let mut ctx = SESSION.create_execution_ctx();
        let indices = PrimitiveArray::from_option_iter([Some(5u16), None, Some(99)]).into_array();
        let taken = <OnPair as TakeExecute>::take(array.as_view(), &indices, &mut ctx)?
            .ok_or_else(|| vortex_err!("take should stay in the compressed domain"))?;
        let expected = VarBinViewArray::from_iter_nullable_str([
            Some(values[5].as_str()),
            None,
            Some(values[99].as_str()),
        ]);
        assert_arrays_eq!(taken, expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn take_of_full_dictionary_falls_back() -> VortexResult<()> {
        let values = strings(50);
        let array = encode(&values, Nullability::NonNullable)?;
        let mut ctx = SESSION.create_execution_ctx();
        let indices = PrimitiveArray::from_iter((0..60u32).map(|i| i % 50)).into_array();
        assert!(<OnPair as TakeExecute>::take(array.as_view(), &indices, &mut ctx)?.is_none());
        Ok(())
    }

    #[test]
    fn dict_over_shared_onpair_executes_taken_rows() -> VortexResult<()> {
        let values = strings(1_000);
        let array = encode(&values, Nullability::NonNullable)?;
        let codes = PrimitiveArray::from_iter([999u16, 1, 500]).into_array();
        let dict = unsafe {
            DictArray::new_unchecked(codes, SharedArray::new(array.into_array()).into_array())
        }
        .into_array();
        let mut ctx = SESSION.create_execution_ctx();
        let expected = VarBinViewArray::from_iter_str([
            values[999].as_str(),
            values[1].as_str(),
            values[500].as_str(),
        ]);
        assert_arrays_eq!(dict, expected, &mut ctx);
        Ok(())
    }
}
