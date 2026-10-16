// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use itertools::Itertools;
use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::Canonical;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::Chunked;
use crate::arrays::ChunkedArray;
use crate::arrays::Constant;
use crate::arrays::ScalarFn;
use crate::arrays::ScalarFnArray;
use crate::arrays::chunked::ChunkedArrayExt;
use crate::arrays::scalar_fn::AnyScalarFn;
use crate::arrays::scalar_fn::ScalarFnArrayExt;
use crate::optimizer::ArrayOptimizer;
use crate::optimizer::rules::ArrayParentReduceRule;
use crate::optimizer::rules::ParentRuleSet;
use crate::scalar_fn::fns::cast::CastReduceAdaptor;
use crate::scalar_fn::fns::fill_null::FillNullReduceAdaptor;
use crate::scalar_fn::fns::list_contains::ListContainsElementReduceAdaptor;

pub(crate) const PARENT_RULES: ParentRuleSet<Chunked> = ParentRuleSet::new(&[
    ParentRuleSet::lift(&CastReduceAdaptor(Chunked)),
    ParentRuleSet::lift(&ChunkedUnaryScalarFnPushDownRule),
    ParentRuleSet::lift(&ListContainsElementReduceAdaptor(Chunked)),
    ParentRuleSet::lift(&ChunkedConstantScalarFnPushDownRule),
    ParentRuleSet::lift(&FillNullReduceAdaptor(Chunked)),
]);

/// Push down any unary scalar function through chunked arrays.
#[derive(Debug)]
struct ChunkedUnaryScalarFnPushDownRule;
impl ArrayParentReduceRule<Chunked> for ChunkedUnaryScalarFnPushDownRule {
    type Parent = AnyScalarFn;

    fn reduce_parent(
        &self,
        array: ArrayView<'_, Chunked>,
        parent: ArrayView<'_, ScalarFn>,
        _child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        if parent.nchildren() != 1 {
            return Ok(None);
        }

        let new_chunks: Vec<_> = array
            .iter_chunks()
            .map(|chunk| {
                ScalarFnArray::try_new(parent.scalar_fn().clone(), vec![chunk.clone()])?
                    .into_array()
                    .optimize()
            })
            .try_collect()?;

        Ok(Some(
            unsafe { ChunkedArray::new_unchecked(new_chunks, parent.dtype().clone()) }.into_array(),
        ))
    }
}

/// Push down non-unary scalar functions through chunked arrays where other siblings are constant.
#[derive(Debug)]
struct ChunkedConstantScalarFnPushDownRule;
impl ArrayParentReduceRule<Chunked> for ChunkedConstantScalarFnPushDownRule {
    type Parent = AnyScalarFn;

    fn reduce_parent(
        &self,
        array: ArrayView<'_, Chunked>,
        parent: ArrayView<'_, ScalarFn>,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        for (idx, child) in parent.iter_children().enumerate() {
            if idx == child_idx {
                continue;
            }
            if !child.is::<Constant>() {
                return Ok(None);
            }
        }

        let mut offset = 0;
        let new_chunks: Vec<_> = array
            .iter_chunks()
            .map(|chunk| {
                // Slicing a constant to an empty range yields a canonical array, which functions
                // that require constant siblings reject, so empty chunks stay empty.
                if chunk.is_empty() {
                    return Ok(Canonical::empty(parent.dtype()).into_array());
                }
                let range = offset..offset + chunk.len();
                offset += chunk.len();
                // Each sibling is sliced to the chunk's rows, so that every chunk shares what the
                // sibling shares rather than getting its own copy of the constant.
                let new_children: Vec<_> = parent
                    .iter_children()
                    .enumerate()
                    .map(|(idx, child)| {
                        if idx == child_idx {
                            Ok(chunk.clone())
                        } else {
                            child.slice(range.clone())
                        }
                    })
                    .try_collect()?;

                ScalarFnArray::try_new(parent.scalar_fn().clone(), new_children)?
                    .into_array()
                    .optimize()
            })
            .try_collect()?;

        Ok(Some(
            unsafe { ChunkedArray::new_unchecked(new_chunks, parent.dtype().clone()) }.into_array(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use crate::Canonical;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::arrays::Chunked;
    use crate::arrays::ChunkedArray;
    use crate::arrays::ConstantArray;
    use crate::arrays::chunked::ChunkedArrayExt;
    use crate::assert_arrays_eq;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::optimizer::ArrayOptimizer;
    use crate::scalar_fn::fns::fill_null::FillNull;
    use crate::scalar_fn::fns::not::Not;

    /// A chunk followed by an empty chunk.
    fn chunked_with_empty_chunk(nullability: Nullability) -> VortexResult<ChunkedArray> {
        let dtype = DType::Bool(nullability);
        let chunk = match nullability {
            Nullability::NonNullable => BoolArray::from_iter([true, false, true]).into_array(),
            Nullability::Nullable => {
                BoolArray::from_iter([Some(true), None, Some(false)]).into_array()
            }
        };
        let empty = Canonical::empty(&dtype).into_array();
        ChunkedArray::try_new(vec![chunk, empty], dtype)
    }

    #[rstest]
    fn test_constant_sibling_push_down_keeps_empty_chunks(
        #[values(Nullability::NonNullable, Nullability::Nullable)] nullability: Nullability,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let chunked = chunked_with_empty_chunk(nullability)?.into_array();
        let filled = FillNull::try_new(chunked, ConstantArray::new(false, 3).into_array())?
            .into_array()
            .optimize()?;
        let filled = filled
            .as_opt::<Chunked>()
            .expect("fill_null is pushed into the chunks");
        assert_eq!(
            filled
                .iter_chunks()
                .map(|chunk| chunk.len())
                .collect::<Vec<_>>(),
            [3, 0]
        );
        let expected = match nullability {
            Nullability::NonNullable => [true, false, true],
            Nullability::Nullable => [true, false, false],
        };
        assert_arrays_eq!(
            filled.array().clone(),
            BoolArray::from_iter(expected),
            &mut ctx
        );
        Ok(())
    }

    #[rstest]
    fn test_not_fill_null_over_chunks_with_an_empty_chunk(
        #[values(Nullability::NonNullable, Nullability::Nullable)] nullability: Nullability,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let chunked = chunked_with_empty_chunk(nullability)?.into_array();
        let filled = FillNull::try_new(chunked, ConstantArray::new(false, 3).into_array())?;
        let array = Not::try_new(filled.into_array())?.into_array();

        let expected = match nullability {
            Nullability::NonNullable => [false, true, false],
            Nullability::Nullable => [false, true, true],
        };
        assert_arrays_eq!(array, BoolArray::from_iter(expected), &mut ctx);
        Ok(())
    }
}
