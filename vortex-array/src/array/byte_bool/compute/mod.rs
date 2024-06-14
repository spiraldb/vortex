use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::BooleanBufferBuilder;
use num_traits::AsPrimitive;
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use super::{ByteBoolArray, ByteBoolMetadata};
use crate::{
    compute::{
        as_arrow::AsArrowArray, scalar_at::ScalarAtFn, slice::SliceFn, take::TakeFn, ArrayCompute,
    },
    encoding::ArrayEncodingRef,
    stats::StatsSet,
    validity::{ArrayValidity, Validity},
    ArrayDType, ArrayData, ArrayTrait, IntoArray,
};

impl ArrayCompute for ByteBoolArray {
    fn as_arrow(&self) -> Option<&dyn crate::compute::as_arrow::AsArrowArray> {
        Some(self)
    }

    fn cast(&self) -> Option<&dyn crate::compute::cast::CastFn> {
        None
    }

    fn compare(&self) -> Option<&dyn crate::compute::compare::CompareFn> {
        None
    }

    fn fill_forward(&self) -> Option<&dyn crate::compute::fill::FillForwardFn> {
        None
    }

    fn filter_indices(&self) -> Option<&dyn crate::compute::filter_indices::FilterIndicesFn> {
        None
    }

    fn patch(&self) -> Option<&dyn crate::compute::patch::PatchFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn subtract_scalar(&self) -> Option<&dyn crate::compute::scalar_subtract::SubtractScalarFn> {
        None
    }

    fn search_sorted(&self) -> Option<&dyn crate::compute::search_sorted::SearchSortedFn> {
        None
    }

    fn slice(&self) -> Option<&dyn crate::compute::slice::SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn crate::compute::take::TakeFn> {
        None
    }
}

impl ScalarAtFn for ByteBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if index >= self.len() {
            vortex_bail!(OutOfBounds: index, 0, self.len());
        }

        let scalar = match self.is_valid(index).then(|| self.buffer()[index] == 1) {
            Some(b) => b.into(),
            None => Scalar::null(self.dtype().clone()),
        };

        Ok(scalar)
    }
}

impl AsArrowArray for ByteBoolArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let nulls = self.logical_validity().to_null_buffer()?;

        let mut builder = BooleanBufferBuilder::new(self.len());

        let bool_slice = self.as_bool_slice();

        builder.append_slice(bool_slice);

        Ok(Arc::new(ArrowBoolArray::new(builder.finish(), nulls)))
    }
}

impl SliceFn for ByteBoolArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<crate::Array> {
        match stop.checked_sub(start) {
            None => vortex_bail!(ComputeError:
                "{}..{} is an invalid slicing range", start, stop
            ),
            Some(length) => {
                let validity = self.validity().slice(start, stop)?;

                let slice_metadata = Arc::new(ByteBoolMetadata {
                    validity: validity.to_metadata(length)?,
                    length,
                });

                ArrayData::try_new(
                    self.encoding(),
                    self.dtype().clone(),
                    slice_metadata,
                    Some(self.buffer().slice(start..stop)),
                    validity
                        .into_array_data()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .into(),
                    StatsSet::new(),
                )
                .map(|arr| crate::Array::Data(arr))
            }
        }
    }
}

impl TakeFn for ByteBoolArray {
    fn take(&self, indices: &crate::Array) -> VortexResult<crate::Array> {
        let validity = self.validity();
        let indices = indices.clone().flatten_primitive()?;

        let bools = match_each_integer_ptype!(indices.ptype(), |$I| {
            take_byte_bool(self.as_bool_slice(), validity, indices.typed_data::<$I>())
        });

        Ok(Self::from(bools).into_array())
    }
}

fn take_byte_bool<I: AsPrimitive<usize>>(
    bools: &[bool],
    validity: Validity,
    indices: &[I],
) -> Vec<Option<bool>> {
    indices
        .iter()
        .map(|&idx| {
            let idx = idx.as_();
            if validity.is_valid(idx) {
                Some(bools[idx])
            } else {
                None
            }
        })
        .collect()
}
