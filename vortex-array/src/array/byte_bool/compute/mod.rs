use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::BooleanBufferBuilder;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use super::{ByteBoolArray, ByteBoolMetadata};
use crate::{
    compute::{as_arrow::AsArrowArray, scalar_at::ScalarAtFn, slice::SliceFn, ArrayCompute},
    encoding::ArrayEncodingRef,
    stats::StatsSet,
    validity::ArrayValidity,
    ArrayDType, ArrayData, ArrayTrait,
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

        // Safety: bool and u8 are the same size. We don't care about logically null values here.
        let bool_slice = unsafe { std::mem::transmute::<_, &[bool]>(self.buffer().as_slice()) };

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
                    validity: validity.to_metadata(length).unwrap(),
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
