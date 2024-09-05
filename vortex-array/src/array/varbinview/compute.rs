use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE_BYTES};
use crate::arrow::FromArrowArray;
use crate::compute::unary::ScalarAtFn;
use crate::compute::{slice, ArrayCompute, SliceFn, TakeFn};
use crate::{Array, ArrayDType, IntoArray, IntoCanonical};

impl ArrayCompute for VarBinViewArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        self.bytes_at(index)
            .map(|bytes| varbin_scalar(Buffer::from(bytes), self.dtype()))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).unwrap()
    }
}

impl SliceFn for VarBinViewArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(
                &self.views(),
                start * VIEW_SIZE_BYTES,
                stop * VIEW_SIZE_BYTES,
            )?,
            (0..self.metadata().buffer_lens.len())
                .map(|i| self.buffer(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}

/// Take involves creating a new array that references the old array, just with the given set of views.
impl TakeFn for VarBinViewArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let array_arrow = self.clone().into_array().into_canonical()?.into_arrow();
        let indices_arrow = indices.clone().into_canonical()?.into_arrow();

        let take_arrow = arrow_select::take::take(&array_arrow, &indices_arrow, None)?;
        let nullable = take_arrow.is_nullable();
        Ok(Array::from_arrow(take_arrow, nullable))
    }
}
