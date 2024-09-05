use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE_BYTES};
use crate::compute::unary::ScalarAtFn;
use crate::compute::{slice, ArrayCompute, SliceFn};
use crate::{Array, ArrayDType, IntoArray};

impl ArrayCompute for VarBinViewArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
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
