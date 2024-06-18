use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::{slice, SliceFn};
use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, IntoArray, IntoArrayData};

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
        if self.is_valid(index) {
            self.bytes_at(index)
                .map(|bytes| varbin_scalar(bytes, self.dtype()))
        } else {
            Ok(Scalar::null(self.dtype().clone()))
        }
    }
}

impl SliceFn for VarBinViewArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.views(), start * VIEW_SIZE, stop * VIEW_SIZE)?
                .into_array_data()
                .into_array(),
            (0..self.metadata().n_children)
                .map(|i| self.bytes(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}
