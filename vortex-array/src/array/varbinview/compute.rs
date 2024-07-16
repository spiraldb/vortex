use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE};
use crate::compute::compare::CompareFn;
use crate::compute::slice::{slice, SliceFn};
use crate::compute::unary::scalar_at::{self, ScalarAtFn};
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

    fn compare(&self) -> Option<&dyn CompareFn> {
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
            (0..self.metadata().data_lens.len())
                .map(|i| self.bytes(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}

impl CompareFn for VarBinViewArray {
    fn compare(&self, other: &Array, predicate: vortex_expr::Operator) -> VortexResult<Array> {
        if self.len() == other.len() {
            for idx in 0..self.len() {
                let lhs = self.scalar_at(index)?;
                let rhs = scalar_at(other, idx)?;
            }

        }
        let lhs = self.scalar_at(index)
    }
}
