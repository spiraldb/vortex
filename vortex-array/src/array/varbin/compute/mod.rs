use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

mod slice;
mod take;

impl ArrayCompute for VarBinArray {
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

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            Ok(varbin_scalar(
                self.bytes_at(index)?
                    // TODO(ngates): update to use buffer when we refactor scalars.
                    .into_vec()
                    .unwrap_or_else(|b| b.as_ref().to_vec()),
                self.dtype(),
            ))
        } else {
            Ok(Scalar::null(self.dtype().clone()))
        }
    }
}
