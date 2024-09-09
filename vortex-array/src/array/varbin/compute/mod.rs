use vortex_error::{VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::compute::unary::ScalarAtFn;
use crate::compute::{ArrayCompute, SliceFn, TakeFn};
use crate::ArrayDType;

mod filter;
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
        Ok(varbin_scalar(self.bytes_at(index)?, self.dtype()))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        varbin_scalar(self.bytes_at(index).vortex_unwrap(), self.dtype())
    }
}
