use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::compute::unary::ScalarAtFn;
use crate::compute::{ArrayCompute, MaybeCompareFn, Operator, SliceFn, TakeFn};
use crate::{Array, ArrayDType};

mod compare;
mod filter;
mod slice;
mod take;

impl ArrayCompute for VarBinArray {
    fn compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        MaybeCompareFn::maybe_compare(self, other, operator)
    }

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
        varbin_scalar(self.bytes_at(index).unwrap(), self.dtype())
    }
}
