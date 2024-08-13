use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{ArrayCompute, SearchSortedFn, SliceFn, TakeFn};
use vortex::ArrayDType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::bitpacking::compress::unpack_single;
use crate::BitPackedArray;

mod search_sorted;
mod slice;
mod take;

impl ArrayCompute for BitPackedArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for BitPackedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some(patches) = self.patches() {
            // NB: All non-null values are considered patches
            if self.bit_width() == 0 || patches.with_dyn(|a| a.is_valid(index)) {
                return scalar_at(&patches, index)?.cast(self.dtype());
            }
        }

        if !self.validity().is_valid(index) {
            return Ok(Scalar::null(self.dtype().clone()));
        }

        unpack_single(self, index)?.cast(self.dtype())
    }
}
