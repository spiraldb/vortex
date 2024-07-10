use vortex::compute::search_sorted::SearchSortedFn;
use vortex::compute::slice::SliceFn;
use vortex::compute::take::TakeFn;
use vortex::compute::unary::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::ArrayDType;
use vortex_error::{vortex_err, VortexResult};
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
        if index >= self.len() {
            return Err(vortex_err!(OutOfBounds: index, 0, self.len()));
        }
        if let Some(patches) = self.patches() {
            // NB: All non-null values are considered patches
            if self.bit_width() == 0 || patches.with_dyn(|a| a.is_valid(index)) {
                return scalar_at(&patches, index)?.cast(self.dtype());
            }
        }
        unpack_single(self, index)?.cast(self.dtype())
    }
}
