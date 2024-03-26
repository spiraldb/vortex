use crate::bitpacking::compress::bitunpack;
use crate::BitPackedArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{flatten, FlattenFn, FlattenedArray};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex_error::VortexResult;

impl ArrayCompute for BitPackedArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for BitPackedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        bitunpack(self).map(FlattenedArray::Primitive)
    }
}

impl TakeFn for BitPackedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        take(&flatten(self)?.into_array(), indices)
    }
}
