use crate::array::primitive::PrimitiveArray;
use crate::compute::cast::CastPrimitiveFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;

mod cast;
mod patch;
mod scalar_at;

impl ArrayCompute for PrimitiveArray {
    fn cast_primitive(&self) -> Option<&dyn CastPrimitiveFn> {
        Some(self)
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}
