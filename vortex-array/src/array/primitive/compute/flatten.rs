use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::FlattenPrimitiveFn;
use crate::error::VortexResult;

impl FlattenPrimitiveFn for PrimitiveArray {
    fn flatten_primitive(&self) -> VortexResult<PrimitiveArray> {
        Ok(self.clone())
    }
}
