use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::error::VortexResult;

impl FlattenFn for PrimitiveArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Primitive(self.clone()))
    }
}
