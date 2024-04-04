use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::ptype::NativePType;

impl<T: NativePType> FlattenFn for &dyn PrimitiveTrait<T> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Primitive(self.to_primitive()))
    }
}
