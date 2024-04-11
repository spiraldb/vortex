use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::ToArrayData;

impl FlattenFn for PrimitiveArray<'_> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        // FIXME(ngates): avoid allocating as array data.
        Ok(FlattenedArray::Primitive(
            self.to_array_data().into_typed_data().unwrap(),
        ))
    }
}
