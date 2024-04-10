use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::{FlattenFn, FlattenedData};
use crate::ToArrayData;

impl FlattenFn for PrimitiveArray<'_> {
    fn flatten(&self) -> VortexResult<FlattenedData> {
        Ok(FlattenedData::Primitive(
            self.to_array_data().into_typed_data().unwrap(),
        ))
    }
}
