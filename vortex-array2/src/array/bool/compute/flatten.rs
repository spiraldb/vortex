use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::ToArrayData;

impl FlattenFn for BoolArray<'_> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Bool(
            self.to_array_data().into_typed_data().unwrap(),
        ))
    }
}
