use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::flatten::{FlattenFn, FlattenedData};
use crate::ToArrayData;

impl FlattenFn for BoolArray<'_> {
    fn flatten(&self) -> VortexResult<FlattenedData> {
        Ok(FlattenedData::Bool(
            self.to_array_data().into_typed_data().unwrap(),
        ))
    }
}
