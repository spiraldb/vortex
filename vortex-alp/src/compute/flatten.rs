use std::cmp::min;
use vortex::array::{Array, ArrayRef};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::cast::cast;
use vortex::compute::flatten::{flatten_primitive, FlattenedArray, FlattenFn};
use vortex::compute::patch::PatchFn;
use vortex::compute::scalar_at::ScalarAtFn;
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::PType;

use crate::ALPArray;

impl FlattenFn for ALPArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let ints = flatten_primitive(self.encoded().as_ref())?;
        match self.dtype() {
            PType::I32 => Ok(FlattenedArray::Primitive(
                PrimitiveArray::from(ints.typed_data::<f32>()).boxed(),
            )),
            PType::I64 => Ok(FlattenedArray::Primitive(
                PrimitiveArray::from(ints.typed_data::<f64>()).boxed(),
            )),
            _ => return Err(VortexError::InvalidPType(ints.ptype())),
        }
        let ends: PrimitiveArray =
            flatten_primitive(cast(self.ends(), &PType::U64.into())?.as_ref())?
                .typed_data::<u64>()
                .iter()
                .map(|v| v - self.offset() as u64)
                .map(|v| min(v, self.len() as u64))
                .take_while(|v| *v <= (self.len() as u64))
                .collect::<Vec<u64>>()
                .into();
    }
}
