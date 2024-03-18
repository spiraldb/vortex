use std::cmp::min;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, CloneOptionalArray};
use vortex::compute::cast::cast;
use vortex::compute::flatten::{flatten, flatten_primitive, FlattenPrimitiveFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::PType;
use vortex::scalar::Scalar;

use crate::compress::ree_decode;
use crate::REEArray;

impl ArrayCompute for REEArray {
    fn flatten_primitive(&self) -> Option<&dyn FlattenPrimitiveFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl FlattenPrimitiveFn for REEArray {
    fn flatten_primitive(&self) -> VortexResult<PrimitiveArray> {
        let ends: PrimitiveArray =
            flatten_primitive(cast(self.ends(), &PType::U64.into())?.as_ref())?
                .typed_data::<u64>()
                .iter()
                .map(|v| v - self.offset() as u64)
                .map(|v| min(v, self.len() as u64))
                .take_while(|v| *v <= (self.len() as u64))
                .collect::<Vec<u64>>()
                .into();

        let values = flatten(self.values())?;
        if let FlattenedArray::Primitive(pvalues) = values {
            ree_decode(&ends, &pvalues, self.validity().clone_optional())
        } else {
            Err(VortexError::InvalidArgument(
                "Cannot yet flatten non-primitive REE array".into(),
            ))
        }
    }
}

impl ScalarAtFn for REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(self.values(), self.find_physical_index(index)?)
    }
}
