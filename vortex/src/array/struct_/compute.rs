use crate::array::struct_::StructArray;
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{Scalar, StructScalar};
use itertools::Itertools;

impl ArrayCompute for StructArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        Ok(StructScalar::new(
            self.dtype.clone(),
            self.fields
                .iter()
                .map(|field| scalar_at(field.as_ref(), index))
                .try_collect()?,
        )
        .boxed())
    }
}
