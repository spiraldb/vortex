use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::typed::TypedArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::Scalar;
use itertools::Itertools;

impl ArrayCompute for TypedArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for TypedArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        Ok(TypedArray::new(
            as_contiguous(
                arrays
                    .into_iter()
                    .map(|array| dyn_clone::clone_box(array.as_typed().untyped_array()))
                    .collect_vec(),
            )?,
            self.dtype().clone(),
        )
        .boxed())
    }
}

impl ScalarAtFn for TypedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let underlying = scalar_at(self.array.as_ref(), index)?;
        underlying.cast(self.dtype())
    }
}
