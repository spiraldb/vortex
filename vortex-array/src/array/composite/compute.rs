use itertools::Itertools;

use crate::array::composite::CompositeArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::Scalar;

impl ArrayCompute for CompositeArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for CompositeArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        Ok(CompositeArray::new(
            self.id(),
            self.metadata().clone(),
            as_contiguous(
                arrays
                    .into_iter()
                    .map(|array| dyn_clone::clone_box(array.as_composite().underlying()))
                    .collect_vec(),
            )?,
        )
        .boxed())
    }
}

impl FlattenFn for CompositeArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Composite(self.clone()))
    }
}

impl ScalarAtFn for CompositeArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let underlying = scalar_at(self.underlying(), index)?;
        underlying.cast(self.dtype())
    }
}
