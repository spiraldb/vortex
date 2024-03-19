use crate::array::composite::array::CompositeArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_arrow::{as_arrow, AsArrowArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::Scalar;
use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;

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

impl AsArrowArray for CompositeArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let typed = self.extension().as_typed_array(self);
        let _foo = format!("{:?}", typed.as_ref());
        as_arrow(typed.as_ref())
    }
}

impl AsContiguousFn for CompositeArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let composites = arrays
            .iter()
            .map(|array| array.as_composite().underlying())
            .map(dyn_clone::clone_box)
            .collect_vec();
        Ok(CompositeArray::new(
            self.id(),
            self.metadata().clone(),
            as_contiguous(composites)?,
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
        // TODO(ngates): this seems wrong...
        let underlying = scalar_at(self.underlying(), index)?;
        underlying.cast(self.dtype())
    }
}
