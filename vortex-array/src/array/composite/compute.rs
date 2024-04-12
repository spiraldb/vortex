use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;
use vortex_error::{vortex_err, VortexResult};

use crate::array::composite::array::CompositeArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
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

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsArrowArray for CompositeArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        self.extension()
            .as_typed_compute(self)
            .as_arrow()
            .map(|a| a.as_arrow())
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "as_arrow",
                    format!("composite extension {}", self.id())
                ))
            })
    }
}

impl AsContiguousFn for CompositeArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        let composites = arrays
            .iter()
            .map(|array| array.as_composite().underlying())
            .cloned()
            .collect_vec();
        Ok(CompositeArray::new(
            self.id(),
            self.metadata().clone(),
            as_contiguous(&composites)?,
        )
        .into_array())
    }
}

impl FlattenFn for CompositeArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Composite(self.clone()))
    }
}

impl ScalarAtFn for CompositeArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // TODO(ngates): this seems wrong... I don't think we just cast scalars like this.
        //  e.g. how do we know what a datetime is in?
        let underlying = scalar_at(self.underlying(), index)?;
        underlying.cast(self.dtype())
    }
}

impl TakeFn for CompositeArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(CompositeArray::new(
            self.id(),
            self.metadata().clone(),
            take(self.underlying(), indices)?,
        )
        .into_array())
    }
}

impl SliceFn for CompositeArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(CompositeArray::new(
            self.id(),
            self.metadata().clone(),
            slice(self.underlying(), start, stop)?,
        )
        .into_array())
    }
}
