use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;
use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::array::composite::array::CompositeArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::{Array, IntoArray, OwnedArray};

impl ArrayCompute for CompositeArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
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

impl AsArrowArray for CompositeArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        self.with_compute(|c| {
            c.as_arrow().map(|a| a.as_arrow()).unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "as_arrow",
                    format!("composite extension {}", self.id())
                ))
            })
        })
    }
}

impl AsContiguousFn for CompositeArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let composites = arrays
            .iter()
            .map(|array| CompositeArray::try_from(array).unwrap())
            .collect_vec();
        let underlyings = composites.iter().map(|c| c.underlying()).collect_vec();
        Ok(CompositeArray::new(
            self.id(),
            self.underlying_metadata().clone(),
            as_contiguous(&underlyings)?,
        )
        .into_array())
    }
}

impl ScalarAtFn for CompositeArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // TODO(ngates): this seems wrong... I don't think we just cast scalars like this.
        //  e.g. how do we know what a datetime is in?
        let underlying = scalar_at(&self.underlying(), index)?;
        underlying.cast(self.dtype())
    }
}

impl TakeFn for CompositeArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        Ok(CompositeArray::new(
            self.id(),
            self.underlying_metadata().clone(),
            take(&self.underlying(), indices)?,
        )
        .into_array())
    }
}

impl SliceFn for CompositeArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(CompositeArray::new(
            self.id(),
            self.underlying_metadata().clone(),
            slice(&self.underlying(), start, stop)?,
        )
        .into_array())
    }
}
