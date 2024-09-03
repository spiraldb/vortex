use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::extension::ExtensionArray;
use crate::compute::unary::{scalar_at, scalar_at_unchecked, CastFn, ScalarAtFn};
use crate::compute::{slice, take, ArrayCompute, SliceFn, TakeFn};
use crate::{Array, IntoArray};

impl ArrayCompute for ExtensionArray {
    fn cast(&self) -> Option<&dyn CastFn> {
        // It's not possible to cast an extension array to another type.
        // TODO(ngates): we should allow some extension arrays to implement a callback
        //  to support this
        None
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

impl ScalarAtFn for ExtensionArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(Scalar::extension(
            self.ext_dtype().clone(),
            scalar_at(&self.storage(), index)?,
        ))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        Scalar::extension(
            self.ext_dtype().clone(),
            scalar_at_unchecked(&self.storage(), index),
        )
    }
}

impl SliceFn for ExtensionArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::new(
            self.ext_dtype().clone(),
            slice(&self.storage(), start, stop)?,
        )
        .into_array())
    }
}

impl TakeFn for ExtensionArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Ok(Self::new(self.ext_dtype().clone(), take(&self.storage(), indices)?).into_array())
    }
}
