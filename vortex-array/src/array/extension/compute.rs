use arrow_array::ArrayRef as ArrowArrayRef;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::array::datetime::LocalDateTimeArray;
use crate::array::extension::ExtensionArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::cast::CastFn;
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::{Array, IntoArray, OwnedArray, ToStatic};

impl ArrayCompute for ExtensionArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

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

impl AsArrowArray for ExtensionArray {
    /// To support full compatability with Arrow, we hard-code the conversion of our datetime
    /// arrays to Arrow's Timestamp arrays here. For all other extension arrays, we return an
    /// Arrow extension array with the same definition.
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        match self.id().as_ref() {
            "vortex.localdatetime" => LocalDateTimeArray::try_from(self)?.as_arrow(),
            _ => vortex_bail!("Arrow extension arrays not yet supported"),
        }
    }
}

impl AsContiguousFn for ExtensionArray {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let storage_arrays = arrays
            .iter()
            .map(|a| {
                ExtensionArray::try_from(a)
                    .expect("not an extension array")
                    .storage()
                    .to_static()
            })
            .collect::<Vec<_>>();

        Ok(
            ExtensionArray::new(self.ext_dtype().clone(), as_contiguous(&storage_arrays)?)
                .into_array(),
        )
    }
}

impl ScalarAtFn for ExtensionArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(Scalar::extension(
            self.ext_dtype().clone(),
            scalar_at(&self.storage(), index)?,
        ))
    }
}

impl SliceFn for ExtensionArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(ExtensionArray::new(
            self.ext_dtype().clone(),
            slice(&self.storage(), start, stop)?,
        )
        .into_array())
    }
}

impl TakeFn for ExtensionArray {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        Ok(
            ExtensionArray::new(self.ext_dtype().clone(), take(&self.storage(), indices)?)
                .into_array(),
        )
    }
}
