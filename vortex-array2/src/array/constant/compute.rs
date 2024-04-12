use itertools::Itertools;
use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::array::constant::ConstantArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::{Array, IntoArray, OwnedArray};

impl ArrayCompute for ConstantArray<'_> {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsContiguousFn for ConstantArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let chunks = arrays
            .iter()
            .map(|a| ConstantArray::try_from(a).unwrap())
            .collect_vec();

        if chunks.iter().map(|c| c.scalar()).all_equal() {
            Ok(ConstantArray::new(
                chunks.first().unwrap().scalar().clone(),
                chunks.iter().map(|c| c.len()).sum(),
            )
            .into_array())
        } else {
            // TODO(ngates): we need to flatten the constant arrays and then concatenate them
            Err(vortex_err!(
                "Cannot concatenate constant arrays with differing scalars"
            ))
        }
    }
}

impl ScalarAtFn for ConstantArray<'_> {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar().clone())
    }
}

impl TakeFn for ConstantArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        Ok(ConstantArray::new(self.scalar().clone(), indices.len()).into_array())
    }
}
