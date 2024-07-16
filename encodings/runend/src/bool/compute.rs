use vortex::array::bool::BoolArray;
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::TakeFn;
use vortex::compute::unary::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::{Array, IntoArray, IntoArrayVariant, ToArray};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::bool::compress::value_at_index;
use crate::bool::RunEndBoolArray;

impl ArrayCompute for RunEndBoolArray {
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

impl ScalarAtFn for RunEndBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let start = self.start();
        Ok(Scalar::from(value_at_index(
            self.find_physical_index(index)?,
            start,
        )))
    }
}

impl TakeFn for RunEndBoolArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let primitive_indices = indices.clone().into_primitive()?;
        let physical_indices = match_each_integer_ptype!(primitive_indices.ptype(), |$P| {
            primitive_indices
                .maybe_null_slice::<$P>()
                .iter()
                .map(|idx| {
                    self.find_physical_index(*idx as usize)
                })
                .collect::<VortexResult<Vec<_>>>()?
        });
        let start = self.start();
        Ok(BoolArray::from(
            physical_indices
                .iter()
                .map(|&it| value_at_index(it, start))
                .collect::<Vec<_>>(),
        )
        .to_array())
    }
}

impl SliceFn for RunEndBoolArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let slice_begin = self.find_physical_index(start)?;
        let slice_end = self.find_physical_index(stop)?;

        Ok(Self::with_offset_and_size(
            slice(&self.ends(), slice_begin, slice_end + 1)?,
            value_at_index(slice_begin, self.start()),
            self.validity().slice(slice_begin, slice_end + 1)?,
            stop - start,
            start,
        )?
        .into_array())
    }
}
