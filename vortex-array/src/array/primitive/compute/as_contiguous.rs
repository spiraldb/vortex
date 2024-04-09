use itertools::Itertools;
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::ptype::NativePType;
use crate::validity::ArrayValidity;
use crate::validity::Validity;

impl<T: NativePType> AsContiguousFn for &dyn PrimitiveTrait<T> {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        if !arrays
            .iter()
            .map(|chunk| chunk.as_primitive().ptype())
            .all_equal()
        {
            vortex_bail!(ComputeError: "Chunks have differing ptypes");
        }

        let validity = if self.dtype().is_nullable() {
            Some(Validity::from_iter(
                arrays.iter().map(|v| v.logical_validity()),
            ))
        } else {
            None
        };

        Ok(PrimitiveArray::from_nullable_in(
            native_primitive_as_contiguous(
                arrays
                    .iter()
                    .map(|a| a.as_primitive().typed_data::<T>())
                    .collect(),
            ),
            validity,
        )
        .into_array())
    }
}

fn native_primitive_as_contiguous<P: NativePType>(arrays: Vec<&[P]>) -> AlignedVec<P> {
    let len = arrays.iter().map(|a| a.len()).sum();
    let mut result = AlignedVec::with_capacity_in(len, ALIGNED_ALLOCATOR);
    arrays.iter().for_each(|arr| result.extend_from_slice(arr));
    result
}
