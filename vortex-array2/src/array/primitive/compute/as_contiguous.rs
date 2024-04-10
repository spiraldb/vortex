use arrow_buffer::ScalarBuffer;
use vortex::match_each_native_ptype;
use vortex_error::VortexResult;

use crate::array::primitive::{PrimitiveArray, PrimitiveData, PrimitiveDef};
use crate::compute::as_contiguous::AsContiguousFn;
use crate::validity::Validity;
use crate::{Array, ArrayTrait, IntoArray, OwnedArray, WithArray};

impl AsContiguousFn for PrimitiveArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(
                arrays
                    .iter()
                    .map(|a| a.with_array(|a| a.logical_validity())),
            )
        } else {
            Validity::NonNullable
        };

        match_each_native_ptype!(self.ptype(), |$T| {
            let mut values: Vec<$T> = Vec::with_capacity(arrays.iter().map(|a| a.len()).sum());
            for array in arrays {
                values.extend(
                    array
                        .to_typed_array::<PrimitiveDef>()
                        .unwrap()
                        .typed_data::<$T>(),
                )
            }
            Ok(PrimitiveData::try_new(ScalarBuffer::from(values), validity)
                .unwrap()
                .into_array())
        })
    }
}
