use arrow_buffer::ScalarBuffer;
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

        let buffer = ScalarBuffer::from_iter(arrays.iter().flat_map(|a| {
            a.to_typed_array::<PrimitiveDef>()
                .unwrap()
                .typed_data::<u16>()
                .into_iter()
                .copied()
        }));

        Ok(PrimitiveData::try_new(buffer, validity)
            .unwrap()
            .into_array())
    }
}
