use arrow_buffer::{MutableBuffer, ScalarBuffer};
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

        let mut buffer = MutableBuffer::with_capacity(
            arrays.iter().map(|a| a.len()).sum::<usize>() * self.ptype().byte_width(),
        );
        for array in arrays {
            array.with_typed_array::<PrimitiveDef, _, _>(|p| {
                buffer.extend_from_slice(p.buffer().as_slice())
            })
        }
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(PrimitiveData::try_new(ScalarBuffer::<$T>::from(buffer), validity)
                .unwrap()
                .into_array())
        })
    }
}
