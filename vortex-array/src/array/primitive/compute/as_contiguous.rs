use arrow_buffer::{MutableBuffer, ScalarBuffer};
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::match_each_native_ptype;
use crate::validity::Validity;
use crate::{Array, IntoArray, OwnedArray};

impl AsContiguousFn for PrimitiveArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(arrays.iter().map(|a| a.with_dyn(|a| a.logical_validity())))
        } else {
            Validity::NonNullable
        };

        let mut buffer = MutableBuffer::with_capacity(
            arrays.iter().map(|a| a.len()).sum::<usize>() * self.ptype().byte_width(),
        );
        for array in arrays {
            buffer.extend_from_slice(PrimitiveArray::try_from(array).unwrap().buffer().as_slice())
        }
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(PrimitiveArray::try_new(ScalarBuffer::<$T>::from(buffer), validity)
                .unwrap()
                .into_array())
        })
    }
}
