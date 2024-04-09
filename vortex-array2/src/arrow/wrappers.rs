use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use vortex::ptype::NativePType;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::flatten::flatten_bool;
use crate::validity::Validity;

pub fn as_scalar_buffer<T: NativePType>(array: PrimitiveArray) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(array.buffer().clone())
}

pub fn as_offset_buffer<T: NativePType>(array: PrimitiveArray) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}

pub fn as_nulls(validity: Validity) -> VortexResult<Option<NullBuffer>> {
    match validity {
        Validity::NonNullable => Ok(None),
        Validity::AllValid => Ok(None),
        Validity::AllInvalid => Ok(Some(NullBuffer::new_null(validity.as_view().len()))),
        Validity::Array(a) => {
            let bool_data = flatten_bool(&a)?;
            Ok(Some(NullBuffer::new(bool_data.as_ref().buffer())))
        }
    }
}
