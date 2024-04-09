use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use log::Log;
use vortex::ptype::NativePType;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::validity::{LogicalValidity, Validity};

pub fn as_scalar_buffer<T: NativePType>(array: PrimitiveArray) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(array.buffer().clone())
}

pub fn as_offset_buffer<T: NativePType>(array: PrimitiveArray) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}
