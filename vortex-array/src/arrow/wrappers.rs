use arrow_buffer::{Buffer as ArrowBuffer, OffsetBuffer, ScalarBuffer};

use crate::array::primitive::PrimitiveArray;
use crate::ptype::NativePType;

pub fn as_scalar_buffer<T: NativePType>(array: PrimitiveArray<'_>) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(ArrowBuffer::from(array.buffer()))
}

pub fn as_offset_buffer<T: NativePType>(array: PrimitiveArray<'_>) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}
