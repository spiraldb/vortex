use arrow_buffer::{ArrowNativeType, Buffer as ArrowBuffer, OffsetBuffer, ScalarBuffer};
use vortex_dtype::NativePType;

use crate::array::primitive::PrimitiveArray;

pub fn as_scalar_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(ArrowBuffer::from(array.buffer()))
}

pub fn as_offset_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}
