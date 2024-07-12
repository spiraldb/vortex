use arrow_buffer::{ArrowNativeType, OffsetBuffer, ScalarBuffer};
use vortex_dtype::NativePType;

use crate::array::primitive::PrimitiveArray;

pub fn as_scalar_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), T::PTYPE);
    ScalarBuffer::from(array.buffer().clone().into_arrow())
}

pub fn as_offset_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}
