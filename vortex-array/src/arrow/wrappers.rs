use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::compute::flatten::flatten_bool;
use crate::error::VortexResult;
use crate::ptype::NativePType;
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};

pub fn as_scalar_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> ScalarBuffer<T> {
    assert_eq!(array.ptype(), &T::PTYPE);
    ScalarBuffer::from(array.buffer().clone())
}

pub fn as_offset_buffer<T: NativePType + ArrowNativeType>(
    array: PrimitiveArray,
) -> OffsetBuffer<T> {
    OffsetBuffer::new(as_scalar_buffer(array))
}

pub fn as_nulls(validity: Option<&dyn Array>) -> VortexResult<Option<NullBuffer>> {
    Ok(validity
        .map(|v| flatten_bool(v))
        .transpose()?
        .map(|b| NullBuffer::new(b.buffer().clone())))
}
