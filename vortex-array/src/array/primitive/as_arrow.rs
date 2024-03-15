use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::arrow::as_arrow::AsArrowArray;
use crate::compute::flatten::flatten_bool;
use crate::error::VortexResult;
use crate::ptype::PType;
use arrow_array::{
    ArrayRef as ArrowArrayRef, ArrowPrimitiveType, PrimitiveArray as ArrowPrimitiveArray,
};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use std::sync::Arc;

impl AsArrowArray for PrimitiveArray {
    fn as_arrow_array(&self) -> VortexResult<ArrowArrayRef> {
        use arrow_array::types::*;
        Ok(match self.ptype() {
            PType::U8 => Arc::new(as_arrow_array_primitive::<UInt8Type>(self)?),
            PType::U16 => Arc::new(as_arrow_array_primitive::<UInt16Type>(self)?),
            PType::U32 => Arc::new(as_arrow_array_primitive::<UInt32Type>(self)?),
            PType::U64 => Arc::new(as_arrow_array_primitive::<UInt64Type>(self)?),
            PType::I8 => Arc::new(as_arrow_array_primitive::<Int8Type>(self)?),
            PType::I16 => Arc::new(as_arrow_array_primitive::<Int16Type>(self)?),
            PType::I32 => Arc::new(as_arrow_array_primitive::<Int32Type>(self)?),
            PType::I64 => Arc::new(as_arrow_array_primitive::<Int64Type>(self)?),
            PType::F16 => Arc::new(as_arrow_array_primitive::<Float16Type>(self)?),
            PType::F32 => Arc::new(as_arrow_array_primitive::<Float32Type>(self)?),
            PType::F64 => Arc::new(as_arrow_array_primitive::<Float64Type>(self)?),
        })
    }
}

fn as_arrow_array_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray,
) -> VortexResult<ArrowPrimitiveArray<T>> {
    let nulls = array
        .validity()
        .map(|v| flatten_bool(v))
        .transpose()?
        .map(|b| NullBuffer::new(b.buffer().clone()));

    Ok(ArrowPrimitiveArray::new(
        ScalarBuffer::<T::Native>::new(array.buffer().clone(), 0, array.len()),
        nulls,
    ))
}
