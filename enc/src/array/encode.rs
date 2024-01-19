use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{
    Array as ArrowArray, ArrayRef, BooleanArray as ArrowBooleanArray, GenericByteArray,
    PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
};
use arrow::array::{ArrowPrimitiveType, OffsetSizeTrait};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ByteArrayType, DataType};

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::Array;
use crate::types::{DType, PType};

impl From<&Buffer> for Array {
    fn from(value: &Buffer) -> Self {
        Array::Primitive(PrimitiveArray::new(PType::U8, value.to_owned()))
    }
}

impl<O: OffsetSizeTrait> From<&OffsetBuffer<O>> for Array {
    fn from(value: &OffsetBuffer<O>) -> Self {
        let buffer = if !O::IS_LARGE {
            arrow::compute::cast(
                &arrow::array::PrimitiveArray::<UInt32Type>::new(
                    ScalarBuffer::<u32>::from(value.inner().inner().to_owned()),
                    None,
                ),
                &DataType::UInt64,
            )
            .unwrap()
            .as_primitive::<UInt64Type>()
            .values()
            .inner()
            .to_owned()
        } else {
            value.inner().inner().to_owned()
        };
        Array::Primitive(PrimitiveArray::new(PType::I64, buffer))
    }
}

impl<T: ArrowPrimitiveType> From<&ArrowPrimitiveArray<T>> for Array {
    fn from(value: &ArrowPrimitiveArray<T>) -> Self {
        let dtype: DType = T::DATA_TYPE.try_into().unwrap();
        Array::Primitive(PrimitiveArray::new(
            (&dtype).try_into().unwrap(),
            value.values().inner().to_owned(),
        ))
    }
}

impl<T: ByteArrayType> From<&GenericByteArray<T>> for Array {
    fn from(value: &GenericByteArray<T>) -> Self {
        Array::VarBin(VarBinArray::new(
            Box::new(value.offsets().into()),
            Box::new(value.values().into()),
            T::DATA_TYPE.try_into().unwrap(),
        ))
    }
}

impl From<&ArrowBooleanArray> for Array {
    fn from(value: &ArrowBooleanArray) -> Self {
        Array::Bool(BoolArray::new(value.values().to_owned()))
    }
}

impl From<&ArrowStructArray> for Array {
    fn from(value: &ArrowStructArray) -> Self {
        Array::Struct(StructArray::new(
            value.column_names(),
            value
                .columns()
                .iter()
                .map(|c| (*c).to_owned().into())
                .collect(),
        ))
    }
}

impl From<ArrayRef> for Array {
    // TODO(robert): Wrap in a TypedArray if physical type is different than the logical type, eg. datetime
    fn from(array: ArrayRef) -> Self {
        match array.data_type() {
            DataType::Boolean => array.as_boolean().into(),
            DataType::UInt8 => array.as_primitive::<UInt8Type>().into(),
            DataType::UInt16 => array.as_primitive::<UInt16Type>().into(),
            DataType::UInt32 => array.as_primitive::<UInt32Type>().into(),
            DataType::UInt64 => array.as_primitive::<UInt64Type>().into(),
            DataType::Int8 => array.as_primitive::<Int8Type>().into(),
            DataType::Int16 => array.as_primitive::<Int16Type>().into(),
            DataType::Int32 => array.as_primitive::<Int32Type>().into(),
            DataType::Int64 => array.as_primitive::<Int64Type>().into(),
            DataType::Float16 => array.as_primitive::<Float16Type>().into(),
            DataType::Float32 => array.as_primitive::<Float32Type>().into(),
            DataType::Float64 => array.as_primitive::<Float64Type>().into(),
            DataType::Utf8 => array.as_string::<i32>().into(),
            DataType::LargeUtf8 => array.as_string::<i64>().into(),
            DataType::Binary => array.as_binary::<i32>().into(),
            DataType::LargeBinary => array.as_binary::<i64>().into(),
            DataType::Struct(_) => array.as_struct().into(),
            _ => panic!("TODO(robert): Implement more"),
        }
    }
}
