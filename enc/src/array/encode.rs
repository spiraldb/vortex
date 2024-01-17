use std::sync::Arc;

use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{
    Array as ArrowArray, BooleanArray as ArrowBooleanArray, GenericBinaryArray as ArrowBinaryArray,
    GenericStringArray as ArrowStringArray, PrimitiveArray as ArrowPrimitiveArray,
    StructArray as ArrowStructArray,
};
use arrow::array::{ArrowPrimitiveType, OffsetSizeTrait};
use arrow::datatypes::DataType;

use crate::array::binary::VarBinArray;
use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::Array;

impl<T: ArrowPrimitiveType> From<ArrowPrimitiveArray<T>> for PrimitiveArray {
    fn from(value: ArrowPrimitiveArray<T>) -> Self {
        PrimitiveArray::new(Arc::new(value))
    }
}

impl<O: OffsetSizeTrait> From<ArrowStringArray<O>> for VarBinArray {
    fn from(value: ArrowStringArray<O>) -> Self {
        VarBinArray::new(Arc::new(value))
    }
}

impl<O: OffsetSizeTrait> From<ArrowBinaryArray<O>> for VarBinArray {
    fn from(value: ArrowBinaryArray<O>) -> Self {
        VarBinArray::new(Arc::new(value))
    }
}

impl From<ArrowBooleanArray> for BoolArray {
    fn from(value: ArrowBooleanArray) -> Self {
        BoolArray::new(Arc::new(value))
    }
}

impl From<ArrowStructArray> for StructArray {
    fn from(value: ArrowStructArray) -> Self {
        StructArray::new(
            value
                .column_names()
                .iter()
                .map(|c| (*c).to_owned())
                .collect(),
            value.columns().iter().map(|c| c.as_ref().into()).collect(),
        )
    }
}

impl From<Arc<dyn ArrowArray>> for Array {
    fn from(value: Arc<dyn ArrowArray>) -> Self {
        value.as_ref().into()
    }
}

impl From<&dyn ArrowArray> for Array {
    // TODO(robert): Wrap in a TypedArray if physical type is different than the logical type, eg. datetime
    fn from(array: &dyn ArrowArray) -> Self {
        match array.data_type() {
            DataType::Boolean => Array::Bool(array.as_boolean().clone().into()),
            DataType::UInt8 => Array::Primitive(array.as_primitive::<UInt8Type>().clone().into()),
            DataType::UInt16 => Array::Primitive(array.as_primitive::<UInt16Type>().clone().into()),
            DataType::UInt32 => Array::Primitive(array.as_primitive::<UInt32Type>().clone().into()),
            DataType::UInt64 => Array::Primitive(array.as_primitive::<UInt64Type>().clone().into()),
            DataType::Int8 => Array::Primitive(array.as_primitive::<Int8Type>().clone().into()),
            DataType::Int16 => Array::Primitive(array.as_primitive::<Int16Type>().clone().into()),
            DataType::Int32 => Array::Primitive(array.as_primitive::<Int32Type>().clone().into()),
            DataType::Int64 => Array::Primitive(array.as_primitive::<Int64Type>().clone().into()),
            DataType::Float16 => {
                Array::Primitive(array.as_primitive::<Float16Type>().clone().into())
            }
            DataType::Float32 => {
                Array::Primitive(array.as_primitive::<Float32Type>().clone().into())
            }
            DataType::Float64 => {
                Array::Primitive(array.as_primitive::<Float64Type>().clone().into())
            }
            DataType::Utf8 => Array::VarBin(array.as_string::<i32>().clone().into()),
            DataType::LargeUtf8 => Array::VarBin(array.as_string::<i64>().clone().into()),
            DataType::Binary => Array::VarBin(array.as_binary::<i32>().clone().into()),
            DataType::LargeBinary => Array::VarBin(array.as_binary::<i64>().clone().into()),
            DataType::Struct(_) => Array::Struct(array.as_struct().clone().into()),
            _ => panic!("TODO(robert): Implement more"),
        }
    }
}
