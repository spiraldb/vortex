use arrow2::array::{
    Array as ArrowArray, BinaryArray as ArrowBinaryArray, BooleanArray as ArrowBooleanArray,
    PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
    Utf8Array as ArrowUtf8Array,
};
use arrow2::datatypes::PhysicalType;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use arrow2::with_match_primitive_without_interval_type;

use crate::array::binary::VarBinArray;
use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::Array;

impl<T: NativeType> From<ArrowPrimitiveArray<T>> for PrimitiveArray {
    fn from(value: ArrowPrimitiveArray<T>) -> Self {
        PrimitiveArray::new(Box::new(value))
    }
}

impl<O: Offset> From<ArrowUtf8Array<O>> for VarBinArray {
    fn from(value: ArrowUtf8Array<O>) -> Self {
        VarBinArray::new(Box::new(value))
    }
}

impl<O: Offset> From<ArrowBinaryArray<O>> for VarBinArray {
    fn from(value: ArrowBinaryArray<O>) -> Self {
        VarBinArray::new(Box::new(value))
    }
}

impl From<ArrowBooleanArray> for BoolArray {
    fn from(value: ArrowBooleanArray) -> Self {
        BoolArray::new(Box::new(value))
    }
}

impl From<ArrowStructArray> for StructArray {
    fn from(value: ArrowStructArray) -> Self {
        StructArray::new(
            ArrowStructArray::get_fields(value.data_type())
                .iter()
                .map(|f| f.name.clone())
                .collect(),
            value.values().iter().map(|v| v.as_ref().into()).collect(),
        )
    }
}

impl From<&dyn ArrowArray> for Array {
    // TODO(robert): Wrap in a TypedArray if physical type is different than the logical type, eg. datetime
    fn from(array: &dyn ArrowArray) -> Self {
        match array.data_type().to_physical_type() {
            PhysicalType::Boolean => {
                let bool_array: BoolArray = array
                    .as_any()
                    .downcast_ref::<ArrowBooleanArray>()
                    .unwrap()
                    .clone()
                    .into();
                bool_array.into()
            }
            PhysicalType::Primitive(prim) => {
                with_match_primitive_without_interval_type!(prim, |$T| {
                    let primitive_array: PrimitiveArray = array
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<$T>>()
                        .unwrap()
                        .clone()
                        .into();
                    primitive_array.into()
                })
            }
            PhysicalType::Utf8 => {
                let utf8_array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowUtf8Array<i32>>()
                    .unwrap()
                    .clone()
                    .into();
                utf8_array.into()
            }
            PhysicalType::LargeUtf8 => {
                let utf8_array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowUtf8Array<i64>>()
                    .unwrap()
                    .clone()
                    .into();
                utf8_array.into()
            }
            PhysicalType::Binary => {
                let binary_array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowBinaryArray<i32>>()
                    .unwrap()
                    .clone()
                    .into();
                binary_array.into()
            }
            PhysicalType::LargeBinary => {
                let binary_array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowBinaryArray<i64>>()
                    .unwrap()
                    .clone()
                    .into();
                binary_array.into()
            }
            PhysicalType::Struct => {
                let struct_array: StructArray = array
                    .as_any()
                    .downcast_ref::<ArrowStructArray>()
                    .unwrap()
                    .clone()
                    .into();
                struct_array.into()
            }
            _ => panic!("TODO(robert): Implement more"),
        }
    }
}
