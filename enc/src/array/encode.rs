use arrow2::array::{
    Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray, Utf8Array as ArrowUtf8Array,
};
use arrow2::datatypes::PhysicalType;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use arrow2::with_match_primitive_without_interval_type;

use crate::array::binary::VarBinArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::Array;

impl<T: NativeType> From<ArrowPrimitiveArray<T>> for PrimitiveArray {
    fn from(value: ArrowPrimitiveArray<T>) -> Self {
        PrimitiveArray::new(Box::new(value))
    }
}

impl<T: NativeType> From<&ArrowPrimitiveArray<T>> for PrimitiveArray {
    fn from(value: &ArrowPrimitiveArray<T>) -> Self {
        value.clone().into()
    }
}

impl<O: Offset> From<ArrowUtf8Array<O>> for VarBinArray {
    fn from(value: ArrowUtf8Array<O>) -> Self {
        VarBinArray::new(Box::new(value))
    }
}

impl<O: Offset> From<&ArrowUtf8Array<O>> for VarBinArray {
    fn from(value: &ArrowUtf8Array<O>) -> Self {
        value.clone().into()
    }
}

impl From<&dyn ArrowArray> for Array {
    // TODO(robert): Wrap in a TypedArray if physical type is different than the logical type, eg. datetime
    fn from(array: &dyn ArrowArray) -> Self {
        match array.data_type().to_physical_type() {
            PhysicalType::Primitive(prim) => {
                with_match_primitive_without_interval_type!(prim, |$T| {
                    let primitive_array: PrimitiveArray = array
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<$T>>()
                        .unwrap()
                        .into();
                    primitive_array.into()
                })
            }
            PhysicalType::Utf8 => {
                let uf8array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowUtf8Array<i32>>()
                    .unwrap()
                    .into();
                uf8array.into()
            }
            PhysicalType::LargeUtf8 => {
                let uf8array: VarBinArray = array
                    .as_any()
                    .downcast_ref::<ArrowUtf8Array<i64>>()
                    .unwrap()
                    .into();
                uf8array.into()
            }
            _ => panic!("TODO(robert): Implement more"),
        }
    }
}
