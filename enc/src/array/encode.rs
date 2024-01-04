use arrow2::array::{Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray};
use arrow2::datatypes::PhysicalType;
use arrow2::types::NativeType;
use arrow2::with_match_primitive_without_interval_type;

use crate::array::primitive::PrimitiveArray;
use crate::array::Array;

impl<T: NativeType> From<&ArrowPrimitiveArray<T>> for PrimitiveArray {
    fn from(value: &ArrowPrimitiveArray<T>) -> Self {
        PrimitiveArray::new(value)
    }
}

impl From<&dyn ArrowArray> for Box<dyn Array> {
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
                    primitive_array.boxed()
                })
            }
            _ => panic!("TODO(robert): Implement more"),
        }
    }
}
