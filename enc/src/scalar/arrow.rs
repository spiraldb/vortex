use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::Scalar as ArrowScalar;
use arrow::array::{Datum, PrimitiveArray};

use crate::scalar::{PScalar, Scalar};

impl From<&dyn Scalar> for Box<dyn Datum> {
    fn from(value: &dyn Scalar) -> Self {
        if let Some(pscalar) = value.as_any().downcast_ref::<PScalar>() {
            return match pscalar {
                PScalar::U8(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<UInt8Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::U16(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<UInt16Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::U32(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<UInt32Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::U64(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<UInt64Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::I8(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Int8Type>::from(vec![*v])))
                }
                PScalar::I16(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Int16Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::I32(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Int32Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::I64(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Int64Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::F16(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Float16Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::F32(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Float32Type>::from(vec![
                        *v,
                    ])))
                }
                PScalar::F64(v) => {
                    Box::new(ArrowScalar::new(PrimitiveArray::<Float64Type>::from(vec![
                        *v,
                    ])))
                }
            };
        }

        todo!("implement other scalar types {:?}", value)
    }
}
