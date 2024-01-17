use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::Scalar as ArrowScalar;
use arrow::array::{Array, Datum, PrimitiveArray};
use arrow::datatypes::DataType;

use crate::scalar::{BoolScalar, NullableScalar, PScalar, Scalar};

impl From<Box<dyn Datum>> for Box<dyn Scalar> {
    fn from(value: Box<dyn Datum>) -> Self {
        value.as_ref().into()
    }
}

impl<T: Array> From<ArrowScalar<T>> for Box<dyn Scalar> {
    fn from(value: ArrowScalar<T>) -> Self {
        let datum: &dyn Datum = &value;
        datum.into()
    }
}

impl From<&dyn Datum> for Box<dyn Scalar> {
    fn from(value: &dyn Datum) -> Self {
        let (arr, is_scalar) = value.get();
        assert!(is_scalar, "Datum was not a scalar");
        if arr.is_null(0) {
            return NullableScalar::None(arr.data_type().try_into().unwrap()).boxed();
        }

        match arr.data_type() {
            DataType::Boolean => {
                let arr = arr.as_boolean();
                BoolScalar::new(arr.value(0)).boxed()
            }
            DataType::Int8 => arr.as_primitive::<Int8Type>().value(0).into(),
            DataType::Int16 => arr.as_primitive::<Int16Type>().value(0).into(),
            DataType::Int32 => arr.as_primitive::<Int32Type>().value(0).into(),
            DataType::Int64 => arr.as_primitive::<Int64Type>().value(0).into(),
            DataType::UInt8 => arr.as_primitive::<UInt8Type>().value(0).into(),
            DataType::UInt16 => arr.as_primitive::<UInt16Type>().value(0).into(),
            DataType::UInt32 => arr.as_primitive::<UInt32Type>().value(0).into(),
            DataType::UInt64 => arr.as_primitive::<UInt64Type>().value(0).into(),
            DataType::Float32 => arr.as_primitive::<Float32Type>().value(0).into(),
            DataType::Float64 => arr.as_primitive::<Float64Type>().value(0).into(),
            _ => todo!("implement other scalar types {:?}", arr),
        }
    }
}
impl From<Box<dyn Scalar>> for Box<dyn Datum> {
    fn from(value: Box<dyn Scalar>) -> Self {
        value.as_ref().into()
    }
}

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
