use arrow2::datatypes::DataType;
use arrow2::scalar::{PrimitiveScalar, Scalar as ArrowScalar};

use crate::scalar::{BoolScalar, NullableScalar, PScalar, Scalar};
use crate::types::{DType, FloatWidth, IntWidth};

macro_rules! convert_primitive_scalar {
    ($any:expr, $tp:ty, $dtype:expr) => {{
        if let Some(scalar) = $any.downcast_ref::<scalar::PrimitiveScalar<$tp>>() {
            return match scalar.value() {
                Some(v) => (*v).into(),
                None => NullableScalar::none($dtype).boxed(),
            };
        }
    }};
}

impl From<Box<dyn ArrowScalar>> for Box<dyn Scalar> {
    fn from(value: Box<dyn ArrowScalar>) -> Self {
        value.as_ref().into()
    }
}

impl From<&dyn ArrowScalar> for Box<dyn Scalar> {
    fn from(value: &dyn ArrowScalar) -> Self {
        use arrow2::datatypes::DataType::*;
        use arrow2::scalar;

        let any = value.as_any();
        match value.data_type() {
            Boolean => {
                if let Some(scalar) = any.downcast_ref::<scalar::BooleanScalar>() {
                    return match scalar.value() {
                        Some(bool) => BoolScalar::new(bool).boxed(),
                        None => NullableScalar::none(DType::Bool).boxed(),
                    };
                }
            }
            Int8 => convert_primitive_scalar!(any, i8, DType::Int(IntWidth::_8)),
            Int16 => convert_primitive_scalar!(any, i16, DType::Int(IntWidth::_16)),
            Int32 => convert_primitive_scalar!(any, i32, DType::Int(IntWidth::_32)),
            Int64 => convert_primitive_scalar!(any, i64, DType::Int(IntWidth::_64)),
            UInt8 => convert_primitive_scalar!(any, u8, DType::UInt(IntWidth::_8)),
            UInt16 => convert_primitive_scalar!(any, u16, DType::UInt(IntWidth::_16)),
            UInt32 => convert_primitive_scalar!(any, u32, DType::UInt(IntWidth::_32)),
            UInt64 => convert_primitive_scalar!(any, u64, DType::UInt(IntWidth::_64)),
            Float32 => convert_primitive_scalar!(any, f32, DType::Float(FloatWidth::_32)),
            Float64 => convert_primitive_scalar!(any, f64, DType::Float(FloatWidth::_64)),
            _ => {}
        }

        todo!("implement other scalar types {:?}", value)
    }
}

impl From<&dyn Scalar> for Box<dyn ArrowScalar> {
    fn from(value: &dyn Scalar) -> Self {
        if let Some(pscalar) = value.as_any().downcast_ref::<PScalar>() {
            let dtype: DataType = value.dtype().into();
            match pscalar {
                PScalar::U8(v) => return Box::new(PrimitiveScalar::<u8>::new(dtype, Some(*v))),
                PScalar::U16(v) => {
                    return Box::new(PrimitiveScalar::<u16>::new(dtype, Some(*v)));
                }
                PScalar::U32(v) => {
                    return Box::new(PrimitiveScalar::<u32>::new(dtype, Some(*v)));
                }
                PScalar::U64(v) => {
                    return Box::new(PrimitiveScalar::<u64>::new(dtype, Some(*v)));
                }
                PScalar::I8(v) => return Box::new(PrimitiveScalar::<i8>::new(dtype, Some(*v))),
                PScalar::I16(v) => {
                    return Box::new(PrimitiveScalar::<i16>::new(dtype, Some(*v)));
                }
                PScalar::I32(v) => {
                    return Box::new(PrimitiveScalar::<i32>::new(dtype, Some(*v)));
                }
                PScalar::I64(v) => {
                    return Box::new(PrimitiveScalar::<i64>::new(dtype, Some(*v)));
                }
                PScalar::F16(_v) => {
                    todo!("Convert half f16 into arrow f16");
                }
                PScalar::F32(v) => {
                    return Box::new(PrimitiveScalar::<f32>::new(dtype, Some(*v)));
                }
                PScalar::F64(v) => {
                    return Box::new(PrimitiveScalar::<f64>::new(dtype, Some(*v)));
                }
            }
        }

        todo!("implement other scalar types {:?}", value)
    }
}
