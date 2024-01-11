use arrow2::scalar::Scalar as ArrowScalar;

use crate::scalar::{BoolScalar, NullableScalar, PrimitiveScalar, Scalar};
use crate::types::{DType, FloatWidth, IntWidth};

macro_rules! convert_primitive_scalar {
    ($any:expr, $tp:ty, $dtype:expr) => {{
        if let Some(scalar) = $any.downcast_ref::<scalar::PrimitiveScalar<$tp>>() {
            return match scalar.value() {
                Some(v) => PrimitiveScalar::new(*v).boxed(),
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
    fn from(_value: &dyn Scalar) -> Self {
        todo!()
    }
}
