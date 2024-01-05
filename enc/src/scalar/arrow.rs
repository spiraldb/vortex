use arrow2::scalar::Scalar as ArrowScalar;

use crate::scalar::{BoolScalar, NullableScalar, PrimitiveScalar, Scalar};
use crate::types::{DType, IntWidth};

impl TryFrom<&dyn arrow2::scalar::Scalar> for Box<dyn Scalar> {
    type Error = ();

    fn try_from(value: &dyn ArrowScalar) -> Result<Self, Self::Error> {
        use arrow2::datatypes::DataType::*;
        use arrow2::scalar;

        let any = value.as_any();
        match value.data_type() {
            Boolean => {
                if let Some(scalar) = any.downcast_ref::<scalar::BooleanScalar>() {
                    return match scalar.value() {
                        Some(bool) => Ok(BoolScalar::new(bool).boxed()),
                        None => Ok(NullableScalar::none(DType::Bool).boxed()),
                    };
                }
            }
            Int8 => {}
            Int16 => {}
            Int32 => {
                if let Some(scalar) = any.downcast_ref::<scalar::PrimitiveScalar<i32>>() {
                    return match scalar.value() {
                        Some(int) => Ok(PrimitiveScalar::new(*int).boxed()),
                        None => Ok(NullableScalar::none(DType::Int(IntWidth::_32)).boxed()),
                    };
                }
            }
            Int64 => {}
            UInt8 => {}
            UInt16 => {}
            UInt32 => {}
            UInt64 => {}
            Float32 => {}
            Float64 => {}
            Struct(_) => {}
            _ => {}
        }

        todo!("implement other scalar types {:?}", value)
    }
}

impl TryFrom<&dyn Scalar> for Box<dyn ArrowScalar> {
    type Error = ();

    fn try_from(_value: &dyn Scalar) -> Result<Self, Self::Error> {
        todo!()
    }
}
