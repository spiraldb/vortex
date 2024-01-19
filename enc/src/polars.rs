use arrow::array::{Array, ArrayRef};
use polars_arrow::array::from_data;
use polars_core::prelude::{AnyValue, Series};

use crate::array::ArrowIterator;
use crate::scalar::{BinaryScalar, BoolScalar, NullableScalar, PScalar, Scalar, Utf8Scalar};
use crate::types::DType;

pub trait IntoPolarsSeries {
    fn into_polars(self) -> Series;
}

impl IntoPolarsSeries for ArrayRef {
    fn into_polars(self) -> Series {
        let polars_array = from_data(&self.to_data());
        ("array", polars_array).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Vec<ArrayRef> {
    fn into_polars(self) -> Series {
        let chunks: Vec<Box<dyn polars_arrow::array::Array>> =
            self.iter().map(|a| from_data(&a.to_data())).collect();
        ("array", chunks).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Box<ArrowIterator> {
    fn into_polars(self) -> Series {
        let chunks: Vec<Box<dyn polars_arrow::array::Array>> =
            self.map(|a| from_data(&a.to_data())).collect();
        ("array", chunks).try_into().unwrap()
    }
}

pub trait IntoPolarsValue {
    fn into_polars<'a>(self) -> AnyValue<'a>;
}

impl IntoPolarsValue for Box<dyn Scalar> {
    fn into_polars<'a>(self) -> AnyValue<'a> {
        self.as_ref().into_polars()
    }
}

impl IntoPolarsValue for &dyn Scalar {
    fn into_polars<'a>(self) -> AnyValue<'a> {
        match self.dtype() {
            DType::Null => AnyValue::Null,
            DType::Nullable(_) => match self.as_any().downcast_ref::<NullableScalar>().unwrap() {
                NullableScalar::Some(value) => value.as_ref().into_polars(),
                NullableScalar::None(_dtype) => AnyValue::Null,
            },
            DType::Bool => {
                AnyValue::Boolean(self.as_any().downcast_ref::<BoolScalar>().unwrap().value())
            }
            DType::Int(_) | DType::UInt(_) | DType::Float(_) => {
                match self.as_any().downcast_ref::<PScalar>().unwrap() {
                    PScalar::U8(v) => AnyValue::UInt8(*v),
                    PScalar::U16(v) => AnyValue::UInt16(*v),
                    PScalar::U32(v) => AnyValue::UInt32(*v),
                    PScalar::U64(v) => AnyValue::UInt64(*v),
                    PScalar::I8(v) => AnyValue::Int8(*v),
                    PScalar::I16(v) => AnyValue::Int16(*v),
                    PScalar::I32(v) => AnyValue::Int32(*v),
                    PScalar::I64(v) => AnyValue::Int64(*v),
                    PScalar::F16(v) => AnyValue::Float32(v.to_f32()),
                    PScalar::F32(v) => AnyValue::Float32(*v),
                    PScalar::F64(v) => AnyValue::Float64(*v),
                }
            }
            DType::Decimal(_, _) => todo!(),
            DType::Utf8 => AnyValue::StringOwned(
                self.as_any()
                    .downcast_ref::<Utf8Scalar>()
                    .unwrap()
                    .value()
                    .into(),
            ),
            DType::Binary => AnyValue::BinaryOwned(
                self.as_any()
                    .downcast_ref::<BinaryScalar>()
                    .unwrap()
                    .value()
                    .clone(),
            ),
            DType::LocalTime(_) => todo!(),
            DType::LocalDate => todo!(),
            DType::Instant(_) => todo!(),
            DType::ZonedDateTime(_) => todo!(),
            DType::Struct(_, _) => todo!(),
            DType::List(_) => todo!(),
            DType::Map(_, _) => todo!(),
        }
    }
}
