use arrow::array::{Array as ArrowArray, ArrayRef as ArrowArrayRef};
use polars_arrow::array::from_data;
use polars_core::prelude::{AnyValue, Series};

use crate::array::ArrowIterator;
use crate::dtype::DType;
use crate::scalar::{BinaryScalar, BoolScalar, PScalar, Scalar, Utf8Scalar};

pub trait IntoPolarsSeries {
    fn into_polars(self) -> Series;
}

impl IntoPolarsSeries for ArrowArrayRef {
    fn into_polars(self) -> Series {
        let polars_array = from_data(&self.to_data());
        ("array", polars_array).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Vec<ArrowArrayRef> {
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

impl IntoPolarsValue for Scalar {
    fn into_polars<'a>(self) -> AnyValue<'a> {
        if let Some(ns) = self.as_any().downcast_ref::<NullableScalar>() {
            return match ns {
                NullableScalar::Some(s, _) => s.as_ref().into_polars(),
                NullableScalar::None(_) => AnyValue::Null,
            };
        }

        match self.dtype() {
            DType::Null => AnyValue::Null,
            DType::Bool(_) => {
                AnyValue::Boolean(self.as_any().downcast_ref::<BoolScalar>().unwrap().value())
            }
            DType::Int(_, _, _) | DType::Float(_, _) => {
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
            DType::Decimal(_, _, _) => todo!(),
            DType::Utf8(_) => AnyValue::StringOwned(
                self.as_any()
                    .downcast_ref::<Utf8Scalar>()
                    .unwrap()
                    .value()
                    .into(),
            ),
            DType::Binary(_) => AnyValue::BinaryOwned(
                self.as_any()
                    .downcast_ref::<BinaryScalar>()
                    .unwrap()
                    .value()
                    .clone(),
            ),
            DType::LocalTime(_, _) => todo!(),
            DType::LocalDate(_) => todo!(),
            DType::Instant(_, _) => todo!(),
            DType::ZonedDateTime(_, _) => todo!(),
            DType::Struct(_, _) => todo!(),
            DType::List(_, _) => todo!(),
            DType::Map(_, _, _) => todo!(),
        }
    }
}
