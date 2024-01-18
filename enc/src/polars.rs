use std::mem;

use arrow::array::ArrayRef;
use itertools::Itertools;
use polars_core::prelude::{AnyValue, Series};

use crate::array::ArrowIterator;
use crate::scalar::{BoolScalar, NullableScalar, PScalar, Scalar, Utf8Scalar};
use crate::types::DType;

pub trait IntoPolarsSeries {
    fn into_polars(self) -> Series;
}

impl IntoPolarsSeries for ArrayRef {
    fn into_polars(self) -> Series {
        let polars_array = into_polars_arrow(&self);
        ("array", polars_array).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Vec<ArrayRef> {
    fn into_polars(self) -> Series {
        let chunks: Vec<Box<dyn polars_arrow::array::Array>> =
            self.iter().map(into_polars_arrow).collect();
        ("array", chunks).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Box<ArrowIterator> {
    fn into_polars(self) -> Series {
        self.collect_vec().into_polars()
    }
}

fn into_polars_arrow(array: &ArrayRef) -> Box<dyn polars_arrow::array::Array> {
    let arrow2_array = arrow::ffi::FFI_ArrowArray::new(&array.to_data());
    let arrow2_schema = arrow::ffi::FFI_ArrowSchema::try_from(array.data_type()).unwrap();

    unsafe {
        // Transmuate the stable Arrow ABI structs from Arrow2 into Polars.
        let polars_array: polars_arrow::ffi::ArrowArray = mem::transmute(arrow2_array);
        let polars_schema: polars_arrow::ffi::ArrowSchema = mem::transmute(arrow2_schema);

        // We unwrap here since we know the exported array was a valid Arrow2 array.
        let polars_field = polars_arrow::ffi::import_field_from_c(&polars_schema).unwrap();
        polars_arrow::ffi::import_array_from_c(polars_array, polars_field.data_type).unwrap()
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
            DType::Binary => todo!(),
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
