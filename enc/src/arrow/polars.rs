use std::mem;

use arrow2::datatypes::DataType;
use polars_core::prelude::{AnyValue, Series};

pub trait IntoPolarsSeries {
    fn into_polars(self) -> Series;
}

impl IntoPolarsSeries for &dyn arrow2::array::Array {
    fn into_polars(self) -> Series {
        let polars_array = into_polars_arrow(self);
        ("array", polars_array).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Vec<&dyn arrow2::array::Array> {
    fn into_polars(self) -> Series {
        let chunks: Vec<Box<dyn polars_arrow::array::Array>> =
            self.iter().map(|array| into_polars_arrow(*array)).collect();
        ("array", chunks).try_into().unwrap()
    }
}

fn into_polars_arrow(array: &dyn arrow2::array::Array) -> Box<dyn polars_arrow::array::Array> {
    let arrow2_array = arrow2::ffi::export_array_to_c(array.to_boxed());
    let arrow2_schema = arrow2::ffi::export_field_to_c(&arrow2::datatypes::Field::new(
        "",
        array.data_type().clone(),
        false,
    ));

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

impl IntoPolarsValue for &dyn arrow2::scalar::Scalar {
    fn into_polars<'a>(self) -> AnyValue<'a> {
        use arrow2::scalar::*;

        macro_rules! unwrap_scalar {
            ($scalar:expr, $type:ty, $variant:tt) => {{
                let scalar = $scalar.as_any().downcast_ref::<$type>().unwrap();
                match scalar.value() {
                    Some(v) => AnyValue::$variant(v.to_owned()),
                    None => AnyValue::Null,
                }
            }};
        }

        match self.data_type() {
            DataType::Null => AnyValue::Null,
            DataType::Boolean => unwrap_scalar!(self, BooleanScalar, Boolean),
            DataType::Int8 => unwrap_scalar!(self, PrimitiveScalar<i8>, Int8),
            DataType::Int16 => unwrap_scalar!(self, PrimitiveScalar<i16>, Int16),
            DataType::Int32 => unwrap_scalar!(self, PrimitiveScalar<i32>, Int32),
            DataType::Int64 => unwrap_scalar!(self, PrimitiveScalar<i64>, Int64),
            DataType::UInt8 => unwrap_scalar!(self, PrimitiveScalar<u8>, UInt8),
            DataType::UInt16 => unwrap_scalar!(self, PrimitiveScalar<u16>, UInt16),
            DataType::UInt32 => unwrap_scalar!(self, PrimitiveScalar<u32>, UInt32),
            DataType::UInt64 => unwrap_scalar!(self, PrimitiveScalar<u64>, UInt64),
            DataType::Float16 => todo!("Float16 not supported"),
            DataType::Float32 => unwrap_scalar!(self, PrimitiveScalar<f32>, Float32),
            DataType::Float64 => unwrap_scalar!(self, PrimitiveScalar<f64>, Float64),
            _ => todo!("implement other scalar types {:?}", self),
        }
    }
}
