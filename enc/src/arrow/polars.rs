use std::mem;

use arrow::array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::array::{Array, BooleanArray, Datum, PrimitiveArray};
use arrow::datatypes::DataType;
use polars_core::prelude::{AnyValue, Series};

pub trait IntoPolarsSeries {
    fn into_polars(self) -> Series;
}

impl IntoPolarsSeries for &dyn arrow::array::Array {
    fn into_polars(self) -> Series {
        let polars_array = into_polars_arrow(self);
        ("array", polars_array).try_into().unwrap()
    }
}

impl IntoPolarsSeries for Vec<&dyn arrow::array::Array> {
    fn into_polars(self) -> Series {
        let chunks: Vec<Box<dyn polars_arrow::array::Array>> =
            self.iter().map(|array| into_polars_arrow(*array)).collect();
        ("array", chunks).try_into().unwrap()
    }
}

fn into_polars_arrow(array: &dyn arrow::array::Array) -> Box<dyn polars_arrow::array::Array> {
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

impl IntoPolarsValue for &dyn Datum {
    fn into_polars<'a>(self) -> AnyValue<'a> {
        macro_rules! unwrap_scalar {
            ($scalar_arr:expr, $type:ty, $variant:tt) => {{
                let typed_arr = $scalar_arr.as_any().downcast_ref::<$type>().unwrap();
                if typed_arr.is_null(0) {
                    AnyValue::Null
                } else {
                    AnyValue::$variant(typed_arr.value(0))
                }
            }};
        }

        let (arr, is_scalar) = self.get();
        assert!(is_scalar, "Datum was not a scalar");
        match arr.data_type() {
            DataType::Null => AnyValue::Null,
            DataType::Boolean => unwrap_scalar!(arr, BooleanArray, Boolean),
            DataType::UInt8 => unwrap_scalar!(arr, PrimitiveArray<UInt8Type>, UInt8),
            DataType::UInt16 => unwrap_scalar!(arr, PrimitiveArray<UInt16Type>, UInt16),
            DataType::UInt32 => unwrap_scalar!(arr, PrimitiveArray<UInt32Type>, UInt32),
            DataType::UInt64 => unwrap_scalar!(arr, PrimitiveArray<UInt64Type>, UInt64),
            DataType::Int8 => unwrap_scalar!(arr, PrimitiveArray<Int8Type>, Int8),
            DataType::Int16 => unwrap_scalar!(arr, PrimitiveArray<Int16Type>, Int16),
            DataType::Int32 => unwrap_scalar!(arr, PrimitiveArray<Int32Type>, Int32),
            DataType::Int64 => unwrap_scalar!(arr, PrimitiveArray<Int64Type>, Int64),
            // DataType::Float16 => unwrap_scalar!(arr, PrimitiveArray<Float16Type>, Float16),
            DataType::Float32 => unwrap_scalar!(arr, PrimitiveArray<Float32Type>, Float32),
            DataType::Float64 => unwrap_scalar!(arr, PrimitiveArray<Float64Type>, Float64),
            _ => todo!("implement other scalar types {:?}", arr),
        }
    }
}
