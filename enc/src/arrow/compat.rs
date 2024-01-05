use std::mem;

pub fn into_polars(array: &dyn arrow2::array::Array) -> Box<dyn polars_arrow::array::Array> {
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
