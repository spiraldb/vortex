use std::mem::MaybeUninit;

use arrow2::array::Array as ArrowArray;
use arrow2::datatypes::{DataType as ArrowDataType, Field};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use pyo3::{PyAny, PyResult};

use enc::array::{Array, ArrayEncoding};
use enc::types::DType;

pub fn import_arrow_array(obj: &PyAny) -> PyResult<Box<dyn ArrowArray>> {
    // Export the array from the PyArrow object
    let mut uninit_array: MaybeUninit<arrow2::ffi::ArrowArray> = MaybeUninit::zeroed();
    let mut uninit_schema: MaybeUninit<arrow2::ffi::ArrowSchema> = MaybeUninit::zeroed();
    obj.call_method(
        "_export_to_c",
        (
            uninit_array.as_mut_ptr() as usize,
            uninit_schema.as_mut_ptr() as usize,
        ),
        None,
    )?;

    unsafe {
        let array_struct = uninit_array.assume_init();
        let schema_struct = uninit_schema.assume_init();

        // We unwrap here since we know the exported array was a valid Arrow2 array.
        let schema_field = arrow2::ffi::import_field_from_c(&schema_struct).unwrap();
        let arrow_array =
            arrow2::ffi::import_array_from_c(array_struct, schema_field.data_type).unwrap();
        Ok(arrow_array)
    }
}

pub fn export_array_array<'py>(py: Python<'py>, array: &Array) -> PyResult<&'py PyAny> {
    // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
    // NOTE(robert): Return RecordBatchStreamReader always?

    // Export the schema once
    let data_type: ArrowDataType = array.dtype().into();
    let field = Field::new(
        "array",
        data_type,
        matches!(array.dtype(), DType::Nullable(_)),
    );
    let schema_struct = arrow2::ffi::export_field_to_c(&field);

    // Import pyarrow and its Array class
    let mod_pyarrow = PyModule::import(py, "pyarrow")?;
    let cls_array = mod_pyarrow.getattr("Array")?;

    // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
    let chunks: PyResult<Vec<&PyAny>> = array
        .iter_arrow()
        .map(|arrow_array| {
            let array_struct = arrow2::ffi::export_array_to_c(arrow_array);
            cls_array.call_method1(
                "_import_from_c",
                (
                    (&array_struct as *const arrow2::ffi::ArrowArray) as usize,
                    (&schema_struct as *const arrow2::ffi::ArrowSchema) as usize,
                ),
            )
        })
        .collect();

    let dtype_array = mod_pyarrow.getattr("DataType")?;
    let dtype_struct = arrow2::ffi::export_field_to_c(&field);
    let pa_data_dtype = dtype_array.call_method1(
        "_import_from_c",
        ((&dtype_struct as *const arrow2::ffi::ArrowSchema) as usize,),
    )?;
    // Combine into a chunked array
    mod_pyarrow.call_method(
        "chunked_array",
        (PyList::new(py, chunks?),),
        Some([("type", pa_data_dtype)].into_py_dict(py)),
    )
}
