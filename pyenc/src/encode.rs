use std::mem::MaybeUninit;

use arrow2::array::Array as ArrowArray;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use enc::array::Array;

use crate::array::PyArray;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = import_arrow_array(obj)?;
        let enc_array: Array = arrow_array.as_ref().into();
        PyArray::wrap(obj.py(), enc_array)
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}

fn import_arrow_array(obj: &PyAny) -> PyResult<Box<dyn ArrowArray>> {
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
