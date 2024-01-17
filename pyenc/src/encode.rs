use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use enc::array::Array;

use crate::array::PyArray;
use crate::arrow;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = arrow::import_arrow_array(obj)?;
        let enc_array: Array = arrow_array.as_ref().into();
        PyArray::wrap(obj.py(), enc_array)
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
