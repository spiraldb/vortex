use arrow::array::{make_array, ArrayData};
use arrow::pyarrow::FromPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::array::PyArray;

/// The main entry point for creating enc arrays from other Python objects.
///
#[pyfunction]
pub fn encode(obj: &PyAny) -> PyResult<Py<PyArray>> {
    let pa = obj.py().import("pyarrow")?;
    let pa_array = pa.getattr("Array")?;

    if obj.is_instance(pa_array)? {
        let arrow_array = ArrayData::from_pyarrow(obj).map(make_array)?;
        let enc_array: enc::array::ArrayRef = arrow_array.into();
        PyArray::wrap(obj.py(), enc_array)
    } else {
        Err(PyValueError::new_err("Cannot convert object to enc array"))
    }
}
