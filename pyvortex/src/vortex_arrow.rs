use arrow::array::{Array as ArrowArray, ArrayRef};
use arrow::error::ArrowError;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use vortex::array::ChunkedArray;
use vortex::{Array, IntoCanonical};

pub fn map_arrow_err(error: ArrowError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

pub fn export_array<'py>(py: Python<'py>, array: &Array) -> PyResult<Bound<'py, PyAny>> {
    // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
    // NOTE(robert): Return RecordBatchStreamReader always?
    let chunks: Vec<ArrayRef> = if let Ok(chunked_array) = ChunkedArray::try_from(array) {
        chunked_array
            .chunks()
            .map(|chunk| chunk.into_canonical().unwrap().into_arrow())
            .collect()
    } else {
        vec![array.clone().into_canonical().unwrap().into_arrow()]
    };
    if chunks.is_empty() {
        return Err(PyValueError::new_err("No chunks in array"));
    }

    // Export the schema once
    let data_type = chunks[0].data_type().clone();
    let pa_data_type = data_type.to_pyarrow(py)?;

    // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
    let chunks: PyResult<Vec<PyObject>> = chunks
        .iter()
        .map(|arrow_array| arrow_array.into_data().to_pyarrow(py))
        .collect();

    // Import pyarrow and its Array class
    let mod_pyarrow = PyModule::import_bound(py, "pyarrow")?;

    // Combine into a chunked array
    mod_pyarrow.call_method(
        "chunked_array",
        (PyList::new_bound(py, chunks?),),
        Some(&[("type", pa_data_type)].into_py_dict_bound(py)),
    )
}
