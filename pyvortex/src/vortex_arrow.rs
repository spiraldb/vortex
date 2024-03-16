use arrow::array::Array as ArrowArray;
use arrow::error::ArrowError;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use vortex::array::Array;
use vortex::compute::as_arrow::as_arrow;

use crate::error::PyVortexError;

pub fn map_arrow_err(error: ArrowError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

pub fn export_array<T: AsRef<dyn Array>>(py: Python, array: &T) -> PyResult<PyObject> {
    // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
    // NOTE(robert): Return RecordBatchStreamReader always?

    // // Export the schema once
    // let data_type: DataType = array.as_ref().dtype().into();
    // let pa_data_type = data_type.to_pyarrow(py)?;
    //
    // // Import pyarrow and its Array class
    // let mod_pyarrow = PyModule::import(py, "pyarrow")?;

    // TODO(ngates): chunked arrays?
    as_arrow(array.as_ref())
        .map_err(PyVortexError::map_err)?
        .into_data()
        .to_pyarrow(py)

    // Combine into a chunked array
    // mod_pyarrow.call_method(
    //     "chunked_array",
    //     (PyList::new(py, chunks?),),
    //     Some([("type", pa_data_type)].into_py_dict(py)),
    // )
}
