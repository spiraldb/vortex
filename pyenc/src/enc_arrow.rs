use arrow::array::Array as ArrowArray;
use arrow::datatypes::DataType;
use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use pyo3::{PyAny, PyResult};

use enc::array::{Array, ArrayEncoding};

pub fn export_array_array<'py>(py: Python<'py>, array: &Array) -> PyResult<&'py PyAny> {
    // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
    // NOTE(robert): Return RecordBatchStreamReader always?

    // Export the schema once
    let data_type: DataType = array.dtype().into();
    let pa_data_type = data_type.to_pyarrow(py)?;

    // Import pyarrow and its Array class
    let mod_pyarrow = PyModule::import(py, "pyarrow")?;

    // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
    let chunks: PyResult<Vec<PyObject>> = array
        .iter_arrow()
        .map(|arrow_array| arrow_array.into_data().to_pyarrow(py))
        .collect();

    // Combine into a chunked array
    mod_pyarrow.call_method(
        "chunked_array",
        (PyList::new(py, chunks?),),
        Some([("type", pa_data_type)].into_py_dict(py)),
    )
}
