// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::Array as ArrowArray;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};

use vortex::array::Array;

pub fn map_arrow_err(error: ArrowError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

pub fn export_array<'py, T: AsRef<dyn Array>>(py: Python<'py>, array: &T) -> PyResult<&'py PyAny> {
    // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
    // NOTE(robert): Return RecordBatchStreamReader always?

    // Export the schema once
    let data_type: DataType = array.as_ref().dtype().into();
    let pa_data_type = data_type.to_pyarrow(py)?;

    // Import pyarrow and its Array class
    let mod_pyarrow = PyModule::import(py, "pyarrow")?;

    // Iterate each chunk, export it to Arrow FFI, then import as a pyarrow array
    let chunks: PyResult<Vec<PyObject>> = array
        .as_ref()
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
