use arrow::array::{Array as ArrowArray, ArrayRef};
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList};
use vortex::array::ChunkedArray;
use vortex::compute::take;
use vortex::{Array, ArrayDType, IntoCanonical};

use crate::dtype::PyDType;
use crate::error::PyVortexError;

#[pyclass(name = "Array", module = "vortex", sequence, subclass)]
pub struct PyArray {
    inner: Array,
}

impl PyArray {
    pub fn new(inner: Array) -> PyArray {
        PyArray { inner }
    }

    pub fn unwrap(&self) -> &Array {
        &self.inner
    }
}

#[pymethods]
impl PyArray {
    fn to_arrow(self_: PyRef<'_, Self>) -> PyResult<Bound<PyAny>> {
        // NOTE(ngates): for struct arrays, we could also return a RecordBatchStreamReader.
        // NOTE(robert): Return RecordBatchStreamReader always?
        let py = self_.py();
        let vortex = &self_.inner;

        let chunks: Vec<ArrayRef> = if let Ok(chunked_array) = ChunkedArray::try_from(vortex) {
            chunked_array
                .chunks()
                .map(|chunk| -> PyResult<ArrayRef> {
                    Ok(chunk
                        .into_canonical()
                        .map_err(PyVortexError::map_err)?
                        .into_arrow())
                })
                .collect::<PyResult<Vec<ArrayRef>>>()?
        } else {
            vec![vortex
                .clone()
                .into_canonical()
                .map_err(PyVortexError::map_err)?
                .into_arrow()]
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

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[getter]
    fn encoding(&self) -> String {
        self.inner.encoding().id().to_string()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }

    #[getter]
    fn dtype(self_: PyRef<Self>) -> PyResult<Py<PyDType>> {
        PyDType::wrap(self_.py(), self_.inner.dtype().clone())
    }

    fn take<'py>(&self, indices: PyRef<'py, Self>) -> PyResult<Bound<'py, PyArray>> {
        take(&self.inner, indices.unwrap())
            .map_err(PyVortexError::map_err)
            .and_then(|arr| Bound::new(indices.py(), PyArray { inner: arr }))
    }
}
