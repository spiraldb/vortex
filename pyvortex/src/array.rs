use pyo3::prelude::*;
use vortex::compute::take;
use vortex::{Array, ArrayDType};

use crate::dtype::PyDType;
use crate::error::PyVortexError;
use crate::vortex_arrow;

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
        vortex_arrow::export_array(self_.py(), &self_.inner)
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
