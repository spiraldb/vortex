use pyo3::types::PyType;
use pyo3::{pyclass, pyfunction, pymethods, Py, PyResult, Python};

use enc::compress::{CompressConfig, CompressCtx};

use crate::array::PyArray;

#[derive(Clone)]
#[pyclass(name = "CompressConfig", module = "enc")]
pub struct PyCompressConfig {
    inner: CompressConfig,
}

#[pymethods]
impl PyCompressConfig {
    #[classmethod]
    pub fn default(cls: &PyType) -> PyResult<Py<PyCompressConfig>> {
        Py::new(
            cls.py(),
            Self {
                inner: Default::default(),
            },
        )
    }
}

#[pyfunction]
#[pyo3(signature = (arr, opts = None))]
pub fn compress(
    py: Python<'_>,
    arr: &PyArray,
    opts: Option<PyCompressConfig>,
) -> PyResult<Py<PyArray>> {
    let compress_opts = opts.map(|o| o.inner).unwrap_or_default();
    let compressed = enc::compress::compress(arr.unwrap(), CompressCtx::new(&compress_opts));
    PyArray::wrap(py, compressed)
}
