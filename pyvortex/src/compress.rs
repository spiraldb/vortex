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

use pyo3::types::PyType;
use pyo3::{pyclass, pyfunction, pymethods, Py, PyResult, Python};

use vortex::compress::{CompressConfig, CompressCtx};

use crate::array::PyArray;

#[derive(Clone)]
#[pyclass(name = "CompressConfig", module = "vortex")]
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
    let ctx = CompressCtx::new(&compress_opts);
    let compressed = py.allow_threads(|| ctx.compress(arr.unwrap(), None));
    PyArray::wrap(py, compressed)
}
