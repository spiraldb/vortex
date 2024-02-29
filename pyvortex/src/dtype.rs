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

use arrow::datatypes::DataType;
use arrow::pyarrow::FromPyArrow;
use pyo3::types::PyType;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use vortex::arrow::convert::TryIntoDType;

use vortex::dtype::DType;

use crate::error::PyVortexError;

#[pyclass(name = "DType", module = "vortex", subclass)]
pub struct PyDType {
    inner: DType,
}

impl PyDType {
    pub fn wrap(py: Python<'_>, inner: DType) -> PyResult<Py<Self>> {
        Py::new(py, Self { inner })
    }

    pub fn unwrap(&self) -> &DType {
        &self.inner
    }
}

#[pymethods]
impl PyDType {
    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    #[classmethod]
    fn from_pyarrow(
        cls: &PyType,
        #[pyo3(from_py_with = "import_arrow_dtype")] arrow_dtype: DataType,
        nullable: bool,
    ) -> PyResult<Py<Self>> {
        PyDType::wrap(
            cls.py(),
            arrow_dtype
                .try_into_dtype(nullable)
                .map_err(PyVortexError::new)?,
        )
    }
}

fn import_arrow_dtype(obj: &PyAny) -> PyResult<DataType> {
    DataType::from_pyarrow(obj)
}
