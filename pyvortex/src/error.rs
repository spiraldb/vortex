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

use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

use vortex::error::VortexError;

pub struct PyVortexError(VortexError);

impl PyVortexError {
    pub fn new(error: VortexError) -> Self {
        Self(error)
    }

    pub fn map_err(error: VortexError) -> PyErr {
        PyVortexError::new(error).into()
    }
}

impl From<PyVortexError> for PyErr {
    fn from(value: PyVortexError) -> Self {
        PyValueError::new_err(value.0.to_string())
    }
}
