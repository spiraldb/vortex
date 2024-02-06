use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

use enc::error::EncError;

pub struct PyEncError(EncError);

impl PyEncError {
    pub fn new(error: EncError) -> Self {
        Self(error)
    }

    pub fn map_err(error: EncError) -> PyErr {
        PyEncError::new(error).into()
    }
}

impl From<PyEncError> for PyErr {
    fn from(value: PyEncError) -> Self {
        PyValueError::new_err(value.0.to_string())
    }
}
