use enc::types::DType;
use pyo3::{pyclass, pymethods};

#[pyclass(name = "DType", module = "enc", subclass)]
pub struct PyDType {
    inner: DType,
}

impl PyDType {
    pub fn new(inner: DType) -> Self {
        Self { inner }
    }
}

impl From<DType> for PyDType {
    fn from(value: DType) -> Self {
        Self::new(value)
    }
}
