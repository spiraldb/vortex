use std::mem::MaybeUninit;

use arrow2::datatypes::DataType as ArrowDataType;
use pyo3::prelude::*;
use pyo3::types::PyType;

use enc::types::DType;

use crate::error::PyEncError;

#[pyclass(name = "DType", module = "enc", subclass)]
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
        #[pyo3(from_py_with = "import_arrow_dtype")] arrow_dtype: ArrowDataType,
    ) -> PyResult<Py<Self>> {
        PyDType::wrap(cls.py(), arrow_dtype.try_into().map_err(PyEncError::new)?)
    }
}

fn import_arrow_dtype(obj: &PyAny) -> PyResult<ArrowDataType> {
    // Export the array from the PyArrow object
    let mut uninit_schema: MaybeUninit<arrow2::ffi::ArrowSchema> = MaybeUninit::zeroed();
    obj.call_method("_export_to_c", (uninit_schema.as_mut_ptr() as usize,), None)?;

    unsafe {
        let schema_struct = uninit_schema.assume_init();

        // We unwrap here since we know the exported array was a valid Arrow2 array.
        let schema_field = arrow2::ffi::import_field_from_c(&schema_struct).unwrap();
        Ok(schema_field.data_type)
    }
}
