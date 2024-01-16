use std::mem::MaybeUninit;

use arrow2::datatypes::DataType as ArrowDataType;
use pyo3::{pyclass, pymethods, PyAny, PyResult};

use crate::error::PyEncError;
use enc::types::DType;

#[pyclass(name = "DType", module = "enc", subclass)]
#[allow(dead_code)]
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

#[pymethods]
impl PyDType {
    #[staticmethod]
    fn from_arrow(
        #[pyo3(from_py_with = "import_arrow_dtype")] arrow_dtype: ArrowDataType,
    ) -> PyResult<Self> {
        Ok(PyDType::new(
            arrow_dtype.try_into().map_err(PyEncError::new)?,
        ))
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
